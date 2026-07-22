import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ===================== Configuration =====================
STATUS_DIR = "status"
SCORES_DIR = "scores"
REPOS_DIR = "repos"
AGE_PENALTY_MIN = 0.3
WEIGHT_1D = 0.4
WEIGHT_3D = 0.3
WEIGHT_7D = 0.3
MAX_SCORE = 100
# Mapping: repos config filename -> output category
CONFIG_CATEGORY_MAP = {"science.json": "science", "general.json": "general"}
# =========================================================

os.makedirs(SCORES_DIR, exist_ok=True)


def parse_iso_date(date_str):
    """Parse GitHub ISO timestamp to UTC datetime with timezone"""
    if not date_str or date_str == "N/A":
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def time_activity_score(last_date, now):
    """Calculate activity score based on time difference from latest event"""
    if not last_date:
        return 0.0
    days = (now - last_date).days
    return max(0.0, 100.0 - (days / 30.0) * 100.0)


def normalize_global(values):
    """Global min-max normalization for a full list across ALL repos (0~100)"""
    if not values:
        return []
    max_v = max(values)
    min_v = min(values)
    if max_v == min_v:
        return [0.0] * len(values)
    return [(v - min_v) / (max_v - min_v) * 100.0 for v in values]


def project_age_penalty(created_at, now):
    """Apply aging penalty for older repositories"""
    if not created_at or not now:
        return AGE_PENALTY_MIN
    days_old = (now - created_at).days
    if days_old <= 365:
        return 1.0
    if days_old >= 730:
        return AGE_PENALTY_MIN
    penalty = 1.0 - ((days_old - 365) / 365.0) * (1.0 - AGE_PENALTY_MIN)
    return round(penalty, 2)


def developer_activity_score(closed_prs, open_prs, closed_issues, open_issues):
    """Score based on PR merge rate and issue close rate"""
    pr_total = closed_prs + open_prs
    pr_merge_rate = closed_prs / pr_total if pr_total > 0 else 0.0

    issue_total = closed_issues + open_issues
    issue_close_rate = closed_issues / issue_total if issue_total > 0 else 0.0

    score = (pr_merge_rate * 60.0) + (issue_close_rate * 40.0)
    return min(MAX_SCORE, score)


def community_activity_score(stars, forks, watching, open_issues, open_prs):
    """Score based on community metrics"""
    score = (
        stars * 0.35
        + forks * 0.25
        + watching * 0.20
        + min(open_issues, 2000) * 0.10
        + min(open_prs, 2000) * 0.10
    )
    return min(MAX_SCORE, score)


def extract_date(filename):
    """Extract YYYYMMDD date string from filename"""
    match = re.search(r"(\d{8})", filename)
    return match.group(1) if match else None


def get_file_type(filename):
    """Identify category: general / science"""
    if filename.startswith("general_"):
        return "general"
    elif filename.startswith("science_"):
        return "science"
    return None


def get_target_date(base_date_str, offset_days):
    """Calculate target history date string (YYYYMMDD)"""
    base_dt = datetime.strptime(base_date_str, "%Y%m%d").date()
    target_dt = base_dt - timedelta(days=offset_days)
    return target_dt.strftime("%Y%m%d")


def load_repo_list_from_config(config_name):
    """Load repo full_name list from repos config file"""
    config_path = os.path.join(REPOS_DIR, config_name)
    if not os.path.exists(config_path):
        print(f"Warning: config file {config_path} not found")
        return []
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw_list = json.load(f)
        valid = []
        for item in raw_list:
            if isinstance(item, str) and "/" in item and item.strip():
                valid.append(item.strip())
        return valid
    except Exception as e:
        print(f"Error load {config_name}: {str(e)}")
        return []


def main():
    now = datetime.now(timezone.utc)

    # Load all snapshot data from status folder
    snapshot_data = defaultdict(dict)  # snapshot_data[repo_fullname][date] = item
    all_dates = set()

    for filename in os.listdir(STATUS_DIR):
        file_type = get_file_type(filename)
        if not file_type or not filename.endswith(".json"):
            continue
        date_str = extract_date(filename)
        if not date_str:
            continue
        all_dates.add(date_str)

        path = os.path.join(STATUS_DIR, filename)
        with open(path, "r", encoding="utf-8") as f:
            items = json.load(f)

        for item in items:
            repo_name = item["name"]
            snapshot_data[repo_name][date_str] = item

    if not all_dates:
        print("No valid status snapshot data found")
        return

    base_date = sorted(all_dates)[-1]
    print(f"Base snapshot date: {base_date}")

    # Process each config file
    for config_filename, category in CONFIG_CATEGORY_MAP.items():
        repo_list = load_repo_list_from_config(config_filename)
        if not repo_list:
            print(f"Skipping {config_filename}: empty repo list")
            continue
        print(
            f"\nProcess category [{category}], total repos in config: {len(repo_list)}"
        )

        repo_meta_list = []
        calculation_history = []

        # First pass: collect raw metrics for repos in config
        for repo_fullname in repo_list:
            snapshots = snapshot_data.get(repo_fullname, {})
            current = snapshots.get(base_date)
            if not current:
                print(f"Warning: No snapshot found for {repo_fullname}, skip")
                continue

            # Time activity sub-scores
            time_sub_scores = [
                time_activity_score(parse_iso_date(current["last_commit"]), now),
                time_activity_score(parse_iso_date(current["last_open_issue"]), now),
                time_activity_score(parse_iso_date(current["last_closed_issue"]), now),
                time_activity_score(parse_iso_date(current["last_open_pr"]), now),
                time_activity_score(parse_iso_date(current["last_closed_pr"]), now),
                time_activity_score(parse_iso_date(current["last_fork"]), now),
            ]
            time_score = sum(time_sub_scores) / len(time_sub_scores)

            stars = current["stars"]
            forks = current["forks"]
            watching = current["watching"]
            open_issues = current["open_issues"]
            closed_issues = current["closed_issues"]
            open_prs = current["open_prs"]
            closed_prs = current["closed_prs"]
            commits = current["commits"]

            dev_score = developer_activity_score(
                closed_prs, open_prs, closed_issues, open_issues
            )
            community_score = community_activity_score(
                stars, forks, watching, open_issues, open_prs
            )
            raw_total = (time_score * 0.4) + (dev_score * 0.3) + (community_score * 0.3)

            age_penalty = project_age_penalty(
                parse_iso_date(current["created_at"]), now
            )
            base_total = raw_total * age_penalty

            # Get incremental metrics
            def get_increment(offset):
                target_dt_str = get_target_date(base_date, offset)
                old = snapshots.get(target_dt_str)
                inc = {
                    "stars": max(0, stars - old["stars"]) if old else None,
                    "forks": max(0, forks - old["forks"]) if old else None,
                    "commits": max(0, commits - old["commits"]) if old else None,
                    "closed_prs": (
                        max(0, closed_prs - old["closed_prs"]) if old else None
                    ),
                    "closed_issues": (
                        max(0, closed_issues - old["closed_issues"]) if old else None
                    ),
                }
                return inc

            inc_1d = get_increment(1)
            inc_3d = get_increment(3)
            inc_7d = get_increment(7)

            repo_meta_list.append(
                {
                    "repo": repo_fullname,
                    "created_at": current.get("created_at", ""),
                    "base_total": base_total,
                    "age_penalty": age_penalty,
                    "time_sub_scores": time_sub_scores,
                    "time_score": time_score,
                    "dev_score": dev_score,
                    "community_score": community_score,
                    "raw_total": raw_total,
                    "inc_1d": inc_1d,
                    "inc_3d": inc_3d,
                    "inc_7d": inc_7d,
                }
            )

        if not repo_meta_list:
            print(f"No valid snapshot data for category {category}, skip output")
            continue

        # Global normalization within current category
        def extract_all_inc(meta_list, inc_key):
            vals = []
            for m in meta_list:
                inc_data = m[inc_key]
                for v in inc_data.values():
                    if v is not None:
                        vals.append(v)
            return vals

        all_inc_1d = extract_all_inc(repo_meta_list, "inc_1d")
        all_inc_3d = extract_all_inc(repo_meta_list, "inc_3d")
        all_inc_7d = extract_all_inc(repo_meta_list, "inc_7d")

        norm_1d_pool = normalize_global(all_inc_1d) if all_inc_1d else []
        norm_3d_pool = normalize_global(all_inc_3d) if all_inc_3d else []
        norm_7d_pool = normalize_global(all_inc_7d) if all_inc_7d else []

        def build_norm_map(raw_list, norm_list):
            return {raw: norm for raw, norm in zip(raw_list, norm_list)}

        map_1d = build_norm_map(all_inc_1d, norm_1d_pool)
        map_3d = build_norm_map(all_inc_3d, norm_3d_pool)
        map_7d = build_norm_map(all_inc_7d, norm_7d_pool)

        results = []
        for meta in repo_meta_list:
            inc_1d = meta["inc_1d"]
            inc_3d = meta["inc_3d"]
            inc_7d = meta["inc_7d"]
            fallback_val = meta["base_total"] * 0.5

            def calc_heat(inc_dict, norm_map):
                norm_vals = []
                for raw_val in inc_dict.values():
                    if raw_val is None:
                        norm_vals.append(fallback_val)
                    else:
                        norm_vals.append(norm_map.get(raw_val, fallback_val))
                return sum(norm_vals) / len(norm_vals)

            heat_1d = calc_heat(inc_1d, map_1d)
            heat_3d = calc_heat(inc_3d, map_3d)
            heat_7d = calc_heat(inc_7d, map_7d)

            total_heat = heat_1d * WEIGHT_1D + heat_3d * WEIGHT_3D + heat_7d * WEIGHT_7D
            total_heat = round(min(MAX_SCORE, total_heat), 2)

            # Main summary result
            summary_item = {
                "repo": meta["repo"],
                "created_at": meta["created_at"],
                "heat_1d": round(heat_1d, 2),
                "heat_3d": round(heat_3d, 2),
                "heat_7d": round(heat_7d, 2),
                "total_heat": total_heat,
                "age_penalty": round(meta["age_penalty"], 2),
                "calculated_at": base_date,
            }
            results.append(summary_item)

            # Full calculation history (all intermediate variables)
            history_item = {
                "repo": meta["repo"],
                "calculated_at": base_date,
                "created_at": meta["created_at"],
                "age_penalty": meta["age_penalty"],
                "score_components": {
                    "time_activity": {
                        "sub_scores": [round(x, 2) for x in meta["time_sub_scores"]],
                        "average_score": round(meta["time_score"], 2),
                    },
                    "developer_activity_score": round(meta["dev_score"], 2),
                    "community_activity_score": round(meta["community_score"], 2),
                    "raw_weighted_total": round(meta["raw_total"], 2),
                    "base_total_after_age_penalty": round(meta["base_total"], 2),
                },
                "increment_data": {
                    "inc_1d": inc_1d,
                    "inc_3d": inc_3d,
                    "inc_7d": inc_7d,
                },
                "heat_result": {
                    "heat_1d": round(heat_1d, 2),
                    "heat_3d": round(heat_3d, 2),
                    "heat_7d": round(heat_7d, 2),
                    "total_heat": total_heat,
                },
            }
            calculation_history.append(history_item)

        # Sort summary by total heat descending
        results.sort(key=lambda x: x["total_heat"], reverse=True)
        calculation_history.sort(
            key=lambda x: x["heat_result"]["total_heat"], reverse=True
        )

        # Output main heat summary
        out_summary = os.path.join(SCORES_DIR, f"{category}_heat.json")
        with open(out_summary, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Saved summary heat: {out_summary}, calculated items: {len(results)}")

        # Output full calculation history with all intermediate steps
        out_history = os.path.join(SCORES_DIR, f"{category}_history.json")
        with open(out_history, "w", encoding="utf-8") as f:
            json.dump(calculation_history, f, indent=2, ensure_ascii=False)
        print(f"Saved full calculation history: {out_history}")


if __name__ == "__main__":
    main()
