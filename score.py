import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ===================== Configuration =====================
STATUS_DIR = "status"
SCORES_DIR = "scores"
AGE_PENALTY_MIN = 0.3
WEIGHT_1D = 0.4
WEIGHT_3D = 0.3
WEIGHT_7D = 0.3
MAX_SCORE = 100
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


def main():
    now = datetime.now(timezone.utc)

    data_by_type = {
        "general": defaultdict(dict),
        "science": defaultdict(dict),
    }
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

        target_dict = data_by_type[file_type]
        for item in items:
            repo_name = item["name"]
            target_dict[repo_name][date_str] = item

    if not all_dates:
        print("No valid status data found")
        return

    base_date = sorted(all_dates)[-1]
    print(f"Base snapshot date: {base_date}")

    for category, repo_snapshots in data_by_type.items():
        if not repo_snapshots:
            continue

        repo_meta_list = []
        # First pass: collect static data and raw increments
        for repo, snapshots in repo_snapshots.items():
            current = snapshots.get(base_date)
            if not current:
                continue

            # Basic activity score calculation
            time_scores = [
                time_activity_score(parse_iso_date(current["last_commit"]), now),
                time_activity_score(parse_iso_date(current["last_open_issue"]), now),
                time_activity_score(parse_iso_date(current["last_closed_issue"]), now),
                time_activity_score(parse_iso_date(current["last_open_pr"]), now),
                time_activity_score(parse_iso_date(current["last_closed_pr"]), now),
                time_activity_score(parse_iso_date(current["last_fork"]), now),
            ]
            time_score = sum(time_scores) / len(time_scores)

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

            # Fetch incremental changes for 1d / 3d /7d
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
                    "repo": repo,
                    "created_at": current.get("created_at", ""),
                    "base_total": base_total,
                    "age_penalty": age_penalty,
                    "inc_1d": inc_1d,
                    "inc_3d": inc_3d,
                    "inc_7d": inc_7d,
                }
            )

        # --------------------------
        # Global normalization for increments across ALL repos in category
        # --------------------------
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

        # Build mapping for normalized value lookup
        def build_norm_map(raw_list, norm_list):
            return {raw: norm for raw, norm in zip(raw_list, norm_list)}

        map_1d = build_norm_map(all_inc_1d, norm_1d_pool)
        map_3d = build_norm_map(all_inc_3d, norm_3d_pool)
        map_7d = build_norm_map(all_inc_7d, norm_7d_pool)

        results = []
        for meta in repo_meta_list:
            repo = meta["repo"]
            inc_1d = meta["inc_1d"]
            inc_3d = meta["inc_3d"]
            inc_7d = meta["inc_7d"]

            def calc_heat(inc_dict, norm_map, fallback):
                norm_vals = []
                for k, raw_val in inc_dict.items():
                    if raw_val is None:
                        norm_vals.append(fallback)
                    else:
                        norm_vals.append(norm_map.get(raw_val, fallback))
                return sum(norm_vals) / len(norm_vals)

            # Fallback: if history snapshot missing, use half base score
            fallback_val = meta["base_total"] * 0.5
            heat_1d = calc_heat(inc_1d, map_1d, fallback_val)
            heat_3d = calc_heat(inc_3d, map_3d, fallback_val)
            heat_7d = calc_heat(inc_7d, map_7d, fallback_val)

            total_heat = heat_1d * WEIGHT_1D + heat_3d * WEIGHT_3D + heat_7d * WEIGHT_7D
            total_heat = round(min(MAX_SCORE, total_heat), 2)

            results.append(
                {
                    "repo": repo,
                    "created_at": meta["created_at"],
                    "heat_1d": round(heat_1d, 2),
                    "heat_3d": round(heat_3d, 2),
                    "heat_7d": round(heat_7d, 2),
                    "total_heat": total_heat,
                    "age_penalty": round(meta["age_penalty"], 2),
                    "calculated_at": base_date,
                }
            )

        # Sort output by total heat descending
        results.sort(key=lambda x: x["total_heat"], reverse=True)

        out_file = os.path.join(SCORES_DIR, f"{category}_heat.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Saved heat result: {out_file}")


if __name__ == "__main__":
    main()
