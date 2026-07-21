import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ===================== 配置项 =====================
STATUS_DIR = "status"
SCORES_DIR = "scores"
AGE_PENALTY_MIN = 0.3
WEIGHT_1D = 0.4
WEIGHT_3D = 0.3
WEIGHT_7D = 0.3
MAX_SCORE = 100
# ==================================================

os.makedirs(SCORES_DIR, exist_ok=True)


def parse_iso_date(date_str):
    """解析 GitHub ISO 日期 → 统一返回带时区的 UTC 时间"""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except:
        return None


def time_activity_score(last_date, now):
    if not last_date:
        return 0
    days = (now - last_date).days
    return max(0, 100 - (days / 30) * 100)


def normalize_list(values):
    if not values or max(values) == 0:
        return [0] * len(values)
    max_v = max(values)
    return [(v / max_v) * 100 for v in values]


def project_age_penalty(created_at, now):
    if not created_at or not now:
        return AGE_PENALTY_MIN
    days_old = (now - created_at).days
    if days_old <= 365:
        return 1.0
    if days_old >= 730:
        return AGE_PENALTY_MIN
    penalty = 1.0 - ((days_old - 365) / 365) * (1.0 - AGE_PENALTY_MIN)
    return round(penalty, 2)


def developer_activity_score(closed_prs, open_prs, closed_issues, open_issues):
    pr_total = closed_prs + open_prs
    pr_merge_rate = closed_prs / pr_total if pr_total > 0 else 0

    issue_total = closed_issues + open_issues
    issue_close_rate = closed_issues / issue_total if issue_total > 0 else 0

    score = (pr_merge_rate * 60) + (issue_close_rate * 40)
    return min(MAX_SCORE, score)


def community_activity_score(stars, forks, watching, open_issues, open_prs):
    score = (
        stars * 0.35
        + forks * 0.25
        + watching * 0.20
        + min(open_issues, 2000) * 0.10
        + min(open_prs, 2000) * 0.10
    )
    return min(MAX_SCORE, score)


def extract_date(filename):
    match = re.search(r"(\d{8})", filename)
    return match.group(1) if match else None


def get_file_type(filename):
    if filename.startswith("general_"):
        return "general"
    elif filename.startswith("science_"):
        return "science"
    return None


def main():
    # ✅ 全局统一：当前 UTC 时间（带时区，彻底解决 TypeError）
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
            repo = item["name"]
            target_dict[repo][date_str] = item

    if not all_dates:
        print("未找到任何有效数据")
        return

    base_date = sorted(all_dates)[-1]
    print(f"基准日期：{base_date}")

    for category, repo_snapshots in data_by_type.items():
        if not repo_snapshots:
            continue

        results = []
        for repo, snapshots in repo_snapshots.items():
            current = snapshots.get(base_date)
            if not current:
                continue

            created_at_str = current.get("created_at", "")

            # 时间活跃度计算
            time_scores = [
                time_activity_score(parse_iso_date(current["last_commit"]), now),
                time_activity_score(parse_iso_date(current["last_open_issue"]), now),
                time_activity_score(parse_iso_date(current["last_closed_issue"]), now),
                time_activity_score(parse_iso_date(current["last_open_pr"]), now),
                time_activity_score(parse_iso_date(current["last_closed_pr"]), now),
                time_activity_score(parse_iso_date(current["last_fork"]), now),
            ]
            time_score = sum(time_scores) / len(time_scores)

            # 基础数据
            stars = current["stars"]
            forks = current["forks"]
            watching = current["watching"]
            open_issues = current["open_issues"]
            closed_issues = current["closed_issues"]
            open_prs = current["open_prs"]
            closed_prs = current["closed_prs"]
            commits = current["commits"]

            # 评分
            dev_score = developer_activity_score(
                closed_prs, open_prs, closed_issues, open_issues
            )
            community_score = community_activity_score(
                stars, forks, watching, open_issues, open_prs
            )
            raw_total = (time_score * 0.4) + (dev_score * 0.3) + (community_score * 0.3)

            # ✅ 惩罚系数：当前时间 - 创建时间（完全生效）
            age_penalty = project_age_penalty(
                parse_iso_date(current["created_at"]), now
            )
            base_total = raw_total * age_penalty

            # 热度计算
            def delta_score(days):
                target = (
                    datetime.strptime(base_date, "%Y%m%d") - timedelta(days=days)
                ).strftime("%Y%m%d")
                old = snapshots.get(target)
                if not old:
                    return base_total * 0.5
                delta = [
                    max(0, stars - old["stars"]),
                    max(0, forks - old["forks"]),
                    max(0, commits - old["commits"]),
                    max(0, closed_prs - old["closed_prs"]),
                    max(0, closed_issues - old["closed_issues"]),
                ]
                norm = normalize_list(delta)
                return min(MAX_SCORE, sum(norm) / len(norm))

            heat_1d = delta_score(1)
            heat_3d = delta_score(3)
            heat_7d = delta_score(7)

            total_heat = (
                (heat_1d * WEIGHT_1D) + (heat_3d * WEIGHT_3D) + (heat_7d * WEIGHT_7D)
            )
            total_heat = round(min(MAX_SCORE, total_heat), 2)

            results.append(
                {
                    "repo": repo,
                    "created_at": created_at_str,
                    "heat_1d": round(heat_1d, 2),
                    "heat_3d": round(heat_3d, 2),
                    "heat_7d": round(heat_7d, 2),
                    "total_heat": total_heat,
                    "age_penalty": round(age_penalty, 2),
                    "calculated_at": base_date,
                }
            )

        out_file = os.path.join(SCORES_DIR, f"{category}_heat.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"✅ {category} 热度结果已保存：{out_file}")


if __name__ == "__main__":
    main()
