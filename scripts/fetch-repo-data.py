import json
import os
from pathlib import Path

import requests

# ===================== 配置区：owner/name 形式 =====================
TARGET_REPOS = [
    "EvoScientist/EvoScientist",
    "tsingyuai/scientify",
    "wentorai/Research-Claw",
    "ymx10086/ResearchClaw",
    "wanshuiyin/Auto-claude-code-research-in-sleep",
    "OpenLAIR/dr-claw",
    "aiming-lab/AutoResearchClaw",
    "InternScience/DrClaw",
    "snap-stanford/Biomni",
    "ur-whitelab/chemcrow-public",
    "shawnleeai/ScholarForge",
    "Prismer-AI/Prismer",
    "ZhihaoAIRobotic/ClawPhD",
    "langchain-ai/local-deep-researcher",
    "zjowowen/InnoClaw",
    "Noietch/ResearchClaw",
    "Mr-Tieguigui/Vibe-Scholar",
    "Leey21/awesome-ai-research-writing",
    "Lylll9436/Paper-Polish-Workflow-skill",
    "lulaiao/DoctorClaw",
    "karpathy/autoresearch",
    "wanshuiyin/Auto-claude-code-research-in-sleep",
    "Orchestra-Research/AI-Research-SKILLs",
    # 在这里加更多仓库："owner/repo"
]
# ==================================================================

GITHUB_API = "https://api.github.com/graphql"
GH_TOKEN = os.getenv("GH_TOKEN")

if not GH_TOKEN:
    print("❌ 错误：未设置 GH_TOKEN 环境变量")
    exit(1)

headers = {"Authorization": f"Bearer {GH_TOKEN}", "Content-Type": "application/json"}

query = """
query getRepoData($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    stargazerCount
    forkCount
    issues(states: OPEN) { totalCount }
    closedIssues: issues(states: CLOSED) { totalCount }
    pullRequests(states: OPEN) { totalCount }
    closedPullRequests: pullRequests(states: CLOSED) { totalCount }
    contributors: collaborators(first: 100) { totalCount }
    defaultBranchRef { target { committedDate } }
    nameWithOwner
    url
    description
  }
}
"""


def fetch_repo(owner, name):
    variables = {"owner": owner, "name": name}
    resp = requests.post(
        GITHUB_API, json={"query": query, "variables": variables}, headers=headers
    )
    data = resp.json()
    if "errors" in data:
        raise Exception(f"API 错误：{data['errors']}")
    return data["data"]["repository"]


def format_data(raw):
    dt = raw.get("defaultBranchRef", {}).get("target", {}).get("committedDate")
    last_commit = "无提交"
    if dt:
        try:
            from datetime import datetime

            last_commit = datetime.fromisoformat(dt.replace("Z", "+00:00")).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except:
            last_commit = dt

    return {
        "repoFullName": raw["nameWithOwner"],
        "repoUrl": raw["url"],
        "description": raw.get("description", ""),
        "stars": raw["stargazerCount"],
        "forks": raw["forkCount"],
        "openIssues": raw["issues"]["totalCount"],
        "closedIssues": raw["closedIssues"]["totalCount"],
        "openPRs": raw["pullRequests"]["totalCount"],
        "closedPRs": raw["closedPullRequests"]["totalCount"],
        "contributors": raw["contributors"]["totalCount"],
        "lastCommit": last_commit,
    }


def main():
    results = []
    for full_name in TARGET_REPOS:
        print(f"🔍 抓取：{full_name}")
        owner, name = full_name.split("/", 1)
        raw = fetch_repo(owner, name)
        fmt = format_data(raw)
        results.append(fmt)

    out_path = Path(__file__).parent.parent / "docs" / "repo-data.json"
    out_path.parent.mkdir(exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"✅ 完成！已保存到 {out_path}")


if __name__ == "__main__":
    main()
