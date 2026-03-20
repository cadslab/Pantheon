import json
import os
import requests

# 1. 配置
TOKEN = os.getenv('PANTHEON_TOKEN')
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"

# 2. 读取项目列表 + 自动去重（保留原顺序）
with open("config.json", "r", encoding="utf-8") as f:
    projects = json.load(f)

# 核心去重逻辑：利用列表+集合实现，保留首次出现的元素，剔除重复项
original_count = len(projects)
unique_projects = []
seen = set()
for repo in projects:
    if repo not in seen and repo.strip() != "":  # 同时过滤空字符串项
        seen.add(repo)
        unique_projects.append(repo)
projects = unique_projects  # 替换为去重后的项目列表
duplicate_count = original_count - len(projects)

results = []

# 3. GraphQL 查询语句
query = """
query GetRepoStats($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    nameWithOwner
    stargazerCount
    forkCount
    url
    openIssues: issues(states: OPEN) { totalCount }
    closedIssues: issues(states: CLOSED) { totalCount }
    openPRs: pullRequests(states: OPEN) { totalCount }
    closedPRs: pullRequests(states: CLOSED) { totalCount }
    contributors: mentionableUsers(first: 100) { totalCount }
    lastCommit: defaultBranchRef {
      target { 
        ... on Commit { 
          committedDate 
        } 
      }
    }
    primaryLanguage { name color }
  }
}
"""

# 打印去重后的扫描日志
print(f"🚀 原始项目数量: {original_count} 个")
if duplicate_count > 0:
    print(f"🔍 自动剔除重复项: {duplicate_count} 个")
print(f"✅ 开始扫描去重后项目: {len(projects)} 个...\n")

# 4. 循环请求
for repo_full_name in projects:
    # 解析 "owner/repo"
    if "/" not in repo_full_name:
        print(f"❌ 格式错误: {repo_full_name}")
        continue
    # 分割 owner 和 name（仅分割一次，兼容仓库名含/的极端情况）
    owner, name = repo_full_name.split("/", 1)
    variables = {"owner": owner, "name": name}
    try:
        response = requests.post(
            URL, headers=HEADERS, json={"query": query, "variables": variables},
            timeout=10  # 新增超时设置，防止请求挂起
        )
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络异常 {repo_full_name}: {str(e)}")
        continue

    if response.status_code == 200:
        data = response.json()
        # 检查 GraphQL 错误
        if "errors" in data:
            print(f"⚠️ 获取失败 {repo_full_name}: {data['errors'][0]['message']}")
            continue
        repo_data = data["data"]["repository"]
        if repo_data:
            results.append(
                {
                    "name": repo_data["nameWithOwner"],
                    "url": repo_data["url"],
                    "stars": repo_data["stargazerCount"],
                    "forks": repo_data["forkCount"],
                    "open_issues": repo_data["openIssues"]["totalCount"],
                    "closed_issues": repo_data["closedIssues"]["totalCount"],
                    "open_prs": repo_data["openPRs"]["totalCount"],
                    "closed_prs": repo_data["closedPRs"]["totalCount"],
                    "contributors": repo_data["contributors"]["totalCount"],
                    "last_commit": (
                        repo_data["lastCommit"]["target"]["committedDate"]
                        if repo_data["lastCommit"] and repo_data["lastCommit"]["target"]
                        else "N/A"
                    ),
                    "language": (
                        repo_data["primaryLanguage"]["name"]
                        if repo_data["primaryLanguage"]
                        else "Unknown"
                    ),
                    "language_color": (
                        repo_data["primaryLanguage"]["color"]
                        if repo_data["primaryLanguage"]
                        else "#ccc"
                    ),
                }
            )
            print(f"✅ {repo_full_name}: {repo_data['stargazerCount']} Stars")
        else:
            print(f"⚠️ 仓库不存在: {repo_full_name}")
    else:
        print(f"❌ 网络错误 {repo_full_name}: 状态码 {response.status_code}")

# 5. 排序并保存（按 Star 数倒序排列）
if results:
    results.sort(key=lambda x: x["stars"], reverse=True)
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n🎉 数据已更新并保存至 data.json，共拉取 {len(results)} 个有效项目")
else:
    print("\n⚠️ 未拉取到任何有效项目，未生成 data.json")