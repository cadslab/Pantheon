import json
import os
import shutil

import requests

# 1. 配置
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
# 定义文件路径，统一管理
CURRENT_FILE = "data_current.json"
PREVIOUS_FILE = "data_previous.json"
CHANGE_FILE = "data_change.json"
# 分批配置：每批查询的项目数量（建议20，单批节点数<GitHub限制，可自定义）
BATCH_SIZE = 20
# 定义需要计算差值的数值字段（与原数据字段名完全一致）
NUMERIC_FIELDS = [
    "stars",
    "forks",
    "open_issues",
    "closed_issues",
    "open_prs",
    "closed_prs",
    "contributors",
]
# 定义非数值字段（沿用当前值，保证数据结构一致）
NON_NUMERIC_FIELDS = ["name", "url", "last_commit", "language", "language_color"]
# 第一步：复制当前数据为历史数据（兼容文件不存在/首次运行）
if os.path.exists(CURRENT_FILE):
    shutil.copy2(CURRENT_FILE, PREVIOUS_FILE)
    print(f"📋 已将 {CURRENT_FILE} 备份为 {PREVIOUS_FILE}\n")
else:
    print(f"📌 未找到 {CURRENT_FILE}，首次运行，无历史数据备份\n")
# 2. 读取项目列表 + 自动去重（保留原顺序，与原代码一致）
with open("config.json", "r", encoding="utf-8") as f:
    projects = json.load(f)
original_count = len(projects)
unique_projects = []
seen = set()
for repo in projects:
    if repo not in seen and repo.strip() != "":
        seen.add(repo)
        unique_projects.append(repo)
projects = unique_projects
duplicate_count = original_count - len(projects)
results = []
# 打印去重日志
print(f"🚀 原始项目数量: {original_count} 个")
if duplicate_count > 0:
    print(f"🔍 自动剔除重复项: {duplicate_count} 个")
# 提前校验项目格式，过滤无效项（含/分割）
valid_projects = []
for repo_full_name in projects:
    if "/" not in repo_full_name:
        print(f"❌ 格式错误，过滤项目: {repo_full_name}")
        continue
    owner, name = repo_full_name.split("/", 1)
    valid_projects.append({"full_name": repo_full_name, "owner": owner, "name": name})
total_valid = len(valid_projects)
print(f"✅ 有效项目总数: {total_valid} 个，将按每批{BATCH_SIZE}个分批查询...\n")


# 核心工具函数1：动态生成单批的GraphQL批量查询语句
def generate_batch_query(batch_projects):
    """
    为单批项目生成批量查询语句
    batch_projects: 单批的有效项目列表
    return: 拼接后的查询语句、查询变量
    """
    # 定义公共字段片段，统一所有仓库的查询字段，减少冗余
    fragment = """
    fragment RepoStats on Repository {
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
    """
    # 动态拼接单批内的每个仓库查询节点（分配唯一别名repo0/repo1...）
    repo_queries = []
    query_variables = {}
    for idx, item in enumerate(batch_projects):
        repo_alias = f"repo{idx}"
        owner_var = f"owner{repo_alias}"
        name_var = f"name{repo_alias}"
        # 拼接单个仓库的查询节点
        repo_queries.append(
            f"""
        {repo_alias}: repository(owner: ${owner_var}, name: ${name_var}) {{
            ...RepoStats
        }}
        """
        )
        # 组装查询变量
        query_variables[owner_var] = item["owner"]
        query_variables[name_var] = item["name"]
    # 【核心修复】将fragment移到query外部，符合GraphQL语法规范
    var_defs = ", ".join([f"${k}: String!" for k in query_variables.keys()])
    full_query = f"""
    {fragment}
    query BatchGetRepoStats({var_defs}) {{
        {''.join(repo_queries)}
    }}
    """
    return full_query, query_variables


# 核心工具函数2：执行单批查询并解析结果
def execute_batch_query(batch_projects, batch_num, total_batch):
    """
    执行单批查询，解析结果并加入总结果集
    batch_projects: 单批项目列表
    batch_num: 当前批次号（从1开始）
    total_batch: 总批次数
    """
    if not batch_projects:
        return
    # 生成单批查询语句和变量
    batch_query, query_vars = generate_batch_query(batch_projects)
    try:
        # 单次POST请求拉取整批数据
        response = requests.post(
            URL,
            headers=HEADERS,
            json={"query": batch_query, "variables": query_vars},
            timeout=30,  # 批量查询延长超时时间，避免中断
        )
        response.raise_for_status()  # 抛出HTTP状态码异常（4xx/5xx）
        data = response.json()
        # 处理GraphQL顶层全局错误
        if "errors" in data and data["errors"]:
            print(
                f"⚠️ 第{batch_num}/{total_batch}批 - 全局查询错误: {data['errors'][0]['message']}"
            )
            return
        # 解析单批内每个仓库的结果
        repo_data = data.get("data", {})
        for idx, item in enumerate(batch_projects):
            repo_alias = f"repo{idx}"
            single_repo = repo_data.get(repo_alias)
            full_name = item["full_name"]
            if not single_repo:
                print(
                    f"⚠️ 第{batch_num}/{total_batch}批 - 仓库查询失败/不存在: {full_name}"
                )
                continue
            # 解析数据，与原逻辑完全一致，保证字段映射无变更
            results.append(
                {
                    "name": single_repo["nameWithOwner"],
                    "url": single_repo["url"],
                    "stars": single_repo["stargazerCount"],
                    "forks": single_repo["forkCount"],
                    "open_issues": single_repo["openIssues"]["totalCount"],
                    "closed_issues": single_repo["closedIssues"]["totalCount"],
                    "open_prs": single_repo["openPRs"]["totalCount"],
                    "closed_prs": single_repo["closedPRs"]["totalCount"],
                    "contributors": single_repo["contributors"]["totalCount"],
                    "last_commit": (
                        single_repo["lastCommit"]["target"]["committedDate"]
                        if single_repo["lastCommit"]
                        and single_repo["lastCommit"]["target"]
                        else "N/A"
                    ),
                    "language": (
                        single_repo["primaryLanguage"]["name"]
                        if single_repo["primaryLanguage"]
                        else "Unknown"
                    ),
                    "language_color": (
                        single_repo["primaryLanguage"]["color"]
                        if single_repo["primaryLanguage"]
                        else "#ccc"
                    ),
                }
            )
            print(
                f"✅ 第{batch_num}/{total_batch}批 - 拉取成功: {full_name} | Stars: {single_repo['stargazerCount']}"
            )
    except requests.exceptions.RequestException as e:
        print(f"❌ 第{batch_num}/{total_batch}批 - 网络异常: {str(e)}")


# 3. 分批执行批量查询（核心新增逻辑）
if total_valid > 0:
    # 计算总批次数（向上取整）
    total_batch = (total_valid + BATCH_SIZE - 1) // BATCH_SIZE
    # 按批次切分有效项目列表，逐批执行
    for batch_num in range(total_batch):
        start = batch_num * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_projects = valid_projects[start:end]
        print(
            f"\n📡 开始执行第{batch_num+1}/{total_batch}批查询，本批项目数: {len(batch_projects)}"
        )
        execute_batch_query(batch_projects, batch_num + 1, total_batch)
else:
    print(f"⚠️ 无有效项目可执行查询")
# 4. 排序并保存当前数据（与原代码完全一致，按stars倒序）
if results:
    results.sort(key=lambda x: x["stars"], reverse=True)
    with open(CURRENT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n🎉 最新数据已保存至 {CURRENT_FILE}，共拉取 {len(results)} 个有效项目")
else:
    print(f"\n⚠️ 未拉取到任何有效项目，未生成 {CURRENT_FILE}")
    # 无有效数据时直接退出，避免后续报错
    exit()


# 5. 加载历史/当前数据，计算差值（与原代码完全一致，保证三文件结构一致）
def load_json(file_path):
    """加载JSON文件，兼容文件不存在/空文件，返回与原数据一致的列表结构"""
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 强制保证是列表（与原数据结构一致）
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError):
        return []


# 加载数据
previous_data = load_json(PREVIOUS_FILE)
current_data = load_json(CURRENT_FILE)
# 将历史数据转为字典（以name为key，方便快速匹配）
previous_dict = {item["name"]: item for item in previous_data}
# 初始化差值结果列表
change_data = []
# 遍历当前所有项目，计算差值
for current_item in current_data:
    repo_name = current_item["name"]
    # 初始化差值项目对象
    change_item = {}
    # 1. 处理非数值字段：直接沿用当前最新值（保证结构一致）
    for field in NON_NUMERIC_FIELDS:
        change_item[field] = current_item.get(field, "")
    # 2. 处理数值字段：计算当前-历史的差值，历史无数据则差值=当前值
    if repo_name in previous_dict:
        previous_item = previous_dict[repo_name]
        for field in NUMERIC_FIELDS:
            current_val = current_item.get(field, 0)
            previous_val = previous_item.get(field, 0)
            change_item[field] = current_val - previous_val
    else:
        # 首次运行/新增项目：历史值为0，差值=当前值
        for field in NUMERIC_FIELDS:
            change_item[field] = current_item.get(field, 0)
    # 将差值项目加入结果列表
    change_data.append(change_item)
# 6. 保存差值数据（按stars差值倒序，与原文件排序逻辑一致）
if change_data:
    # 按差值的stars字段倒序，无变化则按原顺序
    change_data.sort(key=lambda x: x["stars"], reverse=True)
    with open(CHANGE_FILE, "w", encoding="utf-8") as f:
        json.dump(change_data, f, indent=2, ensure_ascii=False)
    print(
        f"📊 差值数据已保存至 {CHANGE_FILE}，共 {len(change_data)} 个项目（含无变化项目）"
    )
else:
    print(f"⚠️ 无差值数据可生成，未创建 {CHANGE_FILE}")
