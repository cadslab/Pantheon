import json
import os
import shutil

import requests

# ===================== 全局配置（与原脚本一致，仅新增文件夹配置）=====================
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
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

# 新增：文件夹路径配置（需确保repos文件夹存在，内含json配置文件）
REPOS_DIR = "repos"  # 存放项目配置json的文件夹
STATUS_DIR = "status"  # 输出统计文件的目标文件夹
# 自动创建输出文件夹（不存在则创建）
os.makedirs(STATUS_DIR, exist_ok=True)


# ===================== 工具函数（原脚本复用，无修改）=====================
def generate_batch_query(batch_projects):
    """为单批项目生成批量查询语句（原逻辑完全复用）"""
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
    repo_queries = []
    query_variables = {}
    for idx, item in enumerate(batch_projects):
        repo_alias = f"repo{idx}"
        owner_var = f"owner{repo_alias}"
        name_var = f"name{repo_alias}"
        repo_queries.append(
            f"""
        {repo_alias}: repository(owner: ${owner_var}, name: ${name_var}) {{
            ...RepoStats
        }}
        """
        )
        query_variables[owner_var] = item["owner"]
        query_variables[name_var] = item["name"]
    var_defs = ", ".join([f"${k}: String!" for k in query_variables.keys()])
    full_query = f"""
    {fragment}
    query BatchGetRepoStats({var_defs}) {{
        {''.join(repo_queries)}
    }}
    """
    return full_query, query_variables


def execute_batch_query(batch_projects, batch_num, total_batch):
    """执行单批查询并解析结果（原逻辑完全复用）"""
    if not batch_projects:
        return
    batch_query, query_vars = generate_batch_query(batch_projects)
    try:
        response = requests.post(
            URL,
            headers=HEADERS,
            json={"query": batch_query, "variables": query_vars},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if "errors" in data and data["errors"]:
            print(
                f"⚠️ 第{batch_num}/{total_batch}批 - 全局查询错误: {data['errors'][0]['message']}"
            )
            return
        repo_data = data.get("data", {})
        results = []
        for idx, item in enumerate(batch_projects):
            repo_alias = f"repo{idx}"
            single_repo = repo_data.get(repo_alias)
            full_name = item["full_name"]
            if not single_repo:
                print(
                    f"⚠️ 第{batch_num}/{total_batch}批 - 仓库查询失败/不存在: {full_name}"
                )
                continue
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
        return results
    except requests.exceptions.RequestException as e:
        print(f"❌ 第{batch_num}/{total_batch}批 - 网络异常: {str(e)}")
        return []


def load_json(file_path):
    """加载JSON文件，兼容文件不存在/空文件（原逻辑完全复用）"""
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError):
        return []


# ===================== 新增：单文件处理核心函数（封装原脚本的完整逻辑）=====================
def process_single_config(config_file_name):
    """
    处理单个配置文件，生成对应统计文件
    :param config_file_name: repos文件夹下的配置文件名（如xxx.json）
    """
    # 1. 解析配置文件基础信息，动态生成输出文件路径
    config_base_name = os.path.splitext(config_file_name)[
        0
    ]  # 去掉后缀，如xxx.json -> xxx
    config_file_path = os.path.join(REPOS_DIR, config_file_name)
    # 输出文件路径：status/xxx_data_current.json、status/xxx_data_previous.json、status/xxx_data_change.json
    current_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_current.json")
    previous_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_previous.json")
    change_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_change.json")

    print("=" * 80)
    print(f"📂 开始处理配置文件：{config_file_name}")
    print(f"🎯 输出文件前缀：{config_base_name}_")
    print("=" * 80 + "\n")

    # 2. 复制当前数据为历史数据（兼容首次运行，原逻辑）
    if os.path.exists(current_file):
        shutil.copy2(current_file, previous_file)
        print(
            f"📋 已将 {os.path.basename(current_file)} 备份为 {os.path.basename(previous_file)}\n"
        )
    else:
        print(f"📌 未找到 {os.path.basename(current_file)}，首次运行，无历史数据备份\n")

    # 3. 读取项目列表 + 自动去重 + 格式校验（原逻辑）
    try:
        with open(config_file_path, "r", encoding="utf-8") as f:
            projects = json.load(f)
    except Exception as e:
        print(f"❌ 读取配置文件{config_file_name}失败：{str(e)}，跳过该文件\n")
        return

    original_count = len(projects)
    unique_projects = []
    seen = set()
    for repo in projects:
        if repo not in seen and repo.strip() != "":
            seen.add(repo)
            unique_projects.append(repo)
    projects = unique_projects
    duplicate_count = original_count - len(projects)

    print(f"🚀 原始项目数量: {original_count} 个")
    if duplicate_count > 0:
        print(f"🔍 自动剔除重复项: {duplicate_count} 个")

    # 格式校验：过滤无/的无效项目
    valid_projects = []
    for repo_full_name in projects:
        if "/" not in repo_full_name:
            print(f"❌ 格式错误，过滤项目: {repo_full_name}")
            continue
        owner, name = repo_full_name.split("/", 1)
        valid_projects.append(
            {"full_name": repo_full_name, "owner": owner, "name": name}
        )
    total_valid = len(valid_projects)
    print(f"✅ 有效项目总数: {total_valid} 个，将按每批{BATCH_SIZE}个分批查询...\n")

    # 4. 分批执行批量查询（原逻辑）
    all_results = []
    if total_valid > 0:
        total_batch = (total_valid + BATCH_SIZE - 1) // BATCH_SIZE
        for batch_num in range(total_batch):
            start = batch_num * BATCH_SIZE
            end = start + BATCH_SIZE
            batch_projects = valid_projects[start:end]
            print(
                f"\n📡 开始执行第{batch_num+1}/{total_batch}批查询，本批项目数: {len(batch_projects)}"
            )
            batch_results = execute_batch_query(
                batch_projects, batch_num + 1, total_batch
            )
            all_results.extend(batch_results)
    else:
        print(f"⚠️ 无有效项目可执行查询")
        return

    # 5. 排序并保存当前数据（原逻辑）
    if not all_results:
        print(f"\n⚠️ 未拉取到任何有效项目，跳过{os.path.basename(current_file)}生成\n")
        return

    all_results.sort(key=lambda x: x["stars"], reverse=True)
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(
        f"\n🎉 最新数据已保存至 {os.path.basename(current_file)}，共拉取 {len(all_results)} 个有效项目"
    )

    # 6. 加载历史/当前数据，计算差值并保存（原逻辑）
    previous_data = load_json(previous_file)
    current_data = load_json(current_file)
    previous_dict = {item["name"]: item for item in previous_data}
    change_data = []

    for current_item in current_data:
        repo_name = current_item["name"]
        change_item = {}
        # 处理非数值字段
        for field in NON_NUMERIC_FIELDS:
            change_item[field] = current_item.get(field, "")
        # 处理数值字段，计算差值
        if repo_name in previous_dict:
            previous_item = previous_dict[repo_name]
            for field in NUMERIC_FIELDS:
                current_val = current_item.get(field, 0)
                previous_val = previous_item.get(field, 0)
                change_item[field] = current_val - previous_val
        else:
            for field in NUMERIC_FIELDS:
                change_item[field] = current_item.get(field, 0)
        change_data.append(change_item)

    # 保存差值数据
    change_data.sort(key=lambda x: x["stars"], reverse=True)
    with open(change_file, "w", encoding="utf-8") as f:
        json.dump(change_data, f, indent=2, ensure_ascii=False)
    print(
        f"📊 差值数据已保存至 {os.path.basename(change_file)}，共 {len(change_data)} 个项目（含无变化项目）"
    )
    print(f"\n✅ 配置文件{config_file_name}处理完成！\n")


# ===================== 主程序：遍历repos文件夹下所有json文件并处理 =====================
if __name__ == "__main__":
    # 校验repos文件夹是否存在
    if not os.path.isdir(REPOS_DIR):
        print(f"❌ 错误：未找到{REPOS_DIR}文件夹，请创建并放入项目配置json文件后重试！")
        exit(1)

    # 遍历repos文件夹，仅处理.json后缀的文件
    config_files = [f for f in os.listdir(REPOS_DIR) if f.endswith(".json")]
    if not config_files:
        print(f"⚠️ {REPOS_DIR}文件夹下无任何.json配置文件，无需处理！")
        exit(0)

    print(f"🚀 开始批量处理，共发现 {len(config_files)} 个配置文件：{config_files}\n")
    # 逐个处理配置文件
    for file_name in config_files:
        process_single_config(file_name)

    print("=" * 80)
    print(f"🎉 所有配置文件处理完成！结果文件已保存至 {STATUS_DIR} 文件夹")
    print("=" * 80)
