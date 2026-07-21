import glob
import json
import os
import shutil
from datetime import datetime, timedelta

import requests

# ===================== Global Configuration =====================
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
BATCH_SIZE = 5  # 每次请求5个项目
RETRY_TIMES = 3  # 请求失败重试次数
RETRY_DELAY = 3  # 重试间隔(秒)
# 仅处理指定配置文件
TARGET_CONFIGS = ["science.json", "general.json"]
# 要查询的所有字段（分类型仅作逻辑区分，无实际过滤）
NUMERIC_FIELDS = [
    "stars",
    "forks",
    "watching",
    "open_issues",
    "closed_issues",
    "open_prs",
    "closed_prs",
    "contributors",
    "commits",
]
TIME_FIELDS = [
    "createdAt",  # <--- 新增：项目创建时间
    "last_commit",
    "last_closed_pr",
    "last_open_pr",
    "last_open_issue",
    "last_closed_issue",
    "last_fork",
]
BASIC_FIELDS = ["name", "url", "language", "language_color"]
# 目录配置
REPOS_DIR = "repos"
STATUS_DIR = "status"
os.makedirs(STATUS_DIR, exist_ok=True)
# 日期相关
TODAY = datetime.now().strftime("%Y%m%d")  # 今日日期（文件名用）
MAX_KEEP_DAYS = 7  # 最多保留7天文件


# ===================== 工具函数 =====================
def clean_old_files():
    """清理status目录下超过7天的历史文件"""
    cutoff = datetime.now() - timedelta(days=MAX_KEEP_DAYS)
    # 匹配所有*_日期.json格式的文件
    file_pattern = os.path.join(STATUS_DIR, "*.json")
    for file_path in glob.glob(file_pattern):
        try:
            # 提取文件名中的日期部分
            file_name = os.path.basename(file_path)
            date_str = file_name.split("_")[-1].replace(".json", "")
            file_date = datetime.strptime(date_str, "%Y%m%d")
            if file_date < cutoff:
                os.remove(file_path)
                print(f"🗑️  清理过期文件: {file_name}")
        except (ValueError, OSError):
            # 日期格式不匹配/文件删除失败则跳过
            continue


def generate_batch_query(batch_projects):
    """生成GraphQL批量查询语句"""
    fragment = """
    fragment RepoStats on Repository {
        nameWithOwner
        createdAt               # <--- 新增：项目创建时间
        stargazerCount
        forkCount
        watchers { totalCount }
        url
        # Issues (open/closed 数量 + 最后操作时间)
        openIssues: issues(states: OPEN, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { updatedAt }
        }
        closedIssues: issues(states: CLOSED, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { closedAt }
        }
        # PRs (open/closed 数量 + 最后操作时间)
        openPRs: pullRequests(states: OPEN, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { updatedAt }
        }
        closedPRs: pullRequests(states: CLOSED, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { closedAt }
        }
        # Forks (数量 + 最后fork时间)
        forks(first: 1, orderBy: {field: CREATED_AT, direction: DESC}) {
            totalCount
            nodes { createdAt }
        }
        # 贡献者数量
        contributors: mentionableUsers(first: 100) { totalCount }
        # 默认分支 - 最后提交时间 + 总提交数
        defaultBranchRef {
            target { 
                ... on Commit { 
                    committedDate
                    history(first: 0) { totalCount }
                } 
            }
        }
        # 主开发语言及颜色
        primaryLanguage { name color }
    }
    """
    repo_queries = []
    query_variables = {}
    for idx, item in enumerate(batch_projects):
        repo_alias = f"repo{idx}"
        owner_var = f"owner{repo_alias}"
        name_var = f"name{repo_alias}"
        repo_queries.append(f"""
        {repo_alias}: repository(owner: ${owner_var}, name: ${name_var}) {{
            ...RepoStats
        }}
        """)
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


def execute_batch_query_with_retry(batch_projects, batch_num, total_batch):
    """带重试机制的GraphQL查询执行"""
    if not batch_projects:
        return []
    # 重试逻辑
    for retry in range(RETRY_TIMES):
        try:
            batch_query, query_vars = generate_batch_query(batch_projects)
            response = requests.post(
                URL,
                headers=HEADERS,
                json={"query": batch_query, "variables": query_vars},
                timeout=45,
            )
            response.raise_for_status()  # 触发HTTP错误异常
            data = response.json()

            # 处理GraphQL业务错误
            if "errors" in data and data["errors"]:
                print(
                    f"⚠️  Batch {batch_num}/{total_batch} 第{retry+1}次重试 - GraphQL错误: {data['errors'][0]['message']}"
                )
                if retry == RETRY_TIMES - 1:  # 最后一次重试仍失败
                    return []
                continue

            repo_data = data.get("data", {})
            results = []
            for idx, item in enumerate(batch_projects):
                repo_alias = f"repo{idx}"
                single_repo = repo_data.get(repo_alias)
                full_name = item["full_name"]
                if not single_repo:
                    print(
                        f"⚠️  Batch {batch_num}/{total_batch} - 仓库未找到: {full_name}"
                    )
                    continue

                # 解析所有指定字段
                parsed = {
                    # 基础字段
                    "name": single_repo["nameWithOwner"],
                    "url": single_repo["url"],
                    "language": "Unknown",
                    "language_color": "#ccc",
                    # 新增：项目创建时间
                    "created_at": single_repo.get("createdAt", "N/A"),  # <--- 新增
                    # 数值指标
                    "stars": single_repo["stargazerCount"],
                    "forks": single_repo["forkCount"],
                    "watching": single_repo["watchers"]["totalCount"],
                    "open_issues": single_repo["openIssues"]["totalCount"],
                    "closed_issues": single_repo["closedIssues"]["totalCount"],
                    "open_prs": single_repo["openPRs"]["totalCount"],
                    "closed_prs": single_repo["closedPRs"]["totalCount"],
                    "contributors": single_repo["contributors"]["totalCount"],
                    "commits": 0,
                    # 时间指标初始值
                    "last_commit": "N/A",
                    "last_open_issue": "N/A",
                    "last_closed_issue": "N/A",
                    "last_open_pr": "N/A",
                    "last_closed_pr": "N/A",
                    "last_fork": "N/A",
                }

                # 解析主语言
                if single_repo.get("primaryLanguage"):
                    parsed["language"] = single_repo["primaryLanguage"].get(
                        "name", "Unknown"
                    )
                    parsed["language_color"] = single_repo["primaryLanguage"].get(
                        "color", "#ccc"
                    )

                # 解析提交数和最后提交时间
                if single_repo.get("defaultBranchRef") and single_repo[
                    "defaultBranchRef"
                ].get("target"):
                    target = single_repo["defaultBranchRef"]["target"]
                    parsed["last_commit"] = target.get("committedDate", "N/A")
                    parsed["commits"] = (
                        target["history"].get("totalCount", 0)
                        if target.get("history")
                        else 0
                    )

                # 解析Issue时间
                if single_repo["openIssues"]["nodes"]:
                    parsed["last_open_issue"] = single_repo["openIssues"]["nodes"][
                        0
                    ].get("updatedAt", "N/A")
                if single_repo["closedIssues"]["nodes"]:
                    parsed["last_closed_issue"] = single_repo["closedIssues"]["nodes"][
                        0
                    ].get("closedAt", "N/A")

                # 解析PR时间
                if single_repo["openPRs"]["nodes"]:
                    parsed["last_open_pr"] = single_repo["openPRs"]["nodes"][0].get(
                        "updatedAt", "N/A"
                    )
                if single_repo["closedPRs"]["nodes"]:
                    parsed["last_closed_pr"] = single_repo["closedPRs"]["nodes"][0].get(
                        "closedAt", "N/A"
                    )

                # 解析最后Fork时间
                if single_repo["forks"]["nodes"]:
                    parsed["last_fork"] = single_repo["forks"]["nodes"][0].get(
                        "createdAt", "N/A"
                    )

                results.append(parsed)
                print(
                    f"✅ Batch {batch_num}/{total_batch} - 成功获取: {full_name} | Stars: {parsed['stars']}"
                )
            return results

        except requests.exceptions.RequestException as e:
            print(
                f"❌ Batch {batch_num}/{total_batch} 第{retry+1}次重试 - 网络错误: {str(e)}"
            )
            if retry == RETRY_TIMES - 1:
                return []
            # 重试前延迟
            import time

            time.sleep(RETRY_DELAY)
        except Exception as e:
            print(
                f"❌ Batch {batch_num}/{total_batch} 第{retry+1}次重试 - 解析错误: {str(e)}"
            )
            if retry == RETRY_TIMES - 1:
                return []
            import time

            time.sleep(RETRY_DELAY)
    return []


def load_json(file_path):
    """加载JSON文件，兼容文件不存在/空文件"""
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError, FileNotFoundError):
        return []


# ===================== 核心处理函数 =====================
def process_target_config(config_file_name):
    """处理指定的配置文件（science.json/general.json）"""
    if config_file_name not in TARGET_CONFIGS:
        print(f"⚠️  跳过非目标配置文件: {config_file_name}")
        return

    config_base_name = os.path.splitext(config_file_name)[0]
    config_file_path = os.path.join(REPOS_DIR, config_file_name)
    # 输出文件命名：前缀_日期.json
    output_file = os.path.join(STATUS_DIR, f"{config_base_name}_{TODAY}.json")

    print("=" * 80)
    print(f"📂 开始处理配置文件: {config_file_name}")
    print(f"📤 输出文件: {os.path.basename(output_file)}")
    print("=" * 80 + "\n")

    # 加载仓库列表
    projects = load_json(config_file_path)
    if not projects:
        print(f"❌ {config_file_name} 无有效仓库数据，跳过\n")
        return

    # 去重+校验仓库格式（owner/name）
    original_count = len(projects)
    unique_projects = list(set(projects))  # 去重
    duplicate_count = original_count - len(unique_projects)
    print(f"🚀 原始仓库数: {original_count}")
    if duplicate_count > 0:
        print(f"🔍 去重数量: {duplicate_count}")

    valid_projects = []
    for repo_full_name in unique_projects:
        if (
            not isinstance(repo_full_name, str)
            or "/" not in repo_full_name
            or repo_full_name.strip() == ""
        ):
            print(f"❌ 格式无效，过滤: {repo_full_name}")
            continue
        owner, name = repo_full_name.split("/", 1)
        valid_projects.append(
            {"full_name": repo_full_name, "owner": owner, "name": name}
        )

    total_valid = len(valid_projects)
    print(f"✅ 有效仓库数: {total_valid} (分批大小: {BATCH_SIZE})\n")
    if total_valid == 0:
        print(f"⚠️  无有效仓库可查询，跳过\n")
        return

    # 分批执行查询
    all_results = []
    total_batch = (total_valid + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_num in range(total_batch):
        start = batch_num * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_projects = valid_projects[start:end]
        print(
            f"\n📡 执行批次 {batch_num+1}/{total_batch} (本次仓库数: {len(batch_projects)})"
        )
        batch_results = execute_batch_query_with_retry(
            batch_projects, batch_num + 1, total_batch
        )
        all_results.extend(batch_results)

    # 保存结果（按stars降序排序）
    if not all_results:
        print(f"\n⚠️  未获取到任何有效仓库数据，跳过保存\n")
        return

    all_results.sort(key=lambda x: x["stars"], reverse=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)
    print(
        f"\n🎉 结果已保存: {os.path.basename(output_file)} (实际获取: {len(all_results)}个仓库)"
    )
    print(f"✅ {config_file_name} 处理完成!\n")


# ===================== 主程序 =====================
if __name__ == "__main__":
    # 前置检查：repos目录是否存在
    if not os.path.isdir(REPOS_DIR):
        print(f"❌ 错误: 未找到{REPOS_DIR}目录，请创建并放入{TARGET_CONFIGS}文件")
        exit(1)

    # 清理过期文件（保留7天）
    clean_old_files()
    print("\n" + "-" * 60 + "\n")

    # 检查目标配置文件是否存在
    existing_configs = [f for f in os.listdir(REPOS_DIR) if f in TARGET_CONFIGS]
    if not existing_configs:
        print(f"❌ {REPOS_DIR}目录下未找到目标文件: {TARGET_CONFIGS}")
        exit(0)

    # 批量处理目标配置文件
    print(
        f"🚀 启动批量处理 (目标配置文件数: {len(existing_configs)}) : {existing_configs}\n"
    )
    for file_name in existing_configs:
        process_target_config(file_name)

    print("=" * 80)
    print(f"🎉 所有目标配置文件处理完成！结果已保存至{STATUS_DIR}目录（最多保留7天）")
    print("=" * 80)
