import json
import os
import shutil

import requests

# ===================== Global Configuration =====================
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
BATCH_SIZE = 5  # 降低批量避免502，推荐值
# Numeric fields (added `watching` for difference calculation, add commits)
NUMERIC_FIELDS = [
    "stars",
    "forks",
    "open_issues",
    "closed_issues",
    "open_prs",
    "closed_prs",
    "contributors",
    "watching",
    "commits",
]
# Non-numeric fields (add all last* time fields)
NON_NUMERIC_FIELDS = [
    "name",
    "url",
    "last_commit",
    "language",
    "language_color",
    "last_open_issue",
    "last_closed_issue",
    "last_open_pr",
    "last_closed_pr",
    "last_fork",
]
# Directory configuration
REPOS_DIR = "repos"
STATUS_DIR = "status"
os.makedirs(STATUS_DIR, exist_ok=True)


# ===================== Tool Functions =====================
def generate_batch_query(batch_projects):
    """Generate GraphQL batch query (fixed: IssueOrder field not support CLOSED_AT)"""
    fragment = """
    fragment RepoStats on Repository {
        nameWithOwner
        stargazerCount
        forkCount
        watchers { totalCount }
        url
        # Issues (open/closed + last item time)
        openIssues: issues(states: OPEN, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { updatedAt }
        }
        closedIssues: issues(states: CLOSED, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { closedAt }
        }
        # PRs (open/closed + last item time) - 核心修复：CLOSED_AT → UPDATED_AT
        openPRs: pullRequests(states: OPEN, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { updatedAt }
        }
        closedPRs: pullRequests(states: CLOSED, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { 
            totalCount 
            nodes { closedAt }
        }
        # Forks (total + last fork time)
        forks(first: 1, orderBy: {field: CREATED_AT, direction: DESC}) {
            totalCount
            nodes { createdAt }
        }
        # Contributors
        contributors: mentionableUsers(first: 100) { totalCount }
        # Default branch - last commit + total commits
        defaultBranchRef {
            target { 
                ... on Commit { 
                    committedDate
                    history(first: 0) { totalCount }
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
    """Execute GraphQL query & parse results (修复PR时间字段无数据问题)"""
    if not batch_projects:
        return []
    batch_query, query_vars = generate_batch_query(batch_projects)
    try:
        response = requests.post(
            URL,
            headers=HEADERS,
            json={"query": batch_query, "variables": query_vars},
            timeout=45,  # 延长超时时间
        )
        response.raise_for_status()
        data = response.json()
        if "errors" in data and data["errors"]:
            print(
                f"⚠️ Batch {batch_num}/{total_batch} - Global Query Error: {data['errors'][0]['message']}"
            )
            return []
        repo_data = data.get("data", {})
        results = []
        for idx, item in enumerate(batch_projects):
            repo_alias = f"repo{idx}"
            single_repo = repo_data.get(repo_alias)
            full_name = item["full_name"]
            if not single_repo:
                print(
                    f"⚠️ Batch {batch_num}/{total_batch} - Repo Not Found/Failed: {full_name}"
                )
                continue
            # 解析基础字段
            base_fields = {
                "name": single_repo["nameWithOwner"],
                "url": single_repo["url"],
                "stars": single_repo["stargazerCount"],
                "forks": single_repo["forkCount"],
                "watching": single_repo["watchers"]["totalCount"],
                "open_issues": single_repo["openIssues"]["totalCount"],
                "closed_issues": single_repo["closedIssues"]["totalCount"],
                "open_prs": single_repo["openPRs"]["totalCount"],
                "closed_prs": single_repo["closedPRs"]["totalCount"],
                "contributors": single_repo["contributors"]["totalCount"],
            }
            # 解析提交数和最后提交时间
            base_fields["commits"] = 0
            base_fields["last_commit"] = "N/A"
            if single_repo.get("defaultBranchRef") and single_repo[
                "defaultBranchRef"
            ].get("target"):
                target = single_repo["defaultBranchRef"]["target"]
                base_fields["last_commit"] = target.get("committedDate", "N/A")
                if target.get("history"):
                    base_fields["commits"] = target["history"].get("totalCount", 0)
            # 解析Issue时间
            base_fields["last_open_issue"] = "N/A"
            if (
                single_repo["openIssues"]["nodes"]
                and len(single_repo["openIssues"]["nodes"]) > 0
            ):
                base_fields["last_open_issue"] = single_repo["openIssues"]["nodes"][
                    0
                ].get("updatedAt", "N/A")

            base_fields["last_closed_issue"] = "N/A"
            if (
                single_repo["closedIssues"]["nodes"]
                and len(single_repo["closedIssues"]["nodes"]) > 0
            ):
                base_fields["last_closed_issue"] = single_repo["closedIssues"]["nodes"][
                    0
                ].get("closedAt", "N/A")

            # ============== 核心修复：PR时间字段双层兜底校验 ==============
            base_fields["last_open_pr"] = "N/A"
            if (
                single_repo["openPRs"]["nodes"]
                and len(single_repo["openPRs"]["nodes"]) > 0
            ):
                # 先判断节点存在，再判断字段存在
                base_fields["last_open_pr"] = single_repo["openPRs"]["nodes"][0].get(
                    "updatedAt", "N/A"
                )
            else:
                print(f"ℹ️ {full_name} - 无开放PR，last_open_pr赋值为N/A")

            base_fields["last_closed_pr"] = "N/A"
            if (
                single_repo["closedPRs"]["nodes"]
                and len(single_repo["closedPRs"]["nodes"]) > 0
            ):
                # 先判断节点存在，再判断字段存在
                base_fields["last_closed_pr"] = single_repo["closedPRs"]["nodes"][
                    0
                ].get("closedAt", "N/A")
            else:
                print(f"ℹ️ {full_name} - 无关闭PR，last_closed_pr赋值为N/A")
            # ============================================================

            # 解析最后Fork时间
            base_fields["last_fork"] = "N/A"
            if single_repo["forks"]["nodes"] and len(single_repo["forks"]["nodes"]) > 0:
                base_fields["last_fork"] = single_repo["forks"]["nodes"][0].get(
                    "createdAt", "N/A"
                )

            # 解析语言和颜色
            base_fields["language"] = "Unknown"
            base_fields["language_color"] = "#ccc"
            if single_repo.get("primaryLanguage"):
                base_fields["language"] = single_repo["primaryLanguage"].get(
                    "name", "Unknown"
                )
                base_fields["language_color"] = single_repo["primaryLanguage"].get(
                    "color", "#ccc"
                )

            results.append(base_fields)
            print(
                f"✅ Batch {batch_num}/{total_batch} - Fetched: {full_name} | Stars: {base_fields['stars']} | OpenPR: {base_fields['open_prs']} | ClosedPR: {base_fields['closed_prs']}"
            )
        return results
    except requests.exceptions.RequestException as e:
        print(f"❌ Batch {batch_num}/{total_batch} - Network Error: {str(e)}")
        return []
    except Exception as e:
        print(f"❌ Batch {batch_num}/{total_batch} - Parse Error: {str(e)}")
        return []


def load_json(file_path):
    """Load JSON file (compatible with non-existent/empty files)"""
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError, FileNotFoundError):
        return []


# ===================== Core Processing Function =====================
def process_single_config(config_file_name):
    """Process single repo config file (e.g., general.json)"""
    config_base_name = os.path.splitext(config_file_name)[0]
    config_file_path = os.path.join(REPOS_DIR, config_file_name)
    current_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_current.json")
    previous_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_previous.json")
    change_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_change.json")

    print("=" * 80)
    print(f"📂 Processing Config File: {config_file_name}")
    print(f"🎯 Output Prefix: {config_base_name}_")
    print("=" * 80 + "\n")

    # Backup current data
    if os.path.exists(current_file):
        shutil.copy2(current_file, previous_file)
        print(
            f"📋 Backed up {os.path.basename(current_file)} to {os.path.basename(previous_file)}\n"
        )
    else:
        print(f"📌 No previous data found (first run), skip backup\n")

    # Load & validate repo list
    try:
        with open(config_file_path, "r", encoding="utf-8") as f:
            projects = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load {config_file_name}: {str(e)}, skip this file\n")
        return

    # Deduplicate & filter valid repos
    original_count = len(projects)
    unique_projects = list(set(projects))
    duplicate_count = original_count - len(unique_projects)
    print(f"🚀 Original Repo Count: {original_count}")
    if duplicate_count > 0:
        print(f"🔍 Removed Duplicates: {duplicate_count}")

    valid_projects = []
    for repo_full_name in unique_projects:
        if (
            not isinstance(repo_full_name, str)
            or repo_full_name.strip() == ""
            or "/" not in repo_full_name
        ):
            print(f"❌ Invalid Format, Filtered: {repo_full_name}")
            continue
        owner, name = repo_full_name.split("/", 1)
        valid_projects.append(
            {"full_name": repo_full_name, "owner": owner, "name": name}
        )
    total_valid = len(valid_projects)
    print(f"✅ Valid Repo Count: {total_valid} (batch size: {BATCH_SIZE})\n")

    if total_valid == 0:
        print(f"⚠️ No valid repos to query, exit\n")
        return

    # Execute batch queries
    all_results = []
    total_batch = (total_valid + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_num in range(total_batch):
        start = batch_num * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_projects = valid_projects[start:end]
        print(
            f"\n📡 Executing Batch {batch_num+1}/{total_batch} (Repo Count: {len(batch_projects)})"
        )
        batch_results = execute_batch_query(batch_projects, batch_num + 1, total_batch)
        all_results.extend(batch_results)

    # Save current stats
    if not all_results:
        print(f"\n⚠️ No valid data fetched, skip current file generation\n")
        return
    all_results.sort(key=lambda x: x["stars"], reverse=True)
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(
        f"\n🎉 Current Data Saved to: {os.path.basename(current_file)} (Fetched: {len(all_results)})"
    )

    # Calculate & save data changes
    previous_data = load_json(previous_file)
    current_data = load_json(current_file)
    previous_dict = {item["name"]: item for item in previous_data}
    change_data = []
    for current_item in current_data:
        repo_name = current_item["name"]
        change_item = {}
        for field in NON_NUMERIC_FIELDS:
            change_item[field] = current_item.get(field, "")
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

    # Save change data
    change_data.sort(key=lambda x: x["stars"], reverse=True)
    with open(change_file, "w", encoding="utf-8") as f:
        json.dump(change_data, f, indent=2, ensure_ascii=False)
    print(
        f"📊 Change Data Saved to: {os.path.basename(change_file)} (Repo Count: {len(change_data)})"
    )
    print(f"\n✅ {config_file_name} Processing Complete!\n")


# ===================== Main Program =====================
if __name__ == "__main__":
    if not os.path.isdir(REPOS_DIR):
        print(
            f"❌ Error: {REPOS_DIR} directory not found! Create it and add your config files."
        )
        exit(1)
    config_files = [f for f in os.listdir(REPOS_DIR) if f.endswith(".json")]
    if not config_files:
        print(f"⚠️ No JSON config files found in {REPOS_DIR} directory.")
        exit(0)
    print(
        f"🚀 Starting Batch Processing (Config File Count: {len(config_files)}) : {config_files}\n"
    )
    for file_name in config_files:
        process_single_config(file_name)
    print("=" * 80)
    print(f"🎉 All Config Files Processed! Results saved in {STATUS_DIR} directory.")
    print("=" * 80)
