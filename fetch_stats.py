import json
import os
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ===================== Global Configuration =====================
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
BATCH_SIZE = 5  # Request 5 repositories per batch
BATCH_SLEEP_SEC = 1.5  # Add delay between batches to avoid rate limit
RETRY_TIMES = 3  # Max retry attempts for failed request
RETRY_DELAY = 3  # Seconds to wait between retries
# Only process specified config files
TARGET_CONFIGS = ["science.json", "general.json"]
# Mapping config filename -> category name
CONFIG_CATEGORY_MAP = {"science.json": "science", "general.json": "general"}

# Numeric metrics fields (only for logical grouping, no filtering)
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
    "createdAt",
    "last_commit",
    "last_closed_pr",
    "last_open_pr",
    "last_open_issue",
    "last_closed_issue",
    "last_fork",
]
BASIC_FIELDS = ["name", "url", "language", "language_color"]

# Directory settings
REPOS_DIR = Path("repos")
STATUS_DIR = Path("status")
BIRTH_DIR = Path("birth")
for folder in (REPOS_DIR, STATUS_DIR, BIRTH_DIR):
    folder.mkdir(exist_ok=True)

# Date config for output filename (local time)
TODAY = datetime.now().strftime("%Y%m%d")
MAX_KEEP_DAYS = 7  # Keep status files for latest N days

# Graceful exit flag
STOP_FLAG = False


def handle_signal(signum, frame):
    global STOP_FLAG
    print("\n[Signal] Received stop signal, will exit after current batch...")
    STOP_FLAG = True


signal.signal(signal.SIGINT, handle_signal)


# ===================== Utility Functions =====================
def clean_old_files():
    """Remove status json files older than MAX_KEEP_DAYS (local time based)"""
    print("[Cleaner] Start scanning expired status files...")
    now = datetime.now()
    cutoff = now - timedelta(days=MAX_KEEP_DAYS)
    cutoff_date = cutoff.date()

    print(
        f"[Cleaner] Now local date: {now.date()}, cutoff date (delete if <=): {cutoff_date}"
    )

    for file_path in STATUS_DIR.glob("*.json"):
        file_name = file_path.name
        try:
            name_stem = file_path.stem
            date_str = name_stem.split("_")[-1]
            file_dt = datetime.strptime(date_str, "%Y%m%d")
            file_date = file_dt.date()

            print(f"[Cleaner] Check file: {file_name}, extracted date: {file_date}")

            if file_date <= cutoff_date:
                try:
                    file_path.unlink()
                    print(f"Removed expired file: {file_name}")
                except Exception as del_err:
                    print(f"Failed to delete file {file_name}, error: {str(del_err)}")
            else:
                print(f"Keep file (within retention period): {file_name}")

        except (ValueError, IndexError):
            print(f"Skip invalid file (no valid date suffix): {file_name}")
            continue
    print("[Cleaner] Expired file scan finished.\n")


def load_birth_cache(category: str) -> Dict[str, str]:
    """一次性加载分类对应的birth缓存到内存字典（静态缓存）"""
    birth_file = BIRTH_DIR / f"{category}_birth.json"
    cache_map: Dict[str, str] = {}
    if birth_file.exists():
        raw = load_json(birth_file)
        for entry in raw:
            repo_name = entry.get("repo")
            create_time = entry.get("created_at")
            if repo_name and create_time:
                cache_map[repo_name] = create_time
    print(
        f"[Birth Cache] Loaded {len(cache_map)} repo creation records for category [{category}]"
    )
    return cache_map


def save_birth_cache(category: str, cache_map: Dict[str, str]):
    """将内存birth缓存整体持久化写入文件（批量写入减少IO）"""
    birth_file = BIRTH_DIR / f"{category}_birth.json"
    export_list = [{"repo": k, "created_at": v} for k, v in cache_map.items()]
    try:
        with open(birth_file, "w", encoding="utf-8") as f:
            json.dump(export_list, f, indent=2, ensure_ascii=False)
        print(f"[Birth Cache] Persisted updated birth cache for [{category}]")
    except Exception as err:
        print(f"[Birth Cache] Write failed! file={birth_file.name}, err={str(err)}")


def generate_batch_query(
    batch_projects: List[Dict[str, str]],
) -> Tuple[str, Dict[str, str]]:
    """Build GraphQL batch query string for repository statistics"""
    fragment = """
    fragment RepoStats on Repository {
        nameWithOwner
        createdAt
        stargazerCount
        forkCount
        watchers { totalCount }
        url
        # Open / Closed Issues
        openIssues: issues(states: OPEN, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) {
            totalCount
            nodes { updatedAt }
        }
        closedIssues: issues(states: CLOSED, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) {
            totalCount
            nodes { closedAt }
        }
        # Open / Closed Pull Requests
        openPRs: pullRequests(states: OPEN, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) {
            totalCount
            nodes { updatedAt }
        }
        closedPRs: pullRequests(states: CLOSED, first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) {
            totalCount
            nodes { closedAt }
        }
        # Fork information
        forks(first: 1, orderBy: {field: CREATED_AT, direction: DESC}) {
            totalCount
            nodes { createdAt }
        }
        # Mentionable contributors count: first:0 reduce GraphQL cost
        contributors: mentionableUsers(first: 0) { totalCount }
        # Default branch commit info
        defaultBranchRef {
            target {
                ... on Commit {
                    committedDate
                    history(first: 0) { totalCount }
                }
            }
        }
        # Primary programming language
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


def execute_batch_query_with_retry(
    batch_projects: List[Dict[str, str]],
    batch_num: int,
    total_batch: int,
    birth_cache: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Execute GraphQL query with automatic retry mechanism"""
    if not batch_projects:
        return []

    for retry in range(RETRY_TIMES):
        try:
            batch_query, query_vars = generate_batch_query(batch_projects)
            response = requests.post(
                URL,
                headers=HEADERS,
                json={"query": batch_query, "variables": query_vars},
                timeout=45,
            )
            response.raise_for_status()
            data = response.json()

            if "errors" in data and data["errors"]:
                err_msg = data["errors"][0]["message"]
                print(
                    f"Warning Batch {batch_num}/{total_batch} Attempt {retry+1} - GraphQL error: {err_msg}"
                )
                if "rate limit exceeded" in err_msg.lower():
                    print("[!] Hit GitHub API rate limit, sleep 60s ...")
                    time.sleep(60)
                if retry == RETRY_TIMES - 1:
                    return []
                time.sleep(RETRY_DELAY)
                continue

            repo_data = data.get("data", {})
            results = []
            for idx, item in enumerate(batch_projects):
                repo_alias = f"repo{idx}"
                single_repo = repo_data.get(repo_alias)
                full_name = item["full_name"]
                if not single_repo:
                    print(
                        f"Warning Batch {batch_num}/{total_batch} - Repository not found: {full_name}"
                    )
                    continue

                # ========== 核心改动：优先读取birth缓存的created_at ==========
                if full_name in birth_cache:
                    created_at_val = birth_cache[full_name]
                else:
                    created_at_val = single_repo.get("createdAt", "N/A")

                parsed = {
                    "name": single_repo["nameWithOwner"],
                    "url": single_repo["url"],
                    "language": "Unknown",
                    "language_color": "#ccc",
                    "created_at": created_at_val,
                    "stars": single_repo["stargazerCount"],
                    "forks": single_repo["forkCount"],
                    "watching": single_repo["watchers"]["totalCount"],
                    "open_issues": single_repo["openIssues"]["totalCount"],
                    "closed_issues": single_repo["closedIssues"]["totalCount"],
                    "open_prs": single_repo["openPRs"]["totalCount"],
                    "closed_prs": single_repo["closedPRs"]["totalCount"],
                    "contributors": single_repo["contributors"]["totalCount"],
                    "commits": 0,
                    "last_commit": "N/A",
                    "last_open_issue": "N/A",
                    "last_closed_issue": "N/A",
                    "last_open_pr": "N/A",
                    "last_closed_pr": "N/A",
                    "last_fork": "N/A",
                }

                if single_repo.get("primaryLanguage"):
                    parsed["language"] = single_repo["primaryLanguage"].get(
                        "name", "Unknown"
                    )
                    parsed["language_color"] = single_repo["primaryLanguage"].get(
                        "color", "#ccc"
                    )

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

                if single_repo["openIssues"]["nodes"]:
                    parsed["last_open_issue"] = single_repo["openIssues"]["nodes"][
                        0
                    ].get("updatedAt", "N/A")
                if single_repo["closedIssues"]["nodes"]:
                    parsed["last_closed_issue"] = single_repo["closedIssues"]["nodes"][
                        0
                    ].get("closedAt", "N/A")

                if single_repo["openPRs"]["nodes"]:
                    parsed["last_open_pr"] = single_repo["openPRs"]["nodes"][0].get(
                        "updatedAt", "N/A"
                    )
                if single_repo["closedPRs"]["nodes"]:
                    parsed["last_closed_pr"] = single_repo["closedPRs"]["nodes"][0].get(
                        "closedAt", "N/A"
                    )

                if single_repo["forks"]["nodes"]:
                    parsed["last_fork"] = single_repo["forks"]["nodes"][0].get(
                        "createdAt", "N/A"
                    )

                results.append(parsed)
                print(
                    f"Success Batch {batch_num}/{total_batch} - Fetched: {full_name} | Stars: {parsed['stars']}"
                )
            return results

        except requests.exceptions.RequestException as e:
            print(
                f"Error Batch {batch_num}/{total_batch} Attempt {retry+1} - Network error: {str(e)}"
            )
            if retry == RETRY_TIMES - 1:
                return []
            time.sleep(RETRY_DELAY)
        except Exception as e:
            print(
                f"Error Batch {batch_num}/{total_batch} Attempt {retry+1} - Parse error: {str(e)}"
            )
            if retry == RETRY_TIMES - 1:
                return []
            time.sleep(RETRY_DELAY)
    return []


def load_json(file_path: Path) -> List[Any]:
    """Load json file, return empty list if file missing / invalid"""
    if not file_path.exists():
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError, FileNotFoundError):
        return []


# ===================== Core Processing Logic =====================
def process_target_config(config_file_name: str):
    """Process single target repository config file"""
    global STOP_FLAG
    if config_file_name not in TARGET_CONFIGS:
        print(f"Warning Skip non-target config file: {config_file_name}")
        return
    category = CONFIG_CATEGORY_MAP[config_file_name]
    config_base_name = Path(config_file_name).stem
    config_file_path = REPOS_DIR / config_file_name
    output_file = STATUS_DIR / f"{config_base_name}_{TODAY}.json"

    print("=" * 80)
    print(f"Start processing config: {config_file_name} [category: {category}]")
    print(f"Output file: {output_file.name}")
    print("=" * 80 + "\n")

    # 加载该分类全局birth缓存（静态内存变量）
    birth_cache = load_birth_cache(category)

    projects = load_json(config_file_path)
    if not projects:
        print(f"Error No valid repository list in {config_file_name}, skip\n")
        return

    original_count = len(projects)
    unique_projects = list(set(projects))
    duplicate_count = original_count - len(unique_projects)
    print(f"Raw repository count: {original_count}")
    if duplicate_count > 0:
        print(f"Duplicate removed: {duplicate_count}")

    valid_projects = []
    for repo_full_name in unique_projects:
        if (
            not isinstance(repo_full_name, str)
            or "/" not in repo_full_name
            or repo_full_name.strip() == ""
        ):
            print(f"Error Invalid format, skip: {repo_full_name}")
            continue
        owner, name = repo_full_name.split("/", 1)
        valid_projects.append(
            {"full_name": repo_full_name, "owner": owner, "name": name}
        )

    total_valid = len(valid_projects)
    print(f"Valid repository count: {total_valid} (batch size: {BATCH_SIZE})\n")
    if total_valid == 0:
        print(f"Warning No valid repositories to query, skip\n")
        return

    all_results = []
    total_batch = (total_valid + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_num in range(total_batch):
        if STOP_FLAG:
            print("\n[Interrupt] Stop fetching batches due to SIGINT")
            break
        start = batch_num * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_projects = valid_projects[start:end]
        print(
            f"\nExecute batch {batch_num+1}/{total_batch} (repo count: {len(batch_projects)})"
        )
        batch_results = execute_batch_query_with_retry(
            batch_projects, batch_num + 1, total_batch, birth_cache
        )
        for item in batch_results:
            repo_name = item["name"]
            create_time = item["created_at"]
            all_results.append(item)
            # 缓存不存在，则新增到内存缓存
            if repo_name not in birth_cache and create_time != "N/A":
                birth_cache[repo_name] = create_time
                print(
                    f"[Birth Cache] New repo discovered, cache {repo_name} -> {create_time}"
                )
        time.sleep(BATCH_SLEEP_SEC)

    # 批次全部跑完后，一次性写入birth文件（减少频繁IO）
    save_birth_cache(category, birth_cache)

    if not all_results:
        print(f"\nWarning No valid data fetched, skip saving\n")
        return

    all_results.sort(key=lambda x: x["stars"], reverse=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nSaved result: {output_file.name} (repos fetched: {len(all_results)})")
    print(f"{config_file_name} processing finished!\n")


# ===================== Main Entry =====================
if __name__ == "__main__":
    # 【修复点】isdir() → is_dir()
    if not REPOS_DIR.is_dir():
        print(
            f"Error: Directory '{REPOS_DIR}' not found. Create it and place {TARGET_CONFIGS} inside."
        )
        exit(1)

    if not TOKEN:
        print("ERROR: Environment variable PANTHEON_TOKEN is not set!")
        exit(1)

    clean_old_files()
    print("\n" + "-" * 60 + "\n")

    existing_configs = [f for f in os.listdir(REPOS_DIR) if f in TARGET_CONFIGS]
    if not existing_configs:
        print(f"Error Target files {TARGET_CONFIGS} not found under {REPOS_DIR}")
        exit(0)

    print(
        f"Start processing (target config count: {len(existing_configs)}) : {existing_configs}\n"
    )
    for file_name in existing_configs:
        process_target_config(file_name)
        if STOP_FLAG:
            break

    print("=" * 80)
    print(
        f"All target config files processed! Results stored in {STATUS_DIR}, retention: {MAX_KEEP_DAYS} days"
    )
    print("=" * 80)
