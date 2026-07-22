import glob
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ===================== Global Configuration =====================
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
BATCH_SIZE = 5  # Request 5 repositories per batch
RETRY_TIMES = 3  # Max retry attempts for failed request
RETRY_DELAY = 3  # Seconds to wait between retries
# Only process specified config files
TARGET_CONFIGS = ["science.json", "general.json"]
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
# Timestamp fields
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
REPOS_DIR = "repos"
STATUS_DIR = "status"
os.makedirs(STATUS_DIR, exist_ok=True)
# Date config for output filename (local time)
TODAY = datetime.now().strftime("%Y%m%d")
MAX_KEEP_DAYS = 7  # Keep status files for latest N days


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

    status_path = Path(STATUS_DIR)
    if not status_path.exists():
        print(f"[Cleaner] Directory {STATUS_DIR} does not exist, skip")
        return

    for file_path in status_path.glob("*.json"):
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


def generate_batch_query(batch_projects):
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
        # Mentionable contributors count
        contributors: mentionableUsers(first: 100) { totalCount }
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


def execute_batch_query_with_retry(batch_projects, batch_num, total_batch):
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
                print(
                    f"Warning Batch {batch_num}/{total_batch} Attempt {retry+1} - GraphQL error: {data['errors'][0]['message']}"
                )
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

                parsed = {
                    "name": single_repo["nameWithOwner"],
                    "url": single_repo["url"],
                    "language": "Unknown",
                    "language_color": "#ccc",
                    "created_at": single_repo.get("createdAt", "N/A"),
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


def load_json(file_path):
    """Load json file, return empty list if file missing / invalid"""
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError, FileNotFoundError):
        return []


# ===================== Core Processing Logic =====================
def process_target_config(config_file_name):
    """Process single target repository config file"""
    if config_file_name not in TARGET_CONFIGS:
        print(f"Warning Skip non-target config file: {config_file_name}")
        return

    config_base_name = os.path.splitext(config_file_name)[0]
    config_file_path = os.path.join(REPOS_DIR, config_file_name)
    output_file = os.path.join(STATUS_DIR, f"{config_base_name}_{TODAY}.json")

    print("=" * 80)
    print(f"Start processing config: {config_file_name}")
    print(f"Output file: {os.path.basename(output_file)}")
    print("=" * 80 + "\n")

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
        start = batch_num * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_projects = valid_projects[start:end]
        print(
            f"\nExecute batch {batch_num+1}/{total_batch} (repo count: {len(batch_projects)})"
        )
        batch_results = execute_batch_query_with_retry(
            batch_projects, batch_num + 1, total_batch
        )
        all_results.extend(batch_results)

    if not all_results:
        print(f"\nWarning No valid data fetched, skip saving\n")
        return

    all_results.sort(key=lambda x: x["stars"], reverse=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)
    print(
        f"\nSaved result: {os.path.basename(output_file)} (repos fetched: {len(all_results)})"
    )
    print(f"{config_file_name} processing finished!\n")


# ===================== Main Entry =====================
if __name__ == "__main__":
    if not os.path.isdir(REPOS_DIR):
        print(
            f"Error: Directory '{REPOS_DIR}' not found. Create it and place {TARGET_CONFIGS} inside."
        )
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

    print("=" * 80)
    print(
        f"All target config files processed! Results stored in {STATUS_DIR}, retention: {MAX_KEEP_DAYS} days"
    )
    print("=" * 80)
