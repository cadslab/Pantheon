import json
import os
import shutil
import requests

# ===================== Global Configuration =====================
TOKEN = os.getenv("PANTHEON_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/graphql"
BATCH_SIZE = 20  # Query batch size (follow GitHub API limits)

# Numeric fields (added `watching` for difference calculation)
NUMERIC_FIELDS = [
    "stars",
    "forks",
    "open_issues",
    "closed_issues",
    "open_prs",
    "closed_prs",
    "contributors",
    "watching",  # New: GitHub repository watchers count
]

# Non-numeric fields (unchanged)
NON_NUMERIC_FIELDS = ["name", "url", "last_commit", "language", "language_color"]

# Directory configuration (match your repo config & output path)
REPOS_DIR = "repos"  # Store general.json here
STATUS_DIR = "status"  # Output stats files here
os.makedirs(STATUS_DIR, exist_ok=True)

# ===================== Tool Functions =====================
def generate_batch_query(batch_projects):
    """Generate GraphQL batch query (added watchers query)"""
    fragment = """
    fragment RepoStats on Repository {
        nameWithOwner
        stargazerCount
        forkCount
        watchers { totalCount }  # New: Fetch watchers total count
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
    """Execute GraphQL query & parse results (added watching field parsing)"""
    if not batch_projects:
        return []
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
            print(f"⚠️ Batch {batch_num}/{total_batch} - Global Query Error: {data['errors'][0]['message']}")
            return []

        repo_data = data.get("data", {})
        results = []
        for idx, item in enumerate(batch_projects):
            repo_alias = f"repo{idx}"
            single_repo = repo_data.get(repo_alias)
            full_name = item["full_name"]

            if not single_repo:
                print(f"⚠️ Batch {batch_num}/{total_batch} - Repo Not Found/Failed: {full_name}")
                continue

            # Parse data (added watching = watchers.totalCount)
            results.append(
                {
                    "name": single_repo["nameWithOwner"],
                    "url": single_repo["url"],
                    "stars": single_repo["stargazerCount"],
                    "forks": single_repo["forkCount"],
                    "watching": single_repo["watchers"]["totalCount"],  # New: Parse watchers count
                    "open_issues": single_repo["openIssues"]["totalCount"],
                    "closed_issues": single_repo["closedIssues"]["totalCount"],
                    "open_prs": single_repo["openPRs"]["totalCount"],
                    "closed_prs": single_repo["closedPRs"]["totalCount"],
                    "contributors": single_repo["contributors"]["totalCount"],
                    "last_commit": (
                        single_repo["lastCommit"]["target"]["committedDate"]
                        if single_repo["lastCommit"] and single_repo["lastCommit"]["target"]
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
            # Updated log: show Stars & Watching count
            print(
                f"✅ Batch {batch_num}/{total_batch} - Fetched: {full_name} | Stars: {single_repo['stargazerCount']} | Watching: {single_repo['watchers']['totalCount']}"
            )
        return results
    except requests.exceptions.RequestException as e:
        print(f"❌ Batch {batch_num}/{total_batch} - Network Error: {str(e)}")
        return []

def load_json(file_path):
    """Load JSON file (compatible with non-existent/empty files)"""
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, PermissionError):
        return []

# ===================== Core Processing Function =====================
def process_single_config(config_file_name):
    """Process single repo config file (e.g., general.json)"""
    config_base_name = os.path.splitext(config_file_name)[0]
    config_file_path = os.path.join(REPOS_DIR, config_file_name)
    # Output file paths (bind to config file name)
    current_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_current.json")
    previous_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_previous.json")
    change_file = os.path.join(STATUS_DIR, f"{config_base_name}_data_change.json")

    print("=" * 80)
    print(f"📂 Processing Config File: {config_file_name}")
    print(f"🎯 Output Prefix: {config_base_name}_")
    print("=" * 80 + "\n")

    # Backup current data to previous data (for difference calculation)
    if os.path.exists(current_file):
        shutil.copy2(current_file, previous_file)
        print(f"📋 Backed up {os.path.basename(current_file)} to {os.path.basename(previous_file)}\n")
    else:
        print(f"📌 No previous data found (first run), skip backup\n")

    # Load & validate repo list from config file
    try:
        with open(config_file_path, "r", encoding="utf-8") as f:
            projects = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load {config_file_name}: {str(e)}, skip this file\n")
        return

    # Deduplicate & filter valid repos (format: owner/name)
    original_count = len(projects)
    unique_projects = list(set(projects))  # Deduplicate
    duplicate_count = original_count - len(unique_projects)
    print(f"🚀 Original Repo Count: {original_count}")
    if duplicate_count > 0:
        print(f"🔍 Removed Duplicates: {duplicate_count}")

    valid_projects = []
    for repo_full_name in unique_projects:
        if repo_full_name.strip() == "" or "/" not in repo_full_name:
            print(f"❌ Invalid Format, Filtered: {repo_full_name}")
            continue
        owner, name = repo_full_name.split("/", 1)
        valid_projects.append({"full_name": repo_full_name, "owner": owner, "name": name})

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
        print(f"\n📡 Executing Batch {batch_num+1}/{total_batch} (Repo Count: {len(batch_projects)})")
        batch_results = execute_batch_query(batch_projects, batch_num + 1, total_batch)
        all_results.extend(batch_results)

    # Save current stats (sorted by stars descending)
    if not all_results:
        print(f"\n⚠️ No valid data fetched, skip current file generation\n")
        return
    all_results.sort(key=lambda x: x["stars"], reverse=True)
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"\n🎉 Current Data Saved to: {os.path.basename(current_file)} (Fetched: {len(all_results)})")

    # Calculate & save data changes (including watching count difference)
    previous_data = load_json(previous_file)
    current_data = load_json(current_file)
    previous_dict = {item["name"]: item for item in previous_data}
    change_data = []

    for current_item in current_data:
        repo_name = current_item["name"]
        change_item = {}
        # Add non-numeric fields
        for field in NON_NUMERIC_FIELDS:
            change_item[field] = current_item.get(field, "")
        # Calculate numeric field differences (auto include watching)
        if repo_name in previous_dict:
            previous_item = previous_dict[repo_name]
            for field in NUMERIC_FIELDS:
                current_val = current_item.get(field, 0)
                previous_val = previous_item.get(field, 0)
                change_item[field] = current_val - previous_val
        else:
            # First run: set difference to current value
            for field in NUMERIC_FIELDS:
                change_item[field] = current_item.get(field, 0)
        change_data.append(change_item)

    # Save change data (sorted by stars difference descending)
    change_data.sort(key=lambda x: x["stars"], reverse=True)
    with open(change_file, "w", encoding="utf-8") as f:
        json.dump(change_data, f, indent=2, ensure_ascii=False)
    print(f"📊 Change Data Saved to: {os.path.basename(change_file)} (Repo Count: {len(change_data)})")
    print(f"\n✅ {config_file_name} Processing Complete!\n")

# ===================== Main Program =====================
if __name__ == "__main__":
    # Check repos directory existence
    if not os.path.isdir(REPOS_DIR):
        print(f"❌ Error: {REPOS_DIR} directory not found! Create it and add your config files.")
        exit(1)
    # Get all JSON config files in repos directory
    config_files = [f for f in os.listdir(REPOS_DIR) if f.endswith(".json")]
    if not config_files:
        print(f"⚠️ No JSON config files found in {REPOS_DIR} directory.")
        exit(0)
    # Process all config files
    print(f"🚀 Starting Batch Processing (Config File Count: {len(config_files)}) : {config_files}\n")
    for file_name in config_files:
        process_single_config(file_name)
    print("=" * 80)
    print(f"🎉 All Config Files Processed! Results saved in {STATUS_DIR} directory.")
    print("=" * 80)