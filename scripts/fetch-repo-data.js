const fs = require('fs');
const path = require('path');

// ===================== 配置区：修改为你要查询的仓库 =====================
const TARGET_REPOS = [
  { owner: "cadslab", name: "Pantheon" },
  // 可添加多个仓库：{ owner: "cadslab", name: "你的仓库名" },
];
// ======================================================================

// GitHub GraphQL API 地址
const GITHUB_API = "https://api.github.com/graphql";
// 读取环境变量中的 Token
const GH_TOKEN = process.env.GH_TOKEN;

if (!GH_TOKEN) {
  console.error("错误：未设置 GH_TOKEN 环境变量");
  process.exit(1);
}

// GraphQL 查询语句（精准获取你需要的所有指标）
const GET_REPO_DATA_QUERY = `
query getRepoData($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    stargazerCount
    forkCount
    # Issues 统计
    issues(states: OPEN) { totalCount }
    closedIssues: issues(states: CLOSED) { totalCount }
    # PR 统计
    pullRequests(states: OPEN) { totalCount }
    closedPullRequests: pullRequests(states: CLOSED) { totalCount }
    # 贡献者（前100名）
    contributors: collaborators(first: 100) { totalCount }
    # 最后提交时间
    defaultBranchRef { target { committedDate } }
    # 仓库基础信息
    nameWithOwner
    url
    description
  }
}
`;

// 发起 GraphQL 请求
async function fetchRepoData(owner, name) {
  const response = await fetch(GITHUB_API, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${GH_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: GET_REPO_DATA_QUERY,
      variables: { owner, name },
    }),
  });

  const result = await response.json();
  if (result.errors) throw new Error(JSON.stringify(result.errors));
  return result.data.repository;
}

// 格式化数据
function formatRepoData(data) {
  return {
    repoFullName: data.nameWithOwner,
    repoUrl: data.url,
    description: data.description,
    stars: data.stargazerCount,
    forks: data.forkCount,
    openIssues: data.issues.totalCount,
    closedIssues: data.closedIssues.totalCount,
    openPRs: data.pullRequests.totalCount,
    closedPRs: data.closedPullRequests.totalCount,
    contributors: data.contributors.totalCount,
    lastCommit: data.defaultBranchRef?.target?.committedDate
      ? new Date(data.defaultBranchRef.target.committedDate).toLocaleString("zh-CN")
      : "无提交记录",
  };
}

// 主函数：批量查询 + 保存数据
async function main() {
  try {
    const repoDataList = [];
    for (const repo of TARGET_REPOS) {
      console.log(`正在抓取：${repo.owner}/${repo.name}`);
      const rawData = await fetchRepoData(repo.owner, repo.name);
      const formattedData = formatRepoData(rawData);
      repoDataList.push(formattedData);
    }

    // 保存数据到 docs 目录（GitHub Pages 读取）
    const outputPath = path.join(__dirname, "../docs/repo-data.json");
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, JSON.stringify(repoDataList, null, 2), "utf8");

    console.log("✅ 数据抓取完成！已保存到 docs/repo-data.json");
    console.log("数据预览：", repoDataList);
  } catch (error) {
    console.error("❌ 抓取失败：", error);
    process.exit(1);
  }
}

main();
