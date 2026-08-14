# PR Analytics - StarRocks PR 智能分析工具

默认通过 Codex 对 StarRocks 开源仓库（`StarRocks/starrocks`）与企业仓库（`CelerData`=历史 / `MirrorShip`=当前源）的合并 PR 生成结构化摘要，也可切换为 Gemini 结构化摘要或 Ollama 双语摘要，再生成向量 embedding 并存入 StarRocks（Primary Key + HNSW 向量索引），支持多仓库联合的语义搜索、关键词过滤、AI 修复分析、Web UI 和 agent HTTP API。

## 架构

```
gh CLI 拉取 PR  →  Codex/Gemini 生成结构化摘要 / Ollama 生成双语摘要  →  bge-m3 生成 embedding
                                                                       ↓
Web UI ← 语义搜索 / searchable_text 关键词过滤 ← StarRocks (Primary Key + HNSW 向量索引)
```

- **摘要**：默认用 Codex 批量调用 `pr-json-summarizer`，基于 PR metadata 和 diff 生成 `diff_keywords` 和更完整的 `searchable_text`；也可切换为 Gemini 执行同样的结构化摘要流程，或切换为 Ollama 只生成中英文摘要（此时 `diff_keywords` 为空，`searchable_text` 使用 title + 双语摘要兜底）
- **embedding**：基于 `searchable_text` 生成，bge-m3 多语言模型支持中英文混合查询
- **关键词过滤**：只查询 `searchable_text` 的 LIKE / MATCH_ALL / MATCH_ANY
- **PR body 清洗**：仅提取 `What I'm doing` section 之前的内容，过滤 template 噪声和 `Fixes #issue` 行
- **Backport 处理**：自动识别 backport PR（title 含 `(backport #xxx)`），enrich 时跳过摘要生成，通过 `pr_versions` 表将版本信息关联到主 PR
- **多仓库（oss + 企业）**：`pr.py` 内置 `REPOS` 注册表，每条带 `kind`（`oss`/`enterprise`）、`label`、`active`，其中 `kind` 驱动所有「企业 vs 开源」分支逻辑，所以换/加源仓库是改配置而非改代码。当前三个：`oss` → `StarRocks/starrocks`；`cd` → `CelerData/celerdata-enterprise`（**已冻结的历史**——企业源已迁走，daemon 不再轮询，但历史数据保留可查）；`ms` → `MirrorShipDB/mirrorship-enterprise`（**当前企业源**）。各仓库数据分目录（`data/<repo>/raw|enriched`，oss 沿用旧 `data/raw`）。`pr_data`/`pr_versions` 以 `(pr_number, repo)`（`pr_versions` 再加 `version`）为联合主键——各仓 PR 号区间重叠，联合主键避免互相覆盖；检索默认联合全部仓库，也可用 `--repo` / `repo=` 限定单仓库（`oss`/`cd`/`ms`/`all`）。
- **企业 PR 四分类**：fetch 阶段按标题 / 标签 / base 分支给每条企业 PR 打标（写入 raw JSON 的 `pr_kind` 字段），决定它进入哪条链路：
  - `sync`（约 90%，标题含 `(sync #N)` 或带 `sync` 标签）：从开源同步落地的 PR。不进 `pr_data`、不生成摘要，只由 `link-sync` 写入 `pr_sync(oss_pr, ent_pr, ent_repo, version, ent_merged_at)` 映射表（`ent_repo` 区分同步落到哪个企业仓库）。
  - `conflict_fix`（base 分支形如 `*-sync-pr-<N>`，或标题匹配冲突解决模式）：同步时人工解决冲突产生的 PR，照常 enrich，但 `change_type` 被强制标记为 `SyncFix`。
  - `backport`（企业内部 `(backport #E)`）：向 `-ee` 发布分支的内部回合，跳过 enrich，由 `link-backport --repo ms` 写入 `pr_versions`。
  - `exclusive`（其余情况）：企业独有 PR，正常走摘要 + embedding 流程。

## 前置准备

```bash
# Ollama
brew install ollama
ollama serve
ollama pull bge-m3         # embedding 模型 (1024维, 多语言)
ollama pull qwen3.5:9b     # 摘要模型

# StarRocks 3.4+ (需支持向量索引)
pip install pymysql

# GitHub CLI，用于拉取 PR metadata；Codex/Gemini provider 还会读取 PR diff
brew install gh
gh auth login

# Codex 是默认结构化摘要 provider；Gemini 可选
# Codex npm 包需要 Node.js 18+，推荐 20+
codex --version
gemini --version
```

## 使用流程

### 常规工作流 (一键执行)

包含以下 Step 1、2、5、6 的所有步骤（`--repo ms` 时额外包含 Step 7 的 `link-sync`），适用于日常增量更新。`--repo` 默认 `oss`，向后兼容。

> **在对企业仓库运行任何 `--repo cd|ms` 命令（包括下面的 `pipeline --repo cd|ms`）之前**，如果 StarRocks 里已经有旧版（本次 repo-aware 改动之前）建好的 `pr_data`/`pr_versions` 表，必须先完成 Step 4 的 `migrate-repo` 迁移并升级重启 `web.py`，否则会报错或导致数据覆盖，详见 Step 4。全新部署（`init-table` 直接建出的表）没有这个问题，可以跳过。

```bash
# 更新过去 1 天的数据（开源仓库，--repo 默认 oss）
python3 pr.py pipeline --days 1

# 更新指定时间范围的数据
python3 pr.py pipeline --since 2025-04-01 --until 2025-04-30

# enrich 阶段从新到旧处理
python3 pr.py pipeline --since 2025-04-01 --until 2025-04-30 --reverse

# 企业仓库：fetch → link-sync → enrich → load → link-backport（先确认已完成 Step 4 迁移）
python3 pr.py pipeline --repo ms --since 2025-04-01 --until 2025-04-30
```

### 1. 拉取 PR 原始数据

按天存储，按周分批拉取（避免 GitHub API 限制），增量去重。

```bash
python3 pr.py fetch --days 1
python3 pr.py fetch --days 30
python3 pr.py fetch --since 2025-04-01
python3 pr.py fetch --since 2025-04-01 --until 2025-04-30
python3 pr.py fetch --repo ms --since 2025-04-01 --until 2025-04-30
```

`--repo` 可选 `oss`（默认）/ `cd` / `ms`。输出：`oss` 写入 `data/raw/pr_raw_20250401.json`、`data/raw/pr_raw_20250402.json` ...；`cd` 写入 `data/cd/raw/pr_raw_20250401.json` ...，文件名格式相同。

拉取企业仓库（`--repo ms`）时，会额外按标题 / 标签 / base 分支给每条 PR 打上四分类标签（`pr_kind`：`sync` / `conflict_fix` / `backport` / `exclusive`），连同 `base_ref`、`sync_source_pr`、派生出的 `version` 一起写入 raw JSON；命令结束时会打印各分类计数（`Kind breakdown`）。

### 2. AI 增强：生成摘要 + embedding

断点续跑，自动跳过已处理的 PR。Backport PR、以及企业仓库中被分类为 `sync` 的 PR 会自动跳过（不生成摘要）；`conflict_fix` 和 `exclusive` 分类的企业 PR 正常 enrich。

```bash
python3 pr.py enrich --days 1
python3 pr.py enrich --file data/raw/pr_raw_20250401.json
python3 pr.py enrich --since 2025-04-01
python3 pr.py enrich --since 2025-04-01 --until 2025-04-30
python3 pr.py enrich --since 2025-04-01 --until 2025-04-30 --reverse  # 从新到旧处理
python3 pr.py enrich --file data/raw/pr_raw_20250401.json --output /tmp/pr_enriched_20250401.json
python3 pr.py enrich --repo ms --since 2025-04-01 --until 2025-04-30

# 默认使用 Codex 批量生成结构化摘要，继承当前执行目录作为工作目录
PR_SUMMARY_BATCH_SIZE=5 python3 pr.py enrich --days 1

# 使用 Gemini 批量生成结构化摘要
PR_SUMMARY_PROVIDER=gemini PR_SUMMARY_BATCH_SIZE=5 python3 pr.py enrich --days 1

# 使用 Ollama 生成双语摘要
PR_SUMMARY_PROVIDER=ollama python3 pr.py enrich --days 1
```

Codex/Gemini provider 会批量调用 `pr-json-summarizer`，开源 PR 的 prompt 形如 `用 pr-json-summarizer 分析 pr 123,456,789`；企业 PR（`--repo ms`）改用完整 GitHub URL，形如 `用 pr-json-summarizer 分析 pr https://github.com/CelerData/celerdata-enterprise/pull/59245,...`，由 agent 自行拉取 PR metadata 和 diff。Codex 调用使用 `--dangerously-bypass-approvals-and-sandbox`，且不传 `-C`，因此会继承当前执行目录；请在包含所需仓库上下文的目录中运行。

输出：`oss` 写入 `data/enriched/pr_enriched_20250401.json` ...；`cd` 写入 `data/cd/enriched/pr_enriched_20250401.json` ...

### 3. 建表（首次）

创建 `pr_data`（Primary Key `(pr_number, repo)` + HNSW 向量索引）、`pr_versions`（版本映射表，Primary Key `(pr_number, repo, version)`）和 `pr_sync`（开源↔企业同步映射表，Primary Key `(oss_pr, ent_pr, ent_repo)`）三张表（schema v2，repo-aware，全新部署直接得到最终 schema）。如果表已存在会报错，使用 `--force` 强制重建（会连带删除三张表）。

```bash
python3 pr.py init-table            # 表已存在则报错
python3 pr.py init-table --force    # 强制删除重建（含 pr_sync）
```

当前 `pr_data` 依赖 `diff_keywords`、`searchable_text` 和 `searchable_text_idx`，从缺少这些列的旧表升级时建议直接 `init-table --force` 后重新导入 enriched 数据。如果旧表已经有 `diff_keywords` 等列、只是缺少 `repo` 列（即本次双仓库改动之前建的表），**不要用 `--force`**，改用下一节的 `migrate-repo` 做原地迁移，避免丢失已经生成过的 embedding。

### 4. 迁移旧库到 repo-aware schema（migrate-repo）

**只需要对"在本次双仓库改动之前就已经建表并导入过数据"的旧库执行一次**；全新部署直接用上一节 `init-table` 建出的表已经是 repo-aware schema，跳过本节。

```bash
python3 pr.py migrate-repo
```

行为：
- 检测 `pr_data` 是否已有 `repo` 列，已存在则确保 `pr_sync` schema（缺 `ent_repo` 列的旧 `pr_sync` 会被删除重建，之后需重跑 `link-sync --repo cd` 和 `--repo ms` 回填）后直接退出（幂等，可重复执行）。
- 否则新建 `pr_data_new` / `pr_versions_new`（新 schema），用 `INSERT INTO ... SELECT` 把旧数据整体标记为 `repo='oss'` 拷贝过去（embedding 原样复制，不重新 enrich），核对新旧表行数一致后再 `RENAME` 交换；旧表分别保留为 `pr_data_bak` / `pr_versions_bak`，验证无误后手动 `DROP TABLE` 清理。
- 最后建 `pr_sync` 表。

**部署顺序（必须严格遵守，不能颠倒）**：

1. 先执行 `python3 pr.py migrate-repo`（此时旧 `web.py` 仍可正常读写，因为迁移只加列不删列）。
2. 迁移成功后，再升级并重启 `web.py`（新代码的 SQL 引用了 `repo` 列，旧库未迁移会直接报错）。
3. 最后才开始跑企业仓库数据（`pipeline --repo ms` / `fetch --repo ms` 等）。

原因：新代码查询一张还没有 `repo` 列的旧表会直接报错；企业数据一旦在旧 schema（无 `repo` 列）下写入 `pr_data`/`pr_versions`，会退化成单列主键，与开源 PR 号冲突时互相覆盖，无法再原地补救。

### 5. 导入 StarRocks

重复导入自动更新（Primary Key 去重）。导入时自动写入主版本到 `pr_versions`。

```bash
python3 pr.py load --days 1
python3 pr.py load --file data/enriched/pr_enriched_20250401.json
python3 pr.py load --since 2025-04-01
python3 pr.py load --since 2025-04-01 --until 2025-04-30
python3 pr.py load --repo ms --since 2025-04-01 --until 2025-04-30
```

### 6. 关联 Backport 版本

扫描 raw 文件，提取 backport PR 的版本信息，写入 `pr_versions` 表关联到主 PR。

```bash
python3 pr.py link-backport --days 1
python3 pr.py link-backport --file data/raw/pr_raw_20250401.json
python3 pr.py link-backport --since 2025-04-01
python3 pr.py link-backport --since 2025-04-01 --until 2025-04-30
python3 pr.py link-backport --repo ms --since 2025-04-01 --until 2025-04-30
```

`enrich`、`load`、`link-backport` 现在都支持 `--days N` 和 `--repo oss|cd|ms`（默认 `oss`），和 `fetch` / `pipeline` 一样适合做最近 N 天的增量补处理；指定 `--file` 或 `--since` 后会忽略 `--days`。

### 7. 关联企业同步映射（link-sync）

只适用于企业仓库：扫描企业仓库（`cd`/`ms`）的 raw 文件，把分类为 `sync`（标题含 `(sync #N)`）的 PR 提取成 `(oss_pr, ent_pr, ent_repo, version, ent_merged_at)`，写入 `pr_sync` 表（幂等 upsert）。这一步只读已经 fetch 下来的 raw JSON，不调用 codex/gemini/ollama。

```bash
python3 pr.py link-sync --since 2025-04-01 --until 2025-04-30
python3 pr.py link-sync --days 1
python3 pr.py link-sync --file data/cd/raw/pr_raw_20250401.json
```

`link-sync` 仅接受企业仓库 `--repo cd|ms`（默认 `ms`，当前企业源），传入 `oss` 或未知值会被 CLI 直接拒绝。`pipeline --repo cd|ms` 会在 `fetch` 之后（`enrich` 之前）自动执行这一步，通常不需要单独调用。

### 8. 语义搜索（命令行）

```bash
python3 pr.py search "内存泄漏"
python3 pr.py search "materialized view refresh" --top 5
python3 pr.py search "compaction 卡住" --repo ms
python3 pr.py search "compaction 卡住" --repo all
```

支持中英文混合查询。`--repo` 可选 `oss` / `cd` / `ms` / `all`，默认 `all`（开源 + 企业联合检索，按相似度混排，每条结果前缀 `[OSS]` / `[CD]` / `[MS]` 标识来源仓库）。

### 9. Web UI

```bash
python3 web.py                # 默认 8888 端口
python3 web.py --port 9090    # 自定义端口
python3 web.py --host 127.0.0.1 --port 9090
```

打开 `http://localhost:8888`，支持：
- **语义搜索**（自然语言，调 Ollama 生成 embedding）
- **AI 修复分析**（调用 `/api/analyze`，先召回最相关 PR，再让 LLM 判断“这个问题是否已被历史 PR 修复”）
- **关键词过滤**（匹配 `searchable_text`，其中包含 title、中英文摘要、diff keywords 等检索文本）。支持四种模式切换：
  - `自动`（默认）：优先使用 `LIKE` 匹配全字段，无结果则自动降级尝试 `MATCH_ALL`，最后尝试 `MATCH_ANY`。
  - `LIKE`：标准 SQL 模糊匹配。
  - `MATCH ALL` / `MATCH ANY`：使用 StarRocks 倒排索引分词匹配 `searchable_text`。
- **筛选条件**：Repo（全部 / `OSS` / `CD` / `MS`，下拉按 `REPOS` 动态生成）/ PR# / Module / Type / Version / Author / 时间范围
- 每条结果带 `OSS` / `CD` / `MS` 徽章标识来源仓库，PR 链接按所属 repo 拼接到对应 GitHub 仓库
- 每个 PR 展示所有关联版本（含 backport），版本号可点击跳转对应 PR
- 开源 PR 如果已经同步进企业版，额外展示 `CD #`/`MS #<企业PR号>` 徽章（形如 `MS #59488 (4.1-ee)`，前缀是同步落到的企业仓库；此处为 PR 自身的直接同步落点，含 backport 间接同步的完整落点见 agent 接口 `/api/agent/pr/<number>`），点击跳转企业仓库对应 PR
- 指定 PR# 过滤时，如果输入的是 backport PR 号，会自动反查并返回主 PR；不限定 Repo（即“全部”）时开源和企业两侧会分别尝试反查
- **AI 分析抽屉**（右上角 “✨ AI 分析” 按钮）：基于 codex CLI 调用 `pr-fix-finder` skill，流式输出分析过程和结论，支持多轮追问。详见下方"AI 分析抽屉"章节。

### 10. 定时运行（守护进程）

`daemon.py` 会每小时自动依次执行开源和企业两个仓库的 `pipeline`，适合长期挂在宿主机上做增量同步。

```bash
python3 daemon.py
nohup python3 daemon.py > daemon.log 2>&1 &
tail -f daemon.log
```

实现细节：
- 每小时运行一次
- 内部对每个 active 仓库（由 `REPOS` 的 `active` 标志决定，当前 `oss` + `ms`；`cd` 已冻结、不再轮询）依次跑 `python3 pr.py pipeline --repo <repo> --days 2`（企业仓库侧会自动带上 `link-sync`）
- 启动时会自动切到 `tools/pr_analytics/` 目录，避免相对路径出错

### 11. HTTP API / Agent 接口

启动 `python3 web.py` 后，除了浏览器页面，还会暴露可供脚本和 agent 调用的 HTTP 接口。

常用查询参数：
- 仓库范围：`repo=oss|cd|ms|all`（默认 `all` 联合检索）
- 通用筛选：`pr_number`、`module`、`change_type`、`version`、`author`、`since`、`until`
- 结果条数：`top`
- 关键词过滤模式：`match_mode=auto|like|all|any`

| Endpoint | 说明 |
|----------|------|
| `GET /api/search?query=...&repo=oss\|cd\|ms\|all` | 语义搜索，返回 `{"results": [...]}`；`repo` 默认 `all`，结果行带 `repo` 字段，开源行附 `ent_syncs`（企业侧同步落点） |
| `GET /api/filter?keyword=...&repo=oss\|cd\|ms\|all` | 关键词过滤，返回 `{"results": [...]}`，字段同上 |
| `GET /api/analyze?query=...&repo=oss\|cd\|ms\|all` | 先召回 Top 5 相关 PR，再调用 Ollama 输出修复分析，返回 `{"analysis": "...", "results": [...]}` |
| `GET /api/agent/search?query=...&repo=oss\|cd\|ms\|all` | 面向 agent 的语义搜索接口，返回格式同 `/api/search` |
| `GET /api/agent/filter?keyword=...&repo=oss\|cd\|ms\|all` | 面向 agent 的关键词接口，返回格式同 `/api/filter` |
| `GET /api/agent/pr/<number>?repo=oss\|cd\|ms` | 查询单个 PR 详情，`repo` 默认 `oss`（传 `all` 也按 `oss` 处理）；返回 `body`、`versions`、`github_url` 等字段。开源 PR（`repo=oss`）额外返回 `ent_syncs`（该 PR 及其 backport PR 已同步到企业版的落点列表，每项含 `ent_pr`/`version`/`via_oss_pr`）；企业 PR（`repo=cd|ms`，企业仓库）额外返回 `synced_from`（同步自哪个开源 PR，含 `oss_pr`/`version`；注意：`sync` 类 PR 不进 `pr_data`，因此用本接口查企业 PR 时 `synced_from` 通常为空，开源→企业方向的落点请用开源 PR 的 `ent_syncs`）。输入 backport PR 号会在对应 repo 内自动反查主 PR（响应附 `resolved_from_backport_pr`） |
| `GET /api/ai/start?prompt=...` | SSE 流，启动新 codex 会话调用 `pr-fix-finder` 分析问题 |
| `GET /api/ai/chat?session=<id>&prompt=...` | SSE 流，使用 session id 续会话进行追问 |

`/api/agent/search` 和 `/api/agent/filter` 额外兼容 `q` / `query` / `keyword` 等参数别名，便于不同 agent 工作流接入。

### 12. AI 分析抽屉

页面右上角 “✨ AI 分析” 按钮打开右侧抽屉，专门做基于 LLM 的修复匹配：

- 触发后自动把搜索框 query 预填到 AI 输入框，可改后发送（也可直接在抽屉里描述新问题）
- 后端调用 `codex exec --json` 跑 `pr-fix-finder` skill，通过 SSE 把事件实时推到前端：`session` / `message` / `tool` / `tool_output` / `error` / `done`
- 中间的 "thinking" 消息会被自动折叠成浅灰小条，只突出最后一条结构化结论
- 助手消息走 marked.js 做 markdown 渲染（代码块、列表、链接、表格等）
- "+ 新对话" 按钮清空会话和消息，下一条走 `/api/ai/start`；否则用 `/api/ai/chat` 续会话
- `Cmd/Ctrl + Enter` 在输入框发送
- 会话仅在服务进程内存里维护，重启即丢；多浏览器标签互不干扰
- 子进程清理：关页面 / 关 EventSource 时，后端会捕获 `BrokenPipeError` 并 `terminate()` 对应的 codex 进程，不会留僵尸

实现：`web.py` 内联前端 + 路由，`chat.py` 封装 codex 子进程和 JSONL 事件解析。`http.server.ThreadingHTTPServer` 保证 SSE 长连接不阻塞其他请求。

### 13. 配套 Skill

目录内新增了 [`skills/pr-fix-finder/SKILL.md`](./skills/pr-fix-finder/SKILL.md)，用于回答“某个 StarRocks 问题是否已被历史 PR 修复”。它依赖上面的 `/api/agent/search`、`/api/agent/filter`、`/api/agent/pr/<number>` 接口做多轮召回和证据补全，默认联合检索开源和企业仓库（`repo=all`），命中企业独有修复时会在结论中标注“仅企业版包含”。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SR_HOST` | `127.0.0.1` | StarRocks FE 地址 |
| `SR_PORT` | `9030` | StarRocks 查询端口 |
| `SR_HTTP_PORT` | `8030` | StarRocks HTTP 端口 |
| `SR_USER` | `root` | StarRocks 用户 |
| `SR_PASSWORD` | (空) | StarRocks 密码 |
| `OLLAMA_HOST` | `localhost` | Ollama 地址 |
| `OLLAMA_PORT` | `11434` | Ollama 端口 |
| `EMBED_MODEL` | `bge-m3` | embedding 模型 |
| `SUMMARY_MODEL` | `qwen3.5:9b` | Ollama 摘要 provider 和 `/api/analyze` 使用的模型 |
| `EMBEDDING_DIM` | `1024` | 向量维度 |
| `PR_SUMMARY_PROVIDER` | `codex` | 摘要生成方式: `codex` / `gemini` / `ollama` |
| `PR_SUMMARY_BATCH_SIZE` | `5` | Codex/Gemini 每批分析的 PR 数 |
| `PR_SUMMARY_TIMEOUT` | `900` | Codex/Gemini 批量分析超时时间（秒） |
| `PR_SUMMARY_BATCH_SLEEP` | `30` | 每批 PR 摘要分析之间的等待时间（秒） |
| `PR_SUMMARY_RETRIES` | `2` | Codex/Gemini 批量分析失败后的重试次数 |
| `PR_SUMMARY_RETRY_SLEEP` | `30` | Codex/Gemini 批量分析重试前等待时间（秒） |
| `PR_SUMMARY_CLEAN_TMP` | `1` | 成功解析摘要后删除本批新生成的 `tmp_req_*` 临时目录 |
| `CODEX_BIN` | `codex` | Codex CLI 可执行文件路径 |
| `GEMINI_BIN` | `gemini` | Gemini CLI 可执行文件路径 |
| `CODEX_MODEL` | (空) | Codex CLI 模型名；为空则使用 Codex 默认模型 |
| `GEMINI_MODEL` | (空) | Gemini CLI 模型名；为空则使用 Gemini 默认模型 |

多仓库支持没有新增环境变量：各仓库（`oss`/`cd`/`ms`）的 slug、`kind`、`active`（是否轮询）与数据目录（见下方「数据目录结构」）当前在 `pr.py` 的 `REPOS` 注册表里硬编码，不通过环境变量配置；新增/替换源仓库改这里即可。

## 表结构

### pr_data（主表）

| 字段 | 说明 |
|------|------|
| `pr_number` | PR 编号（联合主键 `(pr_number, repo)` 之一） |
| `repo` | 仓库短码：`oss`（StarRocks/starrocks）/ `cd`（CelerData/celerdata-enterprise，冻结）/ `ms`（MirrorShipDB/mirrorship-enterprise，当前企业源），默认 `oss`（联合主键 `(pr_number, repo)` 之一） |
| `title` | 标题 |
| `author` | 作者 |
| `labels` | 标签 |
| `created_at` | 创建时间 |
| `merged_at` | 合并时间 |
| `additions` | 增加行数 |
| `deletions` | 删除行数 |
| `changed_files` | 变更文件数 |
| `module` | 模块: FE / BE / Docs / Test / Tool |
| `change_type` | 变更类型: BugFix / Feature / Enhancement / Refactor / UT / Doc / Tool / SyncFix（企业仓库 `conflict_fix` 分类强制打此标签） |
| `version` | 版本 (从 labels 解析, 默认 main；企业仓库无 label 时从 base_ref 派生分支粒度版本) |
| `ai_summary` | AI 中文摘要 (展示用) |
| `ai_summary_en` | AI 英文摘要 |
| `diff_keywords` | 结构化检索关键词，用于展示和诊断 |
| `searchable_text` | 用于 embedding 和全文检索的合并文本 |
| `body` | PR 原始描述 |
| `embedding` | 向量表示 (bge-m3, 1024维) |

两仓 PR 号数值区间有重叠，`(pr_number, repo)` 联合主键避免互相覆盖。企业仓库中分类为 `sync` / `backport` 的 PR 不写入本表（前者只进 `pr_sync`，后者只进 `pr_versions`）。

### pr_versions（版本映射表）

| 字段 | 说明 |
|------|------|
| `pr_number` | 主 PR 编号（联合主键 `(pr_number, repo, version)` 之一） |
| `repo` | 仓库短码：`oss` / `cd` / `ms`，默认 `oss`（联合主键之一） |
| `version` | 版本（联合主键之一） |
| `backport_pr` | backport PR 编号，主版本为 NULL |

### pr_sync（开源↔企业同步映射表）

企业仓库中标题含 `(sync #N)`（分类为 `sync`）的 PR，由 `link-sync --repo cd|ms` 写入本表；不进 `pr_data`。

| 字段 | 说明 |
|------|------|
| `oss_pr` | 开源 PR 编号（联合主键 `(oss_pr, ent_pr, ent_repo)` 之一），即 `(sync #N)` 中的 N |
| `ent_pr` | 企业 PR 编号（联合主键之一） |
| `ent_repo` | 企业仓库短码 `cd` / `ms`（联合主键之一）——两个企业仓库 PR 号区间重叠，靠它区分同步落到哪个仓库 |
| `version` | 企业侧落点版本：release 粒度 `x.y.z-ee`（有 `version:` 标签时）或分支粒度 `main` / `X.Y`（从 base_ref 派生） |
| `ent_merged_at` | 企业 PR 合并时间 |

同一个开源 PR 可能对应多个企业 sync PR（分别同步到不同企业分支 / 不同企业仓库），`(oss_pr, ent_pr, ent_repo)` 联合主键天然容纳这种一对多关系。

## 数据目录结构

```
data/
├── raw/                              # 开源仓库 (oss) 原始数据，路径不变、向后兼容
│   ├── pr_raw_20250401.json         # 4月1日合并的 PR 原始数据
│   ├── pr_raw_20250402.json
│   └── ...
├── enriched/                         # 开源仓库 (oss) 增强后数据
│   ├── pr_enriched_20250401.json    # 4月1日 PR + 摘要 + diff_keywords + searchable_text + embedding
│   ├── pr_enriched_20250402.json
│   └── ...
└── cd/                                # CelerData 企业仓库 (cd) 数据，目录结构与 oss 镜像（ms/ 同理）
    ├── raw/
    │   ├── pr_raw_20250401.json     # 额外带 pr_kind / sync_source_pr / base_ref 字段
    │   └── ...
    └── enriched/
        ├── pr_enriched_20250401.json   # 只包含 conflict_fix / exclusive 分类的 PR（sync/backport 在 fetch 阶段已跳过 enrich）
        └── ...
```
