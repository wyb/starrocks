# PR Analytics - StarRocks PR 智能分析工具

通过 Ollama 本地模型对 StarRocks GitHub PR 生成中英文双语摘要和向量 embedding，存入 StarRocks（Primary Key + HNSW 向量索引），支持语义搜索和 Web UI。

## 架构

```
gh CLI 拉取 PR  →  Ollama 生成英文摘要  →  Ollama 翻译为中文  →  bge-m3 生成 embedding
                                                                        ↓
Web UI ← 语义搜索 / 关键词过滤 ← StarRocks (Primary Key + HNSW 向量索引)
```

- **摘要**：先生成英文摘要（与源码语言一致，质量高），再翻译为中文（展示友好）
- **embedding**：基于 `title + 英文摘要` 生成，bge-m3 多语言模型支持中英文混合查询
- **PR body 清洗**：仅提取 `What I'm doing` section 之前的内容，过滤 template 噪声和 `Fixes #issue` 行
- **Backport 处理**：自动识别 backport PR（title 含 `(backport #xxx)`），enrich 时跳过摘要生成，通过 `pr_versions` 表将版本信息关联到主 PR

## 前置准备

```bash
# Ollama
brew install ollama
ollama serve
ollama pull bge-m3         # embedding 模型 (1024维, 多语言)
ollama pull qwen3.5:9b     # 摘要模型

# StarRocks 3.4+ (需支持向量索引)
pip install pymysql
```

## 使用流程

### 常规工作流 (一键执行)

包含以下 Step 1, 2, 4, 5 的所有步骤，适用于日常增量更新。

```bash
# 更新过去 1 天的数据
python3 pr.py pipeline --days 1

# 更新指定时间范围的数据
python3 pr.py pipeline --since 2025-04-01 --until 2025-04-30
```

### 1. 拉取 PR 原始数据

按天存储，按周分批拉取（避免 GitHub API 限制），增量去重。

```bash
python3 pr.py fetch --days 1
python3 pr.py fetch --days 30
python3 pr.py fetch --since 2025-04-01
python3 pr.py fetch --since 2025-04-01 --until 2025-04-30
```

输出: `data/raw/pr_raw_20250401.json`, `data/raw/pr_raw_20250402.json`, ...

### 2. AI 增强：生成摘要 + embedding

断点续跑，自动跳过已处理的 PR。Backport PR 自动跳过（不生成摘要）。

```bash
python3 pr.py enrich --file data/raw/pr_raw_20250401.json
python3 pr.py enrich --since 2025-04-01
python3 pr.py enrich --since 2025-04-01 --until 2025-04-30
python3 pr.py enrich --since 2025-04-01 --until 2025-04-30 --reverse  # 从新到旧处理
```

输出: `data/enriched/pr_enriched_20250401.json`, `data/enriched/pr_enriched_20250402.json`, ...

### 3. 建表（首次）

创建 `pr_data`（Primary Key + HNSW 向量索引）和 `pr_versions`（版本映射表）。如果表已存在会报错，使用 `--force` 强制重建。

```bash
python3 pr.py init-table            # 表已存在则报错
python3 pr.py init-table --force    # 强制删除重建
```

### 4. 导入 StarRocks

重复导入自动更新（Primary Key 去重）。导入时自动写入主版本到 `pr_versions`。

```bash
python3 pr.py load --file data/enriched/pr_enriched_20250401.json
python3 pr.py load --since 2025-04-01
python3 pr.py load --since 2025-04-01 --until 2025-04-30
```

### 5. 关联 Backport 版本

扫描 raw 文件，提取 backport PR 的版本信息，写入 `pr_versions` 表关联到主 PR。

```bash
python3 pr.py link-backport --since 2025-04-01
python3 pr.py link-backport --since 2025-04-01 --until 2025-04-30
```

### 6. 语义搜索（命令行）

```bash
python3 pr.py search "内存泄漏"
python3 pr.py search "materialized view refresh" --top 5
```

支持中英文混合查询。

### 7. Web UI

```bash
python3 web.py                # 默认 8888 端口
python3 web.py --port 9090    # 自定义端口
```

打开 `http://localhost:8888`，支持：
- **语义搜索**（自然语言，调 Ollama 生成 embedding）
- **关键词过滤**（匹配 title + 中英文摘要）。支持四种模式切换：
  - `自动`（默认）：优先使用 `LIKE` 匹配全字段，无结果则自动降级尝试 `MATCH_ALL`，最后尝试 `MATCH_ANY`。
  - `LIKE`：标准 SQL 模糊匹配。
  - `MATCH ALL` / `MATCH ANY`：使用 StarRocks 倒排索引分词匹配。倒排索引查询会按 `title` > `ai_summary` > `ai_summary_en` 的优先级逐个字段查询，匹配到即返回。
- **筛选条件**：PR# / Module / Type / Version / Author / 时间范围
- 每个 PR 展示所有关联版本（含 backport），版本号可点击跳转对应 PR

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
| `SUMMARY_MODEL` | `qwen3.5:9b` | 摘要模型 |
| `EMBEDDING_DIM` | `1024` | 向量维度 |

## 表结构

### pr_data（主表）

| 字段 | 说明 |
|------|------|
| `pr_number` | PR 编号 (Primary Key) |
| `title` | 标题 |
| `author` | 作者 |
| `labels` | 标签 |
| `created_at` | 创建时间 |
| `merged_at` | 合并时间 |
| `additions` | 增加行数 |
| `deletions` | 删除行数 |
| `changed_files` | 变更文件数 |
| `module` | 模块: FE / BE / Docs / Test / Tool |
| `change_type` | 变更类型: BugFix / Feature / Enhancement / Refactor / UT / Doc / Tool |
| `version` | 版本 (从 labels 解析, 默认 main) |
| `ai_summary` | AI 中文摘要 (展示用) |
| `ai_summary_en` | AI 英文摘要 (embedding 用) |
| `body` | PR 原始描述 |
| `embedding` | 向量表示 (bge-m3, 1024维) |

### pr_versions（版本映射表）

| 字段 | 说明 |
|------|------|
| `pr_number` | 主 PR 编号 (Primary Key) |
| `version` | 版本 (Primary Key) |
| `backport_pr` | backport PR 编号，主版本为 NULL |

## 数据目录结构

```
data/
├── raw/
│   ├── pr_raw_20250401.json        # 4月1日合并的 PR 原始数据
│   ├── pr_raw_20250402.json
│   └── ...
└── enriched/
    ├── pr_enriched_20250401.json   # 4月1日 PR + 双语摘要 + embedding
    ├── pr_enriched_20250402.json
    └── ...
```

## TODO

1. 是否需要分析 PR 内容？有些 PR 没写 description（只能通过 title 生成摘要）或者 description 写的不全。
