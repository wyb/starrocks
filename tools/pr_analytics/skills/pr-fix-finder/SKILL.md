---
name: pr-fix-finder
description: 判断 StarRocks 用户问题是否已被历史 PR 修复。基于结构化 diff_keywords 做多轮检索和证据对齐，输出可解释的修复结论。
---

# PR Fix Finder

这个 skill 用于回答"某个问题是否已经被历史 PR 修复过"。它负责编排检索流程、约束证据对齐和输出格式；不在本地生成 embedding，问题与候选 PR 的相关性分析由外部大模型完成。

## 核心前提

- **知识库状态**：库中所有的 PR 都是**已合入 (Merged)** 的。不存在"正在修复"或"待合并"的状态。
- **目标**：找到修复该问题的具体 PR，或确认在当前知识库中未发现相关修复。
- **核心数据**：每个 PR 都有结构化的 `diff_keywords`（symptom/cause/fix/symbols/files/keywords）和 `searchable_text`。`searchable_text` 是倒排检索文本，也是生成 `embedding` 的语义来源；向量索引建立在 `embedding` 字段上。它们是检索和评估的一等证据源。

## 何时使用

- 用户描述一个 StarRocks 问题，想知道是否已有修复 PR。
- 用户给出报错、堆栈、类名、函数名、SQL、模块名，需要回查历史修复。
- 需要给出"已修复 / 可能相关但证据不足 / 未发现明确修复"的结论，并附上证据 PR 和版本信息。

## 依赖接口

默认假设 PR 检索服务已经启动，基础地址为 `http://172.26.80.30:8888`。如果用户或环境明确提供了其他地址，优先使用那个地址。

### 接口定义

#### `GET /api/agent/search`

语义检索（向量相似度）。

- 参数：`query`（推荐），支持别名 `q` / `keyword`
- 筛选参数：`module`, `change_type`, `version`, `author`, `since`, `until`, `top`
- 返回 `{"results": [...]}`，每个结果包含：
  - `pr_number`, `title`, `author`, `module`, `change_type`
  - `ai_summary`, `ai_summary_en`, `diff_keywords`
  - `merged_at`, `additions`, `deletions`, `changed_files`
  - `score`（相似度分数）
  - `versions`：对象数组，格式为 `[{"version": "main", "backport_pr": null}, {"version": "3.3.2", "backport_pr": 71234}]`

#### `GET /api/agent/filter`

关键词检索，查询 `searchable_text` 的倒排索引。

- 参数：`keyword`（推荐），支持别名 `q` / `query`
- `match_mode`：匹配模式，可选 `auto`（默认）/ `like` / `all` / `any`
- 筛选参数：`module`, `change_type`, `version`, `author`, `since`, `until`, `top`
- 返回 `{"results": [...]}`，字段同 search（无 score）

#### `GET /api/agent/pr/<number>`

PR 详情。

- 返回 `{"result": {...}}`，包含：
  - `pr_number`, `title`, `author`, `module`, `change_type`
  - `ai_summary`, `ai_summary_en`, `diff_keywords`, `searchable_text`
  - `body`（PR 原始描述）
  - `versions`：对象数组，格式同上
  - `github_url`

### 代理使用

访问 PR 检索服务时必须绕过代理。无论使用 `curl`、`WebFetch` 还是其他 HTTP 工具，都要确保请求不经过 `http_proxy` / `https_proxy`。使用 `curl` 时固定带上 `--noproxy '*'`。

### 本地访问限制

如果目标地址是 `127.0.0.1` 或 `localhost`，默认要意识到这可能是 agent 沙箱自己的回环地址，而不是宿主机的本地服务地址。

执行规则直接简化为：

- 在沙箱里请求本地 PR 检索服务时，只要请求失败，不必继续区分失败原因。
- 失败后立即改用宿主机 / 沙箱外执行同一个请求。
- 如果当前 agent 不能直接切到宿主机执行，就请用户在宿主机执行该请求并提供结果，或者提供一个当前沙箱可访问的服务地址。
- 在宿主机视角下同一个请求仍然失败时，才可视为服务异常；在此之前，不要下"接口不可用"或"没有相关 PR"的结论。

## 工作流

最多做 3 轮，每轮在 agent 内部推理过程中记录：

- 本轮使用的关键词
- 命中的候选 PR
- 为什么保留或排除这些候选
- 下一轮关键词如何重写

最终输出时，将每轮记录压缩为一句话摘要（见"输出要求"）。

### 职责边界

- agent 负责流程编排：决定是否进入下一轮、调用哪个接口、合并结果、何时补 PR 详情、何时停止。
- 外部大模型负责语义处理和相关性判断：从用户问题、候选 PR、PR 详情中提取症状词、术语词、代码符号，分析用户问题与候选 PR 是否真正相关，并生成下一轮关键词。
- skill 负责约束重写方式，不允许模型随意发散，也不允许 agent 跳过证据直接下结论。

### 问题拆解

在检索之前，agent 必须先把用户问题显式拆解为以下维度：

- **symptom**：用户可感知的现象（crash, wrong result, OOM, timeout, NPE, hang, data loss 等）
- **cause**：用户推测或描述的根因（如果有）
- **symbols**：类名、函数名、配置项、错误码、SQL 关键字
- **files**：涉及的文件或路径片段
- **module**：FE / BE / MV / Load / Compaction / Query / Tablet / Catalog / Connector 等
- **raw query**：用户完整原始描述（用于语义搜索）

后续每一轮检索都基于这些维度构造 query，而不是直接把用户原话当关键词。如果某个维度用户没有提供，标记为空，不要凭空猜测。

### 检索方式策略

不同场景使用不同检索方式或匹配模式：

| 场景 | 推荐方式 | 说明 |
|------|-----------|------|
| 精确符号：类名、函数名、配置项、错误码 | `all` | 所有 token 必须同时命中 |
| 文件名、路径片段 | 不作为首选 filter query | `searchable_text` 不包含 files 信息；files 主要用于召回后的结构化校验 |
| 症状 + 模块组合 | `all` | 如 `wrong result materialized view`、`crash compaction` |
| cause 短语 | `all` | 如 `null pointer dereference` |
| 术语扩展、相邻表达、宽松召回 | `any` | 任一 token 命中即可 |
| 抽取出的高价值自然语言 query | 走 `/api/agent/search` | 用 symptom/cause/module/symbol 组成更贴近 PR 语言的语义 query |
| 原始长描述、自然语言问题 | 走 `/api/agent/search` | 语义向量检索 |

### 检索策略

- 按**问题拆解维度**构造 query，每个维度生成对应类型的短 query。
- 用户原文中的英文 token（报错、类名、函数名、配置项、SQL）不要翻译掉，直接作为 keyword。
- 调用 `/api/agent/search` 和 `/api/agent/filter` 时显式指定 `top`，且 `top` 至少为 80，保证召回率；agent 再对合并结果做规则化预筛，外部大模型负责最终相关性评估。
- 控制请求数量：每轮总请求不超过 4-6 个。
- 同一轮内 filter 和 search 的调用可以并发执行，但各轮之间必须串行（下一轮依赖上一轮的结果）。

### 候选处理规则

- 合并所有 search/filter 结果后按 `pr_number` 去重。
- agent 可以做规则化预筛，但不做最终深度结论：
  - 统计每个候选的命中来源数和 query 类型。
  - 优先保留命中用户明确 symbol/config/error code 的候选。
  - 优先保留同时被 filter 和 search 命中，或被多个 query 命中的候选。
  - 对明显不相关的模块或症状降权；只有在用户给出的模块/症状非常明确且与候选完全冲突时，才剔除。
- 交给外部大模型评估的候选数量根据证据密度动态控制，默认 30-50 个。
- 如果候选很多，只截掉仅由 raw search 单路命中且缺少 symptom/symbol/cause 对齐的候选。
- 不要因为固定数量上限丢弃强符号、配置项或错误码命中的候选。

### 三轮检索流程

#### 第 1 轮

策略：**精确 token 优先，语义兜底**。并行执行：

1. `/api/agent/filter?keyword=<symbols>&match_mode=all&top=80` — 符号精确命中（类名、函数名、配置项、错误码）。选择 1-3 个最高价值 symbol；如果有多个无关 symbol，分开请求而不是塞进同一个 MATCH_ALL。
2. `/api/agent/filter?keyword=<symptom + module>&match_mode=all&top=80` — 症状定向（如 `crash compaction`、`wrong result materialized view`）。如果 module 为空但 symptom 明确，允许只用 `keyword=<symptom>` 作为低成本召回。
3. `/api/agent/search?query=<high-value natural query>&top=80` — 用抽取出的 symptom/cause/module/symbol 组成高价值自然语言 query 做精准语义召回，如 `wrong result materialized view rewrite`、`HashJoinNode null handling crash`
4. `/api/agent/search?query=<raw query>&top=80` — 保留原始语义上下文的向量兜底

如果某个维度用户未提供，跳过对应请求。
如果用户给了 files 或 path 片段，优先把它们作为候选评估时的校验证据；不要使用 `/api/agent/filter` 期望从 `searchable_text` 中按文件名召回。只有当文件名本身也是高价值符号或错误上下文的一部分时，才可作为低优先级补充 query。

结果按 `pr_number` 去重合并后，agent 先做规则化预筛，再交给外部大模型按结构化评估规则（见"候选评估规则"）判断 `strong` / `possible` / `irrelevant`。

#### 第 2 轮

触发条件：

- 第 1 轮没有 `strong` 命中；或
- 第 1 轮最强候选是 `defensive_fix`（见"防御性修复识别"）。

agent 负责把以下信息提供给模型：

- 用户原始问题的拆解结果
- 第一轮 `possible` 候选的 **`diff_keywords`**（完整内容，不只是标题和摘要）；如果最强候选是 `defensive_fix`，也要纳入
- 第一轮 miss 原因：为什么这些 PR 不够相关

模型重写策略：

- 从已命中候选的 `diff_keywords` 中提取相邻 symptoms/symbols/keywords 扩展搜索
- 把通俗描述改成 StarRocks 术语
- 把宽泛现象改成模块 + 症状
- 保留高价值符号名，不要把具体报错完全丢掉
- 如果第 1 轮命中的是防御性修复，必须从"非法状态的来源"方向扩展，而不是继续只搜崩溃栈。例如：
  - missing tuple id/descriptor -> query context、descriptor generation、plan fragment serialization
  - null DescriptorTbl/query context -> query context reuse、duplicate query id、stale fragment
  - schema/slot/column mismatch -> FE thrift serialization、slot id、column id、descriptor serialization
- 至少保留一条 raw stack 或 body 片段检索，用于发现 PR body 直接贴了相同栈但 `diff_keywords` 不含栈符号的根因 PR

模型输出要求：

- 输出 3 到 5 个短 query，优先覆盖 cause/symptom/symbol 维度
- 至少保留 1 个接近用户原话的 query
- 至少生成 1 个从候选 `diff_keywords` 中提取的相邻术语 query
- 不要输出空泛词，如 `fix`、`issue`、`problem`

再次调用：

- `/api/agent/filter?keyword=<symbol + symptom 或 module + symptom>&match_mode=all&top=80` — 核心组合仍精确匹配
- `/api/agent/filter?keyword=<从候选 diff_keywords 提取的相邻术语/同义词>&match_mode=any&top=80` — 扩展召回
- `/api/agent/search?query=...&top=80` — 语义搜索

#### 第 3 轮

触发条件：第 2 轮仍不清晰。

agent 负责把以下信息提供给模型：

- 用户给出的 stack trace、SQL、配置项
- 前两轮 `possible` 候选的完整 `diff_keywords`（特别是 symbols/files/keywords）
- `/api/agent/pr/<number>` 补到的详情（如有必要）

模型重写策略：

- 优先 symbol + symptom 组合（如 `HashJoinNode null pointer`）
- 优先 module/function-symbol + symptom 组合；files/path 只用于校验，除非文件名本身也是高价值符号
- 优先 config + behavior 组合（如 `parallel_fragment_exec_instance_num wrong result`）
- 如果没有有效符号可提取，维持第二轮最佳 query，不要伪造代码名

模型输出要求：

- 输出 2 到 4 个高信息量 query
- 丢掉泛词，如 `fix`、`error`、`optimize`

只要发现非 `defensive_fix` 的 `strong` 候选，就停止继续扩展搜索。如果 `strong` 候选是防御性修复，继续根因扩展。

如果第 2 轮召回不足，或候选都不够相关，第 3 轮根据缺口少量调用：

- `/api/agent/filter?keyword=<symbol + symptom 或 config + behavior>&match_mode=all&top=80`
- `/api/agent/search?query=<模型重写后的高价值自然语言 query>&top=80`

## 候选评估规则

**`diff_keywords` 是候选评估的一等证据源**，优先于 title/summary/body。

对每个候选，解析其 `diff_keywords` 的 symptom/cause/fix/symbols/files/keywords 行，按以下维度与用户问题对齐：

### 证据分类

- **A 类（问题对齐）**：
  - symptom 与用户现象一致
  - cause 与用户推测根因一致
  - fix 逻辑上能解决用户问题
- **B 类（上下文对齐）**：
  - symbols 命中用户给出的类名/函数名/配置项
  - files 命中用户给出的文件或模块路径
  - 相同模块 + 相同功能路径
- **C 类（佐证）**：
  - title/body 明确是 BugFix
  - PR body 的 Why/What 与用户现象一致

### 判定规则

- `strong` = 满足以下任一条件：
  - **A 类 ≥ 1 项 + B 类 ≥ 1 项**（问题对齐 + 上下文对齐）
  - **A 类 ≥ 2 项且必须包含 fix**（如 symptom + fix、cause + fix），即使缺少上下文对齐也足够。注意：symptom + cause 但无 fix 对齐只能判 possible，除非 title/body/summary 能佐证修复动作覆盖该问题。
- `possible` = 只命中 A 类 1 项或 B 类若干项，证据不够构成完整链条
- `irrelevant` = 关键词重合但问题本质不同；仅同模块但症状不同

### 防御性修复识别

如果候选 PR 的 fix 主要是 fail fast、增加校验、避免 crash、返回错误、补日志、catch exception、cleanup failure path、guard null pointer 等，而 cause 只描述"非法状态会导致 crash"，应标记为 `defensive_fix`。

`defensive_fix` 可以作为 `strong` 或 `possible` 候选，但不能自动代表根因修复，除非 PR 同时解释了非法状态从哪里产生，并且该 cause 与用户问题上下文对齐。

如果 PR 的 fix 同时包含防御逻辑和根因修复（如既加了 null check 又修了上游序列化逻辑），不标记为 `defensive_fix`，按正常 `strong` 处理。

典型防御性修复信号：

- fix: validate/check before create/open/read
- fix: avoid nullptr/SIGABRT/use-after-free by guard
- fix: convert crash to Status/Error
- fix: add diagnostic logging
- cause: invalid/missing descriptor/tuple id/query context/schema，但未说明上游为什么生成这种非法状态

### 内部权重参考

优先看三条证据链是否成立：

- **问题链**：symptom 命中 > cause 命中 > title/summary 泛相似
- **修复链**：fix 覆盖用户问题 > PR body 明确说明修复动作
- **上下文链**：symbols 命中 > files 命中 > 相同模块 + 相同功能路径 > 仅同模块

判定 `strong` 时不要只按单项权重排序；必须同时证明"问题相同或高度相似"和"该 PR 的修复动作能覆盖该问题"。`symbols` / `files` 主要用于定位上下文，不能单独证明修复关系。

仅模块相同但症状不同的候选应降权，避免"同模块优化 PR"干扰真正 bugfix PR。

## 详情补全

- **如果候选已包含 `diff_keywords` 和 `ai_summary_en`，直接评估，不需要补全。**
- 只在以下情况调用 `/api/agent/pr/<number>`：
  - 列表结果缺少 `versions` 字段
  - 证据差一点需要看 body 细节
- 补全后优先分析 `diff_keywords` + `body` + `versions`，`searchable_text` 只作辅助，不要直接长篇引用。
- `/api/agent/pr/<number>` 最多补全 6 个。
- `gh pr view` / `gh pr diff` 作为最后手段，只对高度怀疑的候选使用，最多 3 个。
- 不要把 `gh` / `git` 作为第一步。

## 停止条件

- 发现 `strong` 且不是 `defensive_fix` 的根因候选后停止（包括第一轮就命中的情况）。
- 如果最强候选是 `defensive_fix`，必须继续至少 1 轮根因扩展搜索，除非用户明确只问"是否有防御性修复"。
- 如果 `defensive_fix` 在第 3 轮才发现，直接在第 3 轮内追加根因扩展请求（从"非法状态的来源"方向构造 query），不需要额外轮次。
- 根因扩展后：
  - 如果找到 cause 更具体、且 body/diff 直接包含用户栈或触发场景的 PR，优先判为根因修复。
  - 防御性 PR 作为补充证据输出。
  - 如果没有更具体根因 PR，再输出防御性修复，并明确说明"未找到更具体根因修复"。
- 3 轮后仍然没有 `strong`，输出"未发现明确修复"
- 如果只有 `possible`，明确说明证据不足，不要强行下结论

## 输出要求

最终回答必须包含：

- **`✅/❌/❓ 结论`**：`✅ 已修复` / `❌ 未发现明确修复` / `❓可能相关但证据不足`
- **`🔗 证据 PR`**：编号、标题、链接、理由
- **`📌 命中原因`**：展示结构化证据对齐，例如：
  > symptom 匹配 "wrong result"，symbols 匹配 `HashJoinNode`，fix 描述了修复 NULL 处理逻辑
- **`🌿 版本信息`**：从 PR 详情的 `versions` 数组推断修复进入了哪些分支。`versions` 格式为对象数组：`[{"version": "main", "backport_pr": null}, {"version": "3.3.2", "backport_pr": 71234}]`，其中 `backport_pr` 为 null 表示主合入，非 null 表示通过该 backport PR 合入对应版本。
- **`🔎 检索过程摘要`**：最多 3 轮，每轮一句话

如果同时存在根因修复和防御性修复，必须分开输出：

- **根因修复 PR**：解释非法状态如何产生，以及 fix 如何阻止该状态产生。
- **防御性修复 PR**：解释它如何避免同类非法状态继续 crash，但不一定消除上游根因。

## 约束

- 不要虚构未命中的 PR，不要声称某个修复"应该存在"。
- 不要跳过问题拆解直接搜索。
- 不要在没有结构化证据对齐的情况下判定 `strong`。
