---
name: pr-json-summarizer
description: 用于将一个或多个 GitHub PR 分析为结构化 JSON，输出 english_summary、chinese_summary、diff_keywords 和 searchable_text，供检索、入库或索引使用。
---

# PR JSON Summarizer

这个 skill 用于把一个或多个 GitHub PR 分析成结构化 JSON，供检索、入库、召回或后续排序使用。

## 何时使用

- 用户给出一个或多个 PR 链接或 PR 编号，希望输出结构化 JSON，而不是普通说明文字。
- 输出中必须包含 `english_summary`、`chinese_summary`、`diff_keywords`、`searchable_text`。
- 结果将用于知识库、StarRocks 表、检索系统或历史修复召回。

不要把这个 skill 用于：

- 代码审查
- CI 排障
- 一般性的 PR 讲解

## 最终输出

最终始终返回 **一个 JSON array**。

- 即使只输入一个 PR，也返回长度为 1 的 array
- 如果输入多个 PR，按输入顺序输出多个对象
- 用户可以用逗号或空格分隔多个 PR，例如：
  - `48091,72038`
  - `48091 72038`
  - `https://github.com/StarRocks/starrocks/pull/48091, https://github.com/StarRocks/starrocks/pull/72038`

单个元素的默认结构如下：

```json
[
  {
    "pr_number": 48091,
    "title": "[BugFix] reserve SLICE_MEMEQUAL_OVERFLOW_PADDING for key in hash table",
    "english_summary": "...",
    "chinese_summary": "...",
    "diff_keywords": "symptom: ...\ncause: ...\nfix: ...\nsymbols: ...\nfiles: ...\nkeywords: ...",
    "searchable_text": "..."
  }
]
```

除非用户明确要求，否则最终答案不要再包 Markdown 代码块。

## 数据收集

至少收集以下内容：

- PR 标题
- PR 描述
- 修改文件列表
- PR patch / git diff
- 合并状态（如果可获取）

优先使用：

1. `gh pr view <number> --repo <owner/repo> --json title,body,files,state,author,commits,additions,deletions,changedFiles`
2. `gh pr diff <number> --repo <owner/repo> --patch`

如果用户给的是 GitHub URL，先解析出：

- `owner`
- `repo`
- `pr_number`

如果用户给的是多个值：

- 优先按逗号分隔
- 如果没有逗号，再按空白分隔
- 去掉首尾空格
- 逐个解析每个 PR

## 排除测试相关 diff

测试相关代码 **不能参与**：

- `english_summary`
- `chinese_summary`
- `diff_keywords`

默认排除范围：

- `test/` 目录
- `*_test.*`
- `*_bench.*`
- mock / fixture / testdata-only 文件
- 只用于 benchmark 或验证的新增文件

如果一个 PR 同时修改了生产代码和测试代码：

- 摘要只基于生产代码 diff 生成
- 关键词只从生产代码 diff 提取

## English Summary 规则

`english_summary` 用 3 到 5 句，或 1 到 2 个短段落说明：

- 这个 PR 修复了什么问题，或改进了什么行为
- 根因是什么（前提是 PR body 或生产代码 diff 能支持）
- 核心修复动作是什么
- 改动影响了哪些高层模块
- 是否改变对外行为

要求：

- 优先依据非测试 diff，再用 PR body 补充
- 高价值代码符号要保留
- 不要提测试文件或 benchmark 文件
- 不要提 unit test、integration test、benchmark、test coverage、test case，即使 PR body 提到了这些内容
- 如果 PR body 同时描述了生产改动和测试改动，summary 只保留生产改动部分
- 不要超出证据进行猜测

## 中文摘要规则

`chinese_summary` 是对 `english_summary` 的忠实中文翻译。

要求：

- 技术符号保持原样
- 确定性强弱保持一致
- 不要把测试相关信息从英文摘要翻译进中文摘要
- 不额外添加结论

## diff_keywords 规则

`diff_keywords` 是一个**单列、多行字符串**，用于倒排检索。

必须使用下面的固定顺序：

```text
symptom: ...
cause: ...
fix: ...
symbols: ...
files: ...
keywords: ...
```

格式要求：

- 每个标签只出现一次
- `symbols`、`files`、`keywords` 使用逗号分隔
- `files` 使用裁剪后的仓库相对路径
  - 优先 `exprs/agg/distinct.h`
  - 不优先 `be/src/exprs/agg/distinct.h`
- `symbols` 只保留高价值符号，不要把所有 touched symbol 都塞进去
- 不包含测试文件和 benchmark 文件

每一行的生成规则：

- `symptom`：从 PR body 或生产代码 diff 提炼用户可感知问题类型，例如 `crash risk`、`wrong result`、`memory-safety issue`、`OOM risk`
- `cause`：基于证据提炼最具体的根因短语
- `fix`：核心修复动作，多个动作用 `; ` 分隔
- `symbols`：3 到 6 个高价值 symbol
- `files`：3 到 8 个关键生产文件
- `keywords`：补充短语，避免和上面几类完全重复

### 高价值 symbol 选择规则

优先保留满足以下任一条件的符号：

- 直接体现根因
- 直接体现修复动作
- 高概率出现在 stack trace、日志、报错、用户问题里
- 对区分相似 PR 很有帮助

不要把所有 touched class、function、type 都输出。

## searchable_text 规则

`searchable_text` 用于语义检索，应该偏自然语言，不要把 `diff_keywords` 整段原样复制进去。

必须由以下部分拼接：

1. PR title
2. 完整的 English summary
3. 完整的 Chinese summary
4. `symptom:` 一句话
5. `cause:` 一句话
6. `fix:` 一句话
7. `symbols:` 列表值
8. `modules:` 列表值
9. `keywords:` 列表值

以上每一部分都必须单独占一行，使用换行符分隔，不要把多部分压成一个长段落。

其中：

- `symptom:`、`cause:`、`fix:` 可以是短句
- `symbols:`、`modules:`、`keywords:` 直接使用列表值，不要求改写成自然语言句子
- `symbols:`、`keywords:` 可以直接复用 `diff_keywords` 中对应项的值
- `modules:` 应提炼为模块或功能域短语列表

要求：

- 保持紧凑
- 同时包含英文和中文
- 必须原样包含完整的 `english_summary`
- 必须原样包含完整的 `chinese_summary`
- 只保留少量高价值 symbol
- 不写 benchmark / test 相关内容
- `files` 只保留在 `diff_keywords` 中，不进入 `searchable_text`
- 不要堆很多文件路径

## 工作流

1. 解析用户输入中的一个或多个 PR 链接或编号。
2. 如果是多个 PR，按输入顺序逐个处理，不要打乱顺序。
3. 对每个 PR 拉取元数据和 patch。
4. 将改动拆分为：
   - 生产代码 diff
   - 测试相关 diff
5. 只从生产代码 diff 提取：
   - symptom
   - cause
   - fix actions
   - high-value symbols
   - important files
   - retrieval keywords
6. 基于生产代码 diff + PR body 生成英文摘要。
7. 翻译成中文摘要。
8. 生成 `diff_keywords`。
9. 生成 `searchable_text`。
10. 将所有 PR 结果组装成一个 JSON array 返回。

## 质量检查

返回前检查：

- 测试相关 diff 没有参与摘要和关键词
- 字段名是 `diff_keywords`，不是 `non_test_diff_keywords`
- `diff_keywords` 中每个标签只出现一次
- `symbols` 足够精简，不是枚举所有 touched symbol
- `files` 使用裁剪后的有效路径
- `searchable_text` 必须原样包含完整的 `english_summary`
- `searchable_text` 必须原样包含完整的 `chinese_summary`
- `searchable_text` 必须按行分隔
- `searchable_text` 必须包含 `symptom:`、`cause:`、`fix:`、`symbols:`、`modules:`、`keywords:`
- JSON 可解析
- 最终顶层结构必须是 array，不是单个 object

## 信息不足时的处理

如果 PR 信息不完整，也要返回 JSON，但表述必须保守。

允许使用的兜底写法：

- `symptom: not explicit in PR body`
- `cause: not explicit in PR body; inferred from non-test diff`

如果根因或症状是从 diff 推断出来的，在摘要中要体现不确定性，例如：

- `The root cause appears to be ...`
- `根因看起来是 ...`
