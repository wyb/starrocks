#!/usr/bin/env python3
"""
StarRocks PR Analytics - 拉取 PR、AI 摘要、向量 embedding、语义搜索

Prerequisites:
    brew install ollama
    ollama serve
    ollama pull bge-m3         # embedding 模型 (1024维, 多语言)
    ollama pull qwen3.5:9b     # 摘要模型
    pip install pymysql        # StarRocks 连接

Usage:
    # 常用：一键跑通全流程 (fetch + enrich + load + link-backport)
    python3 pr.py pipeline --days 1
    python3 pr.py pipeline --since 2025-04-01 --until 2025-04-30

    # Step 1: 拉取 PR 原始数据 (按天存储, 按周分批, 增量去重)
    python3 pr.py fetch --days 1
    python3 pr.py fetch --since 2025-04-01 --until 2025-04-30

    # Step 2: AI 增强 (生成摘要 + embedding, 断点续跑, 自动跳过 backport PR)
    python3 pr.py enrich --file data/raw/pr_raw_20250401.json
    python3 pr.py enrich --since 2025-04-01 --until 2025-04-30
    python3 pr.py enrich --since 2025-04-01 --until 2025-04-30 --reverse

    # Step 3: 建表 (pr_data + pr_versions, 支持 --force 强制重建)
    python3 pr.py init-table
    python3 pr.py init-table --force

    # Step 4: 导入 StarRocks (重复导入自动更新, 同时写入 pr_versions 主版本)
    python3 pr.py load --file data/enriched/pr_enriched_20250401.json
    python3 pr.py load --since 2025-04-01 --until 2025-04-30

    # Step 5: 关联 backport 版本 (扫描 raw 文件, 写入 pr_versions)
    python3 pr.py link-backport --since 2025-04-01 --until 2025-04-30

    # Step 6: 语义搜索
    python3 pr.py search "内存泄漏"
    python3 pr.py search "物化视图刷新" --top 5
"""

import argparse
import base64
import http.client
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import pymysql
from datetime import datetime, timedelta
from pathlib import Path

# --- Config ---
SR_HOST = os.getenv("SR_HOST", "127.0.0.1")
SR_PORT = os.getenv("SR_PORT", "9030")
SR_HTTP_PORT = os.getenv("SR_HTTP_PORT", "8030")
SR_USER = os.getenv("SR_USER", "root")
SR_PASSWORD = os.getenv("SR_PASSWORD", "")
SR_DB = "pr_analytics"

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "localhost")
OLLAMA_PORT = int(os.getenv("OLLAMA_PORT", "11434"))
EMBED_MODEL = os.getenv("EMBED_MODEL", "bge-m3")
SUMMARY_MODEL = os.getenv("SUMMARY_MODEL", "qwen3.5:9b")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "1024"))  # bge-m3 = 1024
PR_SUMMARY_PROVIDER = os.getenv("PR_SUMMARY_PROVIDER", "codex").lower()
PR_SUMMARY_BATCH_SIZE = int(os.getenv("PR_SUMMARY_BATCH_SIZE", "5"))
PR_SUMMARY_TIMEOUT = int(os.getenv("PR_SUMMARY_TIMEOUT", "900"))
PR_SUMMARY_BATCH_SLEEP = int(os.getenv("PR_SUMMARY_BATCH_SLEEP", "30"))
PR_SUMMARY_RETRIES = int(os.getenv("PR_SUMMARY_RETRIES", "2"))
PR_SUMMARY_RETRY_SLEEP = int(os.getenv("PR_SUMMARY_RETRY_SLEEP", "30"))
PR_SUMMARY_CLEAN_TMP = os.getenv("PR_SUMMARY_CLEAN_TMP", "1").lower() not in ("0", "false", "no")
CODEX_BIN = os.getenv("CODEX_BIN", "codex")
GEMINI_BIN = os.getenv("GEMINI_BIN", "gemini")
CODEX_MODEL = os.getenv("CODEX_MODEL", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "")

REPO = "StarRocks/starrocks"
RAW_DIR = Path(__file__).parent / "data" / "raw"
ENRICHED_DIR = Path(__file__).parent / "data" / "enriched"


# --- GitHub Data Fetching ---

def _fetch_prs_batch(since: str, until: str) -> list[dict]:
    """Fetch a single batch of PRs for a date range."""
    # GitHub search uses UTC; shift -8h to compensate for UTC+8 timezone
    utc_since = (datetime.strptime(since, "%Y-%m-%d") - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S")
    utc_until = (datetime.strptime(until, "%Y-%m-%d") + timedelta(hours=24 - 8)).strftime("%Y-%m-%dT%H:%M:%S")
    date_range = f"merged:{utc_since}..{utc_until}"
    cmd = [
        "gh", "pr", "list",
        "--repo", REPO,
        "--state", "merged",
        "--limit", "1000",
        "--search", date_range,
        "--json", "number,title,body,labels,author,mergedAt,createdAt,"
                  "additions,deletions,changedFiles,files",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Warning: gh failed for {since}..{until}: {result.stderr.strip()}")
        return []
    prs = json.loads(result.stdout)
    return prs


def fetch_prs(since: str, until: str = None) -> list[dict]:
    """Fetch PRs, splitting into weekly batches to avoid GitHub API limits."""
    start = datetime.strptime(since, "%Y-%m-%d")
    end = datetime.strptime(until, "%Y-%m-%d") if until else datetime.now()
    print(f"Fetching PRs merged {since} .. {end.strftime('%Y-%m-%d')} ...")

    all_prs = []
    seen = set()
    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=6), end)
        s = batch_start.strftime("%Y-%m-%d")
        e = batch_end.strftime("%Y-%m-%d")
        print(f"  Batch {s} .. {e} ...", end=" ")
        prs = _fetch_prs_batch(s, e)
        new_count = 0
        for pr in prs:
            if pr["number"] not in seen:
                seen.add(pr["number"])
                all_prs.append(pr)
                new_count += 1
        print(f"{new_count} PRs")
        batch_start = batch_end + timedelta(days=1)

    print(f"  Total: {len(all_prs)} PRs")
    return all_prs


def parse_change_type(title: str, body: str) -> str:
    mapping = {
        "bugfix": "BugFix", "bug fix": "BugFix", "bug": "BugFix", "fix": "BugFix",
        "feature": "Feature", "new feature": "Feature", "feat": "Feature",
        "enhancement": "Enhancement", "improve": "Enhancement", "optimize": "Enhancement",
        "refactor": "Refactor", "refact": "Refactor",
        "ut": "UT", "unit test": "UT", "test": "UT", "tests": "UT",
        "doc": "Doc", "docs": "Doc", "documentation": "Doc",
        "tool": "Tool", "tools": "Tool", "build": "Tool", "ci": "Tool",
    }

    # 1. Try to extract from body checklist: ## What type of PR is this:
    if body:
        # Extract content between "What type of PR is this:" and "Does this PR entail a change in behavior?"
        m = re.search(r"## What type of PR is this:.*?\n(.*?)(?=Does this PR entail a change in behavior\?|##|\Z)", body, re.DOTALL | re.IGNORECASE)
        if m:
            content = m.group(1)
            # Find the first checked item: - [x] Type
            checked = re.search(r"-\s*\[[xX]\]\s*(.*)", content)
            if checked:
                val = checked.group(1).strip().lower()
                # Find matching key in mapping
                for k, v in mapping.items():
                    if k in val:
                        return v
                return val.capitalize()

    # 2. Fallback to title tags [BugFix][Feature] etc.
    # Find all tags like [Feature], [BugFix], [branch-3.1]
    tags = re.findall(r"\[([^\]]+)\]", title)
    for tag in tags:
        tag_lower = tag.strip().lower()
        # Direct match in mapping
        if tag_lower in mapping:
            return mapping[tag_lower]
        # Partial match in mapping
        for k, v in mapping.items():
            if k in tag_lower:
                return v

    return "Other"


def parse_backport(title: str) -> list[int]:
    """Extract source PR numbers from backport title like '(backport #71082)'. Returns list of source PR numbers, empty if not a backport."""
    return [int(m) for m in re.findall(r"\(backport\s+#(\d+)\)", title, re.IGNORECASE)]


def infer_version(labels: str) -> str:
    """Extract version from labels like 'version:4.1.1'."""
    m = re.search(r"version[:\s]*([\d]+\.[\d]+(?:\.[\d]+)?)", labels)
    if m:
        return m.group(1)
    return "main"


def infer_module(pr: dict) -> str:
    files = pr.get("files") or []
    paths = [f.get("path", "") for f in files]
    if not paths:
        title_lower = pr.get("title", "").lower()
        if "fe" in title_lower:
            return "FE"
        if "be" in title_lower:
            return "BE"
        return "Unknown"

    counters = {"FE": 0, "BE": 0, "Docs": 0, "Test": 0, "Tool": 0, "Other": 0}
    for p in paths:
        if p.startswith("fe/"):
            counters["FE"] += 1
        elif p.startswith("be/"):
            counters["BE"] += 1
        elif p.startswith("docs/"):
            counters["Docs"] += 1
        elif p.startswith("test/"):
            counters["Test"] += 1
        elif p.startswith("build/") or p.startswith(".github/") or p.startswith("docker/"):
            counters["Tool"] += 1
        else:
            counters["Other"] += 1

    return max(counters, key=counters.get)


# --- Ollama: Summary + Embedding ---

def _ollama_post(path: str, body: dict, timeout: int = 120) -> dict:
    """Call Ollama API via http.client (bypasses proxy entirely)."""
    conn = http.client.HTTPConnection(OLLAMA_HOST, OLLAMA_PORT, timeout=timeout)
    payload = json.dumps(body)
    try:
        conn.request("POST", path, body=payload,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = resp.read().decode("utf-8")
        if resp.status != 200:
            raise RuntimeError(f"Ollama {resp.status}: {data[:500]}")
        return json.loads(data)
    finally:
        conn.close()


def ollama_embed(text: str) -> list[float]:
    """Generate embedding via Ollama /api/embed endpoint."""
    resp = _ollama_post("/api/embed", {"model": EMBED_MODEL, "input": text[:4000]})
    embeddings = resp.get("embeddings")
    if embeddings and len(embeddings) > 0:
        return embeddings[0]
    raise RuntimeError(f"Ollama embed failed: {resp}")


def ollama_summarize(title: str, body: str) -> dict:
    """Generate English summary, then translate to Chinese. Returns {"zh": ..., "en": ...}."""
    # Strip backport tag like "(backport #71082)" from title
    title = re.sub(r"\s*\(backport\s+#\d+\)", "", title, flags=re.IGNORECASE).strip()
    # Keep everything up to end of "What I'm doing" section, discard the rest
    body_text = body or ""
    m = re.search(r"(#+\s*What I'm doing[:\s]*.*?)(?=\n#+\s|\Z)", body_text, re.DOTALL | re.IGNORECASE)
    if m:
        body_text = body_text[:m.end()]
    # Remove noise lines
    lines = [l for l in body_text.split("\n")
             if not re.match(r"\s*(Fixes|Closes|Resolves)\s+#", l, re.IGNORECASE)
             and not re.match(r"\s*This is an automatic backport of pull request #\d+", l, re.IGNORECASE)]
    body_truncated = "\n".join(lines).strip()[:2000]

    # Single request: generate English summary, then translate to Chinese
    # Use structured output format so English is generated first (quality preserved),
    # then Chinese translation follows naturally
    resp = _ollama_post("/api/chat", {
        "model": SUMMARY_MODEL,
        "messages": [{"role": "user", "content":
            "Summarize this GitHub PR in ONE concise sentence in English. "
            "Focus on WHAT changed and WHY. "
            "Then translate it to Chinese.\n"
            "Output exactly 2 lines, no labels or prefixes:\n"
            "Line 1: English summary\n"
            "Line 2: Chinese translation\n\n"
            f"Title: {title}\n"
            f"Description: {body_truncated}"}],
        "stream": False,
        "options": {"num_predict": 400},
        "think": False,
    }, timeout=300)
    content = resp.get("message", {}).get("content", "").strip()
    lines = [l.strip() for l in content.split("\n") if l.strip()]
    if len(lines) >= 2:
        return {"en": lines[0], "zh": lines[1]}
    # Fallback: detect if single line is Chinese or English
    single = content or title
    has_chinese = any('\u4e00' <= c <= '\u9fff' for c in single)
    if has_chinese:
        return {"en": title, "zh": single}
    return {"en": single, "zh": single}


def _fallback_searchable_text(row: dict, english_summary: str, chinese_summary: str, diff_keywords: str = "") -> str:
    parts = [
        row.get("title", ""),
        english_summary,
        chinese_summary,
        diff_keywords,
    ]
    return "\n".join(p for p in parts if p)


def _normalize_summary_item(row: dict, item: dict) -> dict:
    """Normalize summarizer output while keeping existing pr_data summary column names."""
    english = item.get("english_summary") or item.get("ai_summary_en") or item.get("en") or ""
    chinese = item.get("chinese_summary") or item.get("ai_summary") or item.get("zh") or ""
    diff_keywords = item.get("diff_keywords") or ""
    searchable_text = item.get("searchable_text") or _fallback_searchable_text(
        row, english, chinese, diff_keywords)

    if not english or not chinese or not searchable_text:
        raise RuntimeError(
            f"Invalid summary for PR #{row['pr_number']}: missing english_summary, "
            "chinese_summary, or searchable_text")

    return {
        "pr_number": int(item.get("pr_number") or row["pr_number"]),
        "title": item.get("title") or row.get("title", ""),
        "ai_summary": chinese,
        "ai_summary_en": english,
        "diff_keywords": diff_keywords,
        "searchable_text": searchable_text,
    }


def _load_summary_json(text: str) -> list[dict]:
    """Parse a JSON array, tolerating accidental text around the array."""
    text = text.strip()
    if not text:
        raise RuntimeError("Summarizer returned empty output")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("[")
        end = text.rfind("]")
        if start < 0 or end < start:
            raise
        data = json.loads(text[start:end + 1])
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise RuntimeError("Summarizer output must be a JSON array")
    return data


def _format_batch_progress(batch_index: int | None, batch_total: int | None) -> str:
    if batch_index is None or batch_total is None:
        return ""
    return f" [{batch_index}/{batch_total}]"


def _snapshot_prjson_tmp_dirs() -> set[Path]:
    cwd = Path.cwd().resolve()
    return {
        p.resolve()
        for p in cwd.iterdir()
        if p.is_dir() and p.name.startswith("tmp_req_") and p.parent.resolve() == cwd
    }


def _tmp_dir_matches_pr_batch(path: Path, pr_numbers: list[int]) -> bool:
    tokens = set(re.findall(r"\d+", path.name))
    return all(str(n) in tokens for n in pr_numbers)


def _cleanup_prjson_tmp_dirs(before: set[Path], pr_numbers: list[int]):
    if not PR_SUMMARY_CLEAN_TMP:
        return
    cwd = Path.cwd().resolve()
    for path in sorted(_snapshot_prjson_tmp_dirs() - before):
        if path.parent.resolve() != cwd or not path.name.startswith("tmp_req_"):
            continue
        if not _tmp_dir_matches_pr_batch(path, pr_numbers):
            continue
        try:
            shutil.rmtree(path)
            print(f"    Removed temporary PR JSON directory: {path.name}")
        except OSError as e:
            print(f"    Warning: failed to remove temporary directory {path}: {e}")


def _run_summarizer_json(cmd: list[str], provider: str, nums: str, pr_numbers: list[int], out: Path = None) -> list[dict]:
    attempts = PR_SUMMARY_RETRIES + 1
    last_error = None
    tmp_dirs_before = _snapshot_prjson_tmp_dirs()
    for attempt in range(1, attempts + 1):
        try:
            if out is not None and out.exists():
                out.unlink()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=PR_SUMMARY_TIMEOUT)
            if result.returncode == 0:
                if out is not None and not out.exists():
                    raise RuntimeError(f"{provider} did not write summary output: {out}")
                text = out.read_text() if out is not None else result.stdout
                try:
                    data = _load_summary_json(text)
                    _cleanup_prjson_tmp_dirs(tmp_dirs_before, pr_numbers)
                    return data
                except json.JSONDecodeError as e:
                    bad_file = None
                    if out is not None:
                        bad_file = out.with_suffix(out.suffix + f".bad_attempt_{attempt}")
                        bad_file.write_text(text)
                    msg = f"{provider} returned invalid JSON for PR batch {nums}: {e}"
                    if bad_file is not None:
                        msg += f" (saved to {bad_file})"
                    raise RuntimeError(msg)

            stderr = result.stderr.strip()
            stdout = result.stdout.strip()
            if provider == "Codex" and "SyntaxError: Unexpected reserved word" in stderr and "@openai/codex" in stderr:
                raise RuntimeError(
                    "Codex CLI failed before running the PR summarizer. The installed codex command "
                    "is the npm package @openai/codex, and the active Node.js runtime is too old for "
                    "its top-level await syntax. Run `node -v` and use Node.js 18+ (preferably 20+), "
                    "or set CODEX_BIN to a codex executable that works in this environment."
                )
            raise RuntimeError(stderr or stdout or f"{provider} exited with code {result.returncode}")
        except subprocess.TimeoutExpired as e:
            last_error = RuntimeError(
                f"{provider} PR summary batch timed out after {PR_SUMMARY_TIMEOUT}s: {nums}")
        except RuntimeError as e:
            last_error = e

        if attempt < attempts:
            print(f"    {provider} summary batch failed (attempt {attempt}/{attempts}): {last_error}")
            if PR_SUMMARY_RETRY_SLEEP > 0:
                print(f"    Sleeping {PR_SUMMARY_RETRY_SLEEP}s before retry ...")
                time.sleep(PR_SUMMARY_RETRY_SLEEP)

    raise last_error


def _summarize_batch_ollama(rows: list[dict], batch_index: int = None, batch_total: int = None) -> dict[int, dict]:
    result = {}
    for row in rows:
        print(f"    Summarizing PR #{row['pr_number']} with Ollama{_format_batch_progress(batch_index, batch_total)} ...")
        summaries = ollama_summarize(row["title"], row.get("body") or "")
        item = _normalize_summary_item(row, {
            "pr_number": row["pr_number"],
            "title": row["title"],
            "english_summary": summaries["en"],
            "chinese_summary": summaries["zh"],
            "diff_keywords": "",
            "searchable_text": _fallback_searchable_text(row, summaries["en"], summaries["zh"]),
        })
        result[row["pr_number"]] = item
    return result


def _summarize_batch_codex(rows: list[dict], batch_index: int = None, batch_total: int = None) -> dict[int, dict]:
    pr_numbers = [int(r["pr_number"]) for r in rows]
    nums = ",".join(str(n) for n in pr_numbers)
    prompt = f"用 pr-json-summarizer 分析 pr {nums}"
    out = Path(tempfile.gettempdir()) / f"pr_summary_codex_{os.getpid()}_{pr_numbers[0]}_{pr_numbers[-1]}.json"
    cmd = [
        CODEX_BIN, "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "-o", str(out),
    ]
    if CODEX_MODEL:
        cmd.extend(["--model", CODEX_MODEL])
    cmd.append(prompt)
    print(f"    Summarizing PR batch with Codex{_format_batch_progress(batch_index, batch_total)}: {nums}")
    items = _run_summarizer_json(cmd, "Codex", nums, pr_numbers, out)
    rows_by_pr = {int(r["pr_number"]): r for r in rows}
    return {
        int(item["pr_number"]): _normalize_summary_item(rows_by_pr[int(item["pr_number"])], item)
        for item in items
    }


def _summarize_batch_gemini(rows: list[dict], batch_index: int = None, batch_total: int = None) -> dict[int, dict]:
    pr_numbers = [int(r["pr_number"]) for r in rows]
    nums = ",".join(str(n) for n in pr_numbers)
    prompt = f"用 pr-json-summarizer 分析 pr {nums}"
    cmd = [
        GEMINI_BIN,
        "--approval-mode", "yolo",
        "--output-format", "text",
    ]
    if GEMINI_MODEL:
        cmd.extend(["--model", GEMINI_MODEL])
    cmd.extend(["-p", prompt])
    print(f"    Summarizing PR batch with Gemini{_format_batch_progress(batch_index, batch_total)}: {nums}")
    items = _run_summarizer_json(cmd, "Gemini", nums, pr_numbers)
    rows_by_pr = {int(r["pr_number"]): r for r in rows}
    return {
        int(item["pr_number"]): _normalize_summary_item(rows_by_pr[int(item["pr_number"])], item)
        for item in items
    }


def summarize_pr_batch(rows: list[dict], batch_index: int = None, batch_total: int = None) -> dict[int, dict]:
    if PR_SUMMARY_PROVIDER == "ollama":
        return _summarize_batch_ollama(rows, batch_index, batch_total)
    if PR_SUMMARY_PROVIDER == "codex":
        return _summarize_batch_codex(rows, batch_index, batch_total)
    if PR_SUMMARY_PROVIDER == "gemini":
        return _summarize_batch_gemini(rows, batch_index, batch_total)
    raise RuntimeError(f"Unsupported PR_SUMMARY_PROVIDER: {PR_SUMMARY_PROVIDER}")


def _chunks(items: list, size: int):
    if size <= 0:
        raise RuntimeError("PR_SUMMARY_BATCH_SIZE must be positive")
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _is_current_enriched(row: dict) -> bool:
    required = ("ai_summary", "ai_summary_en", "searchable_text", "embedding")
    return all(row.get(k) for k in required)


def parse_dt(s: str | None) -> str | None:
    """Parse ISO datetime string to 'YYYY-MM-DD HH:MM:SS' in UTC+8."""
    if not s:
        return None
    dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=8)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def cmd_pipeline(args):
    """Execute full workflow: fetch → enrich → load → link-backport."""
    since = args.since or (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    until = args.until or datetime.now().strftime("%Y-%m-%d")

    print(f">>> Starting Pipeline [{since} .. {until}] ...")

    print("\n--- [1/4] Fetching raw PR data ---")
    cmd_fetch(args)

    print("\n--- [2/4] Generating AI summaries and embeddings (enrich) ---")
    # cmd_enrich needs file=None to use since/until logic
    args.file = None
    args.output = None
    cmd_enrich(args)

    print("\n--- [3/4] Loading enriched data into StarRocks ---")
    cmd_load(args)

    print("\n--- [4/4] Linking backport versions ---")
    cmd_link_backport(args)

    print(f"\n>>> Pipeline Completed Successfully [{since} .. {until}].")

# --- Step 1: fetch → process → save JSON ---

def cmd_fetch(args):
    """Fetch raw PR data from GitHub and save to JSON file."""
    if args.since:
        since = args.since
    else:
        since = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    prs = fetch_prs(since, args.until)
    if not prs:
        print("No PRs found.")
        return

    rows = []
    for pr in prs:
        num = pr["number"]
        title = pr.get("title", "")
        body = pr.get("body") or ""
        author = (pr.get("author") or {}).get("login", "unknown")
        labels = ",".join(lb.get("name", "") for lb in (pr.get("labels") or []))

        row = {
            "pr_number": num,
            "title": title,
            "author": author,
            "labels": labels,
            "created_at": parse_dt(pr.get("createdAt")),
            "merged_at": parse_dt(pr.get("mergedAt")),
            "additions": pr.get("additions", 0),
            "deletions": pr.get("deletions", 0),
            "changed_files": len(pr.get("files") or []) or pr.get("changedFiles", 0),
            "module": infer_module(pr),
            "change_type": parse_change_type(title, body),
            "version": infer_version(labels),
            "body": body[:10000],
        }
        rows.append(row)

    # Group by merged date, one file per day
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    by_date = {}
    for row in rows:
        merged = row.get("merged_at") or row.get("created_at") or ""
        date_key = merged[:10].replace("-", "") if merged else "unknown"
        by_date.setdefault(date_key, []).append(row)

    total_saved = 0
    for date_key, day_rows in sorted(by_date.items()):
        out_file = RAW_DIR / f"pr_raw_{date_key}.json"

        # Append to existing file if it exists
        existing = []
        if out_file.exists():
            with open(out_file) as f:
                existing = json.load(f)
            existing_nums = {r["pr_number"] for r in existing}
            new_rows = [r for r in day_rows if r["pr_number"] not in existing_nums]
            day_rows = existing + new_rows
        else:
            new_rows = day_rows

        with open(out_file, "w") as f:
            json.dump(day_rows, f, ensure_ascii=False, indent=2)

        if new_rows:
            print(f"\n  {out_file.name}: {len(new_rows)} new, {len(day_rows)} total")
            for r in new_rows:
                print(f"    #{r['pr_number']} [{r['change_type']}] [{r['module']}] {r['title'][:80]}")
        total_saved += len(new_rows)

    print(f"\nSaved {total_saved} PRs across {len(by_date)} files")


def _date_range(since: str, until: str) -> list[str]:
    """Generate list of date strings (YYYYMMDD) from since to until (inclusive)."""
    start = datetime.strptime(since, "%Y-%m-%d")
    end = datetime.strptime(until, "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return dates


def _collect_raw_files(args) -> list[Path]:
    """Resolve raw JSON files from --file, --since/--until, or --days."""
    if getattr(args, "file", None):
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = RAW_DIR / args.file
        if not file_path.exists():
            print(f"File not found: {args.file}")
            sys.exit(1)
        return [file_path]

    since = getattr(args, "since", None)
    if not since and getattr(args, "days", None):
        since = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    if not since:
        print("Error: must specify --file, --since or --days")
        sys.exit(1)

    until = getattr(args, "until", None) or datetime.now().strftime("%Y-%m-%d")
    dates = _date_range(since, until)
    files = []
    for d in dates:
        f = RAW_DIR / f"pr_raw_{d}.json"
        if f.exists():
            files.append(f)
        else:
            print(f"  Skipping {f.name} (not found)")
    if not files:
        print(f"No raw files found in range [{since} .. {until}].")
        sys.exit(1)
    return files


def _enrich_file(file_path: Path, output: Path = None):
    """Enrich a single raw JSON file."""
    with open(file_path) as f:
        raw_rows = json.load(f)

    # Output file: pr_raw_xxx.json → enriched/pr_enriched_xxx.json
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    out_file = output or \
        ENRICHED_DIR / file_path.name.replace("pr_raw_", "pr_enriched_")

    # Load existing enriched file for resume support
    existing = {}
    if out_file.exists():
        with open(out_file) as f:
            for r in json.load(f):
                existing[r["pr_number"]] = r

    enriched_rows = []
    pending_rows = []
    total = len(raw_rows)
    skip_backport = 0
    for i, row in enumerate(raw_rows):
        pr_num = row["pr_number"]

        # Skip backport PRs (they only contribute version info via link-backport)
        if parse_backport(row.get("title", "")):
            print(f"  [{i+1}/{total}] PR #{pr_num}: skip backport - {row['title'][:80]}")
            skip_backport += 1
            continue

        # Skip already enriched with the current schema
        if pr_num in existing and _is_current_enriched(existing[pr_num]):
            enriched_rows.append(existing[pr_num])
            continue

        print(f"  [{i+1}/{total}] PR #{pr_num}: pending - {row['title'][:80]}...")
        pending_rows.append(row)

    new_count = 0
    batches = list(_chunks(pending_rows, PR_SUMMARY_BATCH_SIZE))
    for batch_index, batch in enumerate(batches, start=1):
        if batch_index > 1 and PR_SUMMARY_BATCH_SLEEP > 0:
            print(f"    Sleeping {PR_SUMMARY_BATCH_SLEEP}s before next PR summary batch ...")
            time.sleep(PR_SUMMARY_BATCH_SLEEP)
        summaries_by_pr = summarize_pr_batch(batch, batch_index, len(batches))
        for row in batch:
            pr_num = row["pr_number"]
            if pr_num not in summaries_by_pr:
                raise RuntimeError(f"Summarizer did not return PR #{pr_num}")
            summary = summaries_by_pr[pr_num]
            print(f"    Embedding PR #{pr_num} ...")
            embedding = ollama_embed(summary["searchable_text"])

            enriched_row = {
                **row,
                "ai_summary": summary["ai_summary"],
                "ai_summary_en": summary["ai_summary_en"],
                "diff_keywords": summary.get("diff_keywords", ""),
                "searchable_text": summary["searchable_text"],
                "embedding": embedding,
            }
            enriched_rows.append(enriched_row)
            new_count += 1

        # Save progress after each batch (resume-friendly)
        with open(out_file, "w") as f:
            json.dump(enriched_rows, f, ensure_ascii=False, indent=2)

    print(f"  {out_file.name}: enriched {new_count}, skipped backport {skip_backport}, skipped existing {total - new_count - skip_backport}")
    return new_count


def cmd_enrich(args):
    """Generate AI summaries + embeddings for raw PR data, save to new enriched files."""
    files = _collect_raw_files(args)
    if getattr(args, 'reverse', False):
        files = list(reversed(files))
    output = Path(args.output) if args.output else None

    # If --output is set with multiple files, ignore it (use default per-file naming)
    if output and len(files) > 1:
        print("Warning: --output ignored when processing multiple files")
        output = None

    total_enriched = 0
    for f in files:
        print(f"\nProcessing {f.name} ...")
        total_enriched += _enrich_file(f, output if len(files) == 1 else None)

    print(f"\nDone. Total enriched: {total_enriched} PRs across {len(files)} files")


# --- Step 2: init table ---

def cmd_init_table(args):
    """Create database and table with vector index."""
    print("Creating database and tables ...")
    ddl = f"CREATE DATABASE IF NOT EXISTS {SR_DB}"
    sr_execute_sql(ddl, database=None)

    if not args.force:
        # Check if table already exists
        try:
            rows = sr_query(f"SHOW TABLES LIKE 'pr_data'")
            if rows:
                print(f"Error: Table {SR_DB}.pr_data already exists. Use --force to drop and recreate.")
                sys.exit(1)
        except Exception:
            pass  # Database may not exist yet, proceed

    drop_data = "DROP TABLE IF EXISTS pr_data;" if args.force else ""
    drop_versions = "DROP TABLE IF EXISTS pr_versions;" if args.force else ""
    ddl = f"""
{drop_data}

CREATE TABLE pr_data (
    pr_number       INT            NOT NULL COMMENT 'PR编号',
    title           VARCHAR(65533) NOT NULL COMMENT '标题',
    author          VARCHAR(256)   COMMENT '作者',
    labels          VARCHAR(65533) COMMENT '标签',
    created_at      DATETIME       COMMENT '创建时间',
    merged_at       DATETIME       COMMENT '合并时间',
    additions       INT            COMMENT '增加行数',
    deletions       INT            COMMENT '删除行数',
    changed_files   INT            COMMENT '变更文件数',
    module          VARCHAR(64)    COMMENT '模块: FE/BE/Docs/Tool',
    change_type     VARCHAR(64)    COMMENT '变更类型',
    version         VARCHAR(64)    NOT NULL DEFAULT 'main' COMMENT '版本: main/4.1.1/...',
    ai_summary      VARCHAR(65533) COMMENT 'AI中文摘要',
    ai_summary_en   VARCHAR(65533) COMMENT 'AI英文摘要',
    diff_keywords   VARCHAR(65533) COMMENT '结构化检索关键词, 用于展示和诊断',
    searchable_text STRING         COMMENT '用于 embedding 和全文检索的合并文本',
    body            STRING         COMMENT 'PR描述',
    embedding       ARRAY<FLOAT>   NOT NULL COMMENT '向量 dim={EMBEDDING_DIM}',
    INDEX vec_idx (embedding) USING VECTOR (
        "index_type" = "hnsw",
        "metric_type" = "cosine_similarity",
        "is_vector_normed" = "false",
        "M" = "16",
        "dim" = "{EMBEDDING_DIM}"
    ),
    INDEX searchable_text_idx (searchable_text) USING GIN("parser" = "standard", "imp_lib" = "builtin")
) ENGINE = OLAP
PRIMARY KEY(pr_number)
DISTRIBUTED BY HASH(pr_number) BUCKETS 1
PROPERTIES("replication_num" = "1");

{drop_versions}

CREATE TABLE pr_versions (
    pr_number     INT          NOT NULL COMMENT '主 PR 编号',
    version       VARCHAR(64)  NOT NULL COMMENT '版本',
    backport_pr   INT          COMMENT 'backport PR 编号, 主版本为 NULL'
) ENGINE = OLAP
PRIMARY KEY(pr_number, version)
DISTRIBUTED BY HASH(pr_number) BUCKETS 1
PROPERTIES("replication_num" = "1");
"""
    sr_execute_sql(ddl)
    print("  Done. Tables pr_analytics.pr_data and pr_analytics.pr_versions created.")


# --- Step 3: load JSON into StarRocks ---

def _collect_enriched_files(args) -> list[Path]:
    """Resolve enriched JSON files from --file, --since/--until, or --days."""
    if getattr(args, "file", None):
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = ENRICHED_DIR / args.file
        if not file_path.exists():
            print(f"File not found: {args.file}")
            sys.exit(1)
        return [file_path]

    since = getattr(args, "since", None)
    if not since and getattr(args, "days", None):
        since = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    if not since:
        print("Error: must specify --file, --since or --days")
        sys.exit(1)

    until = getattr(args, "until", None) or datetime.now().strftime("%Y-%m-%d")
    dates = _date_range(since, until)
    files = []
    for d in dates:
        f = ENRICHED_DIR / f"pr_enriched_{d}.json"
        if f.exists():
            files.append(f)
        else:
            print(f"  Skipping {f.name} (not found)")
    if not files:
        print(f"No enriched files found in range [{since} .. {until}].")
        sys.exit(1)
    return files


def load_versions(version_rows: list):
    """Load version mappings into pr_versions via Stream Load."""
    if not version_rows:
        return
    url = f"http://{SR_HOST}:{SR_HTTP_PORT}/api/{SR_DB}/pr_versions/_stream_load"
    payload = json.dumps(version_rows)
    auth = base64.b64encode(f"{SR_USER}:{SR_PASSWORD}".encode()).decode()
    cmd = [
        "curl", "-s", "-L", "--location-trusted",
        "-X", "PUT",
        "-H", f"Authorization: Basic {auth}",
        "-H", "Content-Type: application/json",
        "-H", "Expect: 100-continue",
        "-H", "format: json",
        "-H", "strip_outer_array: true",
        "-d", "@-",
        "--max-time", "120",
        url,
    ]
    result = subprocess.run(cmd, input=payload, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"Stream Load pr_versions failed: {result.stderr}")
    resp = json.loads(result.stdout)
    status = resp.get("Status", "Unknown")
    loaded = resp.get("NumberLoadedRows", 0)
    print(f"  Stream Load pr_versions: Status={status}, Loaded={loaded} rows.")
    if status not in ("Success", "Publish Timeout"):
        print(f"  Full response: {json.dumps(resp, indent=2)}")


def cmd_load(args):
    """Load enriched JSON files into StarRocks via Stream Load."""
    files = _collect_enriched_files(args)

    total_loaded = 0
    all_versions = []
    for file_path in files:
        with open(file_path) as f:
            rows = json.load(f)
        print(f"Loading {len(rows)} rows from {file_path.name} ...")
        load_to_starrocks(rows)
        total_loaded += len(rows)

        # Collect version mappings (each PR gets its own version entry)
        for row in rows:
            all_versions.append({
                "pr_number": row["pr_number"],
                "version": row.get("version", "main"),
                "backport_pr": None,
            })

    # Load version mappings
    if all_versions:
        print(f"\nLoading {len(all_versions)} version mappings ...")
        load_versions(all_versions)

    print(f"\nDone! Total loaded: {total_loaded} rows from {len(files)} files")


# --- Step 4: link backport versions ---

def cmd_link_backport(args):
    """Scan raw files, extract backport relationships, and load into pr_versions."""
    files = _collect_raw_files(args)

    version_rows = []
    for file_path in files:
        with open(file_path) as f:
            raw_rows = json.load(f)
        for row in raw_rows:
            title = row.get("title", "")
            source_prs = parse_backport(title)
            if not source_prs:
                continue
            # This is a backport PR → extract its version and link to source PRs
            version = row.get("version", "main")
            if version == "main":
                # Backport PRs should have a branch version; skip if still "main"
                continue
            backport_pr = row["pr_number"]
            for src_pr in source_prs:
                version_rows.append({
                    "pr_number": src_pr,
                    "version": version,
                    "backport_pr": backport_pr,
                })

    if not version_rows:
        print("No backport relationships found.")
        return

    print(f"Found {len(version_rows)} backport version mappings, loading into pr_versions ...")
    load_versions(version_rows)
    print("Done.")


# --- Step 5: semantic search ---

def cmd_search(args):
    """Semantic search PRs using vector index."""
    query = args.query
    top_k = args.top

    print(f"Searching: {query}")
    print("  Generating query embedding via Ollama ...")
    query_embedding = ollama_embed(query)
    vec_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

    sql = f"""
SELECT pr_number, title, author, module, change_type, version, ai_summary, ai_summary_en, merged_at,
       approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) AS score
FROM pr_data
WHERE approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) >= 0.3
ORDER BY approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) DESC
LIMIT {top_k};
"""
    print("  Querying StarRocks ...")
    rows = sr_query(sql)

    if not rows:
        print("\nNo results found.")
        return

    print(f"\n{'=' * 80}")
    print(f"  Search: \"{query}\"  |  {len(rows)} results")
    print(f"{'=' * 80}")

    for i, r in enumerate(rows):
        score = float(r.get("score", 0))
        score_bar = "#" * int(score * 20)
        pr_num = r.get('pr_number', '?')
        pr_url = f"https://github.com/{REPO}/pull/{pr_num}"
        print(f"\n  [{i+1}] PR #{pr_num}  "
              f"score: {score:.4f} [{score_bar:<20}]")
        print(f"      Link:    {pr_url}")
        print(f"      Title:   {r.get('title', '')}")
        print(f"      Type:    {r.get('change_type', '')}  |  "
              f"Module: {r.get('module', '')}  |  "
              f"Version: {r.get('version', '')}  |  "
              f"Author: {r.get('author', '')}")
        print(f"      Merged:  {r.get('merged_at', '')}")
        print(f"      Summary: {r.get('ai_summary', '')}")
        if r.get('ai_summary_en'):
            print(f"      English: {r.get('ai_summary_en', '')}")

    print(f"\n{'=' * 80}")


# --- StarRocks Helpers ---

def _get_conn(database=SR_DB):
    """Get a pymysql connection to StarRocks."""
    return pymysql.connect(
        host=SR_HOST,
        port=int(SR_PORT),
        user=SR_USER,
        password=SR_PASSWORD,
        database=database,
        charset="utf8mb4",
    )


def sr_execute_sql(sql: str, database=SR_DB):
    """Execute one or more SQL statements via pymysql."""
    conn = _get_conn(database=database)
    try:
        with conn.cursor() as cur:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def sr_query(sql: str, database=SR_DB) -> list:
    """Execute SQL and return list of dicts."""
    conn = _get_conn(database=database)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            return cur.fetchall()
    finally:
        conn.close()


def load_to_starrocks(rows: list[dict]):
    if not rows:
        print("No rows to load.")
        return

    url = f"http://{SR_HOST}:{SR_HTTP_PORT}/api/{SR_DB}/pr_data/_stream_load"
    payload = json.dumps(rows)

    auth = base64.b64encode(f"{SR_USER}:{SR_PASSWORD}".encode()).decode()

    # Use curl with -L to follow 307 redirect from FE to BE
    cmd = [
        "curl", "-s", "-L", "--location-trusted",
        "-X", "PUT",
        "-H", f"Authorization: Basic {auth}",
        "-H", "Content-Type: application/json",
        "-H", "Expect: 100-continue",
        "-H", "format: json",
        "-H", "strip_outer_array: true",
        "-d", "@-",
        "--max-time", "120",
        url,
    ]
    result = subprocess.run(cmd, input=payload, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(
            f"Stream Load failed (rc={result.returncode}): {result.stderr}")

    resp = json.loads(result.stdout)
    status = resp.get("Status", "Unknown")
    loaded = resp.get("NumberLoadedRows", 0)
    msg = resp.get("Message", "")
    print(f"  Stream Load: Status={status}, Loaded={loaded} rows. {msg}")
    if status not in ("Success", "Publish Timeout"):
        print(f"  Full response: {json.dumps(resp, indent=2)}")


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(description="StarRocks PR Analytics (Ollama)")
    sub = parser.add_subparsers(dest="command", required=True)

    # pipeline: fetch → enrich → load → link-backport
    p_pipe = sub.add_parser("pipeline", help="Run full pipeline: fetch → enrich → load → link-backport")
    p_pipe.add_argument("--days", type=int, default=1, help="Last N days")
    p_pipe.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_pipe.add_argument("--until", type=str, help="End date, e.g. today")
    p_pipe.add_argument("--reverse", action="store_true", help="Process enrich in reverse order")

    # fetch: pull raw PR data from GitHub
    p_fetch = sub.add_parser("fetch", help="Fetch raw PR data from GitHub → save JSON")
    p_fetch.add_argument("--days", type=int, default=1, help="Fetch PRs from last N days (ignored if --since is set)")
    p_fetch.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_fetch.add_argument("--until", type=str, help="End date, e.g. 2025-04-30")

    # enrich: AI summary + embedding
    p_enrich = sub.add_parser("enrich", help="Generate AI summaries + embeddings for raw PR JSON")
    p_enrich.add_argument("--file", type=str, help="Raw PR JSON file path")
    p_enrich.add_argument("--days", type=int, default=1, help="Process last N days (ignored if --since or --file is set)")
    p_enrich.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_enrich.add_argument("--until", type=str, help="End date (default: today)")
    p_enrich.add_argument("--output", type=str, help="Output enriched JSON path (single file only)")
    p_enrich.add_argument("--reverse", action="store_true", help="Process files from newest to oldest")

    # init-table
    p_init = sub.add_parser("init-table", help="Create StarRocks database and table")
    p_init.add_argument("--force", action="store_true", help="Drop and recreate table if it exists")

    # load: import into StarRocks
    p_load = sub.add_parser("load", help="Load enriched JSON files into StarRocks")
    p_load.add_argument("--file", type=str, help="Enriched JSON file path")
    p_load.add_argument("--days", type=int, default=1, help="Process last N days (ignored if --since or --file is set)")
    p_load.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_load.add_argument("--until", type=str, help="End date (default: today)")

    # link-backport: scan raw files and populate pr_versions with backport relationships
    p_link = sub.add_parser("link-backport", help="Scan raw files and load backport version mappings into pr_versions")
    p_link.add_argument("--file", type=str, help="Raw PR JSON file path")
    p_link.add_argument("--days", type=int, default=1, help="Process last N days (ignored if --since or --file is set)")
    p_link.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_link.add_argument("--until", type=str, help="End date (default: today)")

    # search
    p_search = sub.add_parser("search", help="Semantic search PRs")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--top", type=int, default=10, help="Top K results")

    args = parser.parse_args()

    if args.command == "fetch":
        cmd_fetch(args)
    elif args.command == "enrich":
        cmd_enrich(args)
    elif args.command == "init-table":
        cmd_init_table(args)
    elif args.command == "load":
        cmd_load(args)
    elif args.command == "link-backport":
        cmd_link_backport(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "pipeline":
        cmd_pipeline(args)


if __name__ == "__main__":
    main()
