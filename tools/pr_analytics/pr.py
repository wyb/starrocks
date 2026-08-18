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
    # 多数子命令支持 --repo oss|cd|ms (默认 oss)；link-sync 仅企业仓库 cd|ms；init-table/migrate-repo 不分 repo；search 支持 oss|cd|ms|all (默认 all 联合检索)

    # 常用：一键跑通全流程 (fetch + enrich + load + link-backport；企业仓库 --repo cd|ms 额外含 link-sync)
    python3 pr.py pipeline --days 1
    python3 pr.py pipeline --repo ms --since 2025-04-01 --until 2025-04-30

    # Step 1: 拉取 PR 原始数据 (按天存储, 按周分批, 增量去重；企业仓库自动分类打标)
    python3 pr.py fetch --days 1
    python3 pr.py fetch --repo ms --since 2025-04-01 --until 2025-04-30

    # Step 2: AI 增强 (生成摘要 + embedding, 断点续跑, 自动跳过 sync/backport PR)
    python3 pr.py enrich --file data/raw/pr_raw_20250401.json
    python3 pr.py enrich --since 2025-04-01 --until 2025-04-30

    # Step 3: 建表 (pr_data + pr_versions + pr_sync, 支持 --force 强制重建)
    python3 pr.py init-table
    # 迁移旧库到 repo 感知 schema (给 pr_data/pr_versions 加 repo 列 + 建 pr_sync, 一次性幂等)
    python3 pr.py migrate-repo

    # Step 4: 导入 StarRocks (重复导入自动更新, 同时写入 pr_versions 主版本)
    python3 pr.py load --file data/enriched/pr_enriched_20250401.json
    python3 pr.py load --since 2025-04-01 --until 2025-04-30

    # Step 5: 关联版本映射 (link-backport 写 pr_versions；link-sync 写 pr_sync, 仅企业仓库)
    python3 pr.py link-backport --since 2025-04-01 --until 2025-04-30
    python3 pr.py link-sync --repo ms --since 2025-04-01 --until 2025-04-30

    # Step 6: 语义搜索 (--repo oss|cd|ms|all, 默认 all)
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
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "300"))  # per ollama HTTP call
EMBED_MODEL = os.getenv("EMBED_MODEL", "bge-m3")
SUMMARY_MODEL = os.getenv("SUMMARY_MODEL", "qwen3.5:9b")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "1024"))  # bge-m3 = 1024
PR_SUMMARY_PROVIDER = os.getenv("PR_SUMMARY_PROVIDER", "codex").lower()
PR_SUMMARY_BATCH_SIZE = int(os.getenv("PR_SUMMARY_BATCH_SIZE", "5"))
PR_SUMMARY_TIMEOUT = int(os.getenv("PR_SUMMARY_TIMEOUT", "900"))
GH_FETCH_TIMEOUT = int(os.getenv("GH_FETCH_TIMEOUT", "300"))  # per gh-batch wall clock; gh has no HTTP deadline
PR_SUMMARY_BATCH_SLEEP = int(os.getenv("PR_SUMMARY_BATCH_SLEEP", "30"))
PR_SUMMARY_RETRIES = int(os.getenv("PR_SUMMARY_RETRIES", "2"))
PR_SUMMARY_RETRY_SLEEP = int(os.getenv("PR_SUMMARY_RETRY_SLEEP", "30"))
PR_SUMMARY_CLEAN_TMP = os.getenv("PR_SUMMARY_CLEAN_TMP", "1").lower() not in ("0", "false", "no")
CODEX_BIN = os.getenv("CODEX_BIN", "codex")
GEMINI_BIN = os.getenv("GEMINI_BIN", "gemini")
CODEX_MODEL = os.getenv("CODEX_MODEL", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "")

DATA_DIR = Path(__file__).parent / "data"
# Repo registry. `kind` drives all enterprise-vs-oss behavior (classification,
# sync mapping, migration guard) so adding a new source repo is config-only.
# `cd` = CelerData, the current enterprise source; `ms` = MirrorShip (not live yet —
# no PRs until the future cutover, so it is inactive and the daemon skips it).
REPOS = {
    "oss": {
        "slug": "StarRocks/starrocks",
        "kind": "oss",
        "label": "OSS",
        "active": True,
        "raw_dir": DATA_DIR / "raw",             # 现有目录不动，向后兼容
        "enriched_dir": DATA_DIR / "enriched",
    },
    "cd": {
        "slug": "CelerData/celerdata-enterprise",
        "kind": "enterprise",
        "label": "CD",
        "active": False,    # not live yet
        "raw_dir": DATA_DIR / "cd" / "raw",
        "enriched_dir": DATA_DIR / "cd" / "enriched",
    },
    "ms": {
        "slug": "MirrorShipDB/mirrorship-enterprise",
        "kind": "enterprise",
        "label": "MS",
        "active": False,   # not live yet: enterprise source is still cd; daemon skips until cutover
        "raw_dir": DATA_DIR / "ms" / "raw",
        "enriched_dir": DATA_DIR / "ms" / "enriched",
    },
}


def is_enterprise(repo: str) -> bool:
    """True for any enterprise-kind repo (celerdata, mirrorship, ...)."""
    return REPOS.get(repo, {}).get("kind") == "enterprise"


def _assert_known_repo(repo: str, ctx: str):
    """Fail closed on an unrecognized repo code (e.g. stale data from before a repo-code
    rename). Otherwise `is_enterprise` returns False (bypassing the migration guard),
    `REPOS[repo]` KeyErrors, or a wrong-repo GitHub link gets rendered for the row."""
    if repo not in REPOS:
        raise RuntimeError(
            f"Unknown repo code {repo!r} in {ctx} — valid codes are {list(REPOS)}. "
            "Likely stale data from before a repo-code rename; re-fetch or remove the file.")


ENTERPRISE_REPOS = [r for r, m in REPOS.items() if m.get("kind") == "enterprise"]
ACTIVE_REPOS = [r for r, m in REPOS.items() if m.get("active", True)]  # repos the daemon polls
REPO_CHOICES = list(REPOS.keys())            # --repo for fetch/enrich/load/link-backport/pipeline

# Backward-compat aliases (oss defaults)
REPO = REPOS["oss"]["slug"]
RAW_DIR = REPOS["oss"]["raw_dir"]
ENRICHED_DIR = REPOS["oss"]["enriched_dir"]


# --- GitHub Data Fetching ---

def _fetch_prs_batch(repo: str, since: str, until: str) -> list[dict]:
    """Fetch a single batch of PRs for a date range."""
    # GitHub search uses UTC; shift -8h to compensate for UTC+8 timezone
    utc_since = (datetime.strptime(since, "%Y-%m-%d") - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S")
    utc_until = (datetime.strptime(until, "%Y-%m-%d") + timedelta(hours=24 - 8)).strftime("%Y-%m-%dT%H:%M:%S")
    date_range = f"merged:{utc_since}..{utc_until}"
    cmd = [
        "gh", "pr", "list",
        "--repo", REPOS[repo]["slug"],
        "--state", "merged",
        "--limit", "1000",
        "--search", date_range,
        "--json", "number,title,body,labels,author,mergedAt,createdAt,"
                  "additions,deletions,changedFiles,files,baseRefName",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=GH_FETCH_TIMEOUT)
    except subprocess.TimeoutExpired:
        # gh has no HTTP deadline; without this an hourly daemon can freeze forever on a
        # stalled connection/proxy. Treat as a batch failure (warn + skip) like a gh error.
        print(f"  Warning: gh timed out for {since}..{until} after {GH_FETCH_TIMEOUT}s")
        return []
    if result.returncode != 0:
        print(f"  Warning: gh failed for {since}..{until}: {result.stderr.strip()}")
        return []
    return json.loads(result.stdout)


def _warn_if_capped(label: str, prs: list):
    """Warn when a single-day fetch hits the 1000-row GitHub search cap — one day is the
    finest granularity we split to, so results for that day may be silently truncated."""
    if len(prs) >= 1000:
        print(f"\n  WARNING: {label} returned {len(prs)} PRs at the GitHub search 1000-row cap; "
              f"results for this day may be TRUNCATED (cannot split finer than one day).", end=" ")


def fetch_prs(repo: str, since: str, until: str = None) -> list[dict]:
    """Fetch PRs, splitting into weekly batches to avoid GitHub API limits."""
    start = datetime.strptime(since, "%Y-%m-%d")
    end = datetime.strptime(until, "%Y-%m-%d") if until else datetime.now()
    print(f"Fetching {REPOS[repo]['slug']} PRs merged {since} .. {end.strftime('%Y-%m-%d')} ...")

    all_prs = []
    seen = set()
    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=6), end)
        s = batch_start.strftime("%Y-%m-%d")
        e = batch_end.strftime("%Y-%m-%d")
        print(f"  Batch {s} .. {e} ...", end=" ")
        prs = _fetch_prs_batch(repo, s, e)
        if len(prs) >= 1000 and s != e:
            # GitHub search caps at 1000: suspected truncation, re-fetch per day
            print("hit 1000-row cap, splitting per day ...", end=" ")
            prs = []
            d = batch_start
            while d <= batch_end:
                ds = d.strftime("%Y-%m-%d")
                day_prs = _fetch_prs_batch(repo, ds, ds)
                _warn_if_capped(ds, day_prs)
                prs.extend(day_prs)
                d += timedelta(days=1)
        elif len(prs) >= 1000:
            # Single-day batch already at finest granularity — cannot split further
            _warn_if_capped(s, prs)
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


SYNC_RE = re.compile(r"\(sync\s+#(\d+)\)", re.IGNORECASE)
CONFLICT_TITLE_RE = re.compile(
    r"^fix conflict|resolve committed merge conflict|resolve .*sync conflict",
    re.IGNORECASE)
SYNC_BRANCH_RE = re.compile(r"sync-pr-\d+$")


def parse_sync(title: str) -> list[int]:
    """Extract OSS source PR numbers from '(sync #N)' markers."""
    return [int(m) for m in SYNC_RE.findall(title or "")]


def clean_base_ref(base_ref: str) -> str:
    """Derive branch-granularity version from a base ref.
    main-sync-pr-76688 -> main; branch-4.1-sync-pr-76229 -> 4.1; branch-3.5 -> 3.5;
    mergify/bp/branch-4.0/pr-51951 -> 4.0 (Mergify backport branch -> its target branch)."""
    b = base_ref or ""
    # Mergify backport/copy branches encode the real target branch in the middle segment:
    #   mergify/bp/branch-4.0/pr-51951 -> branch-4.0 -> 4.0 ; mergify/bp/main/pr-123 -> main.
    # Without unwrapping it here the whole ref leaks through as the "version" string.
    m = re.match(r"^mergify/(?:bp|copy)/(.+)/pr-\d+$", b)
    if m:
        b = m.group(1)
    b = re.sub(r"-sync-pr-\d+$", "", b)
    if not b or b == "main":
        return "main"
    m = re.match(r"branch-(.+)$", b)
    if m:
        return m.group(1)
    return b


def normalize_version(v: str) -> str:
    """Unify version granularity by dropping the -ee (enterprise edition) suffix, e.g.
    4.1.4-ee -> 4.1.4, so a version is comparable across the OSS/enterprise split and
    pr_data/pr_versions/pr_sync join on one canonical version key."""
    v = re.sub(r"(?:-(?:ee|cc))+$", "", (v or "").strip())
    return v or "main"


def derive_version(repo: str, labels: str, base_ref: str) -> str:
    """version:x.y.z(-ee) label first; otherwise fall back to the base branch — for ANY repo.
    Without the base_ref fallback for oss too, a backport merged into an unlabeled release
    branch (one that never gets a version: label) derives "main" and is then
    silently dropped by link-backport; the fallback gives it a branch-granularity version.
    The result is normalize_version()'d (no -ee suffix) and clamped to the version
    VARCHAR(64) column. (repo kept for call-site clarity.)"""
    v = infer_version(labels)
    if v == "main":
        v = clean_base_ref(base_ref)
    return normalize_version(v)[:64]


def classify_ent_pr(title: str, labels: str, base_ref: str) -> tuple[str, int | None]:
    """Classify an enterprise PR. Returns (pr_kind, sync_source_pr).
    Priority: sync-by-title > conflict_fix > sync-by-label > backport > exclusive.
    The conflict_fix base_ref signal (`*-sync-pr-N`) / conflict title is definitive and
    outranks a bare `sync` label, so a conflict-resolution PR that also happens to carry a
    `sync` label is not swallowed as sync (and thus still gets enriched as SyncFix)."""
    sync_srcs = parse_sync(title)
    if sync_srcs:
        return "sync", sync_srcs[0]
    if SYNC_BRANCH_RE.search(base_ref or "") or CONFLICT_TITLE_RE.search(title or ""):
        return "conflict_fix", None
    if "sync" in (labels or "").split(","):
        return "sync", None  # label-only sync (no title marker, not a conflict branch)
    if parse_backport(title or ""):
        return "backport", None
    return "exclusive", None


def infer_version(labels: str) -> str:
    """Extract version from labels like 'version:4.1.1'."""
    m = re.search(r"version[:\s]*([\d]+\.[\d]+(?:\.[\d]+)?(?:-ee)?)", labels)
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

def _ollama_post(path: str, body: dict, timeout: int = OLLAMA_TIMEOUT) -> dict:
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


def _finalize_summaries(rows: list[dict], items: list[dict]) -> dict[int, dict]:
    """Map summarizer items back to their rows and normalize. Raises RuntimeError (which the
    caller's retry loop catches) on any content-level mismatch — a renumbered/hallucinated
    pr_number, an unrequested PR, a missing PR, or an item missing required summary fields —
    so these non-deterministic LLM failure modes get retried instead of raising uncaught."""
    rows_by_pr = {int(r["pr_number"]): r for r in rows}
    result = {}
    for item in items:
        try:
            pn = int(item["pr_number"])
        except (KeyError, TypeError, ValueError):
            raise RuntimeError(f"Summarizer item has missing/invalid pr_number: {str(item)[:200]}")
        if pn not in rows_by_pr:
            raise RuntimeError(f"Summarizer returned unrequested PR #{pn} (batch was {sorted(rows_by_pr)})")
        result[pn] = _normalize_summary_item(rows_by_pr[pn], item)
    missing = sorted(set(rows_by_pr) - set(result))
    if missing:
        raise RuntimeError(f"Summarizer did not return PR(s) {missing}")
    return result


def _run_summarizer_json(cmd: list[str], provider: str, nums: str, pr_numbers: list[int], out: Path = None, finalize=None):
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
                except json.JSONDecodeError as e:
                    bad_file = None
                    if out is not None:
                        bad_file = out.with_suffix(out.suffix + f".bad_attempt_{attempt}")
                        bad_file.write_text(text)
                    msg = f"{provider} returned invalid JSON for PR batch {nums}: {e}"
                    if bad_file is not None:
                        msg += f" (saved to {bad_file})"
                    raise RuntimeError(msg)
                # content validation runs INSIDE the retry loop: a renumbered/missing/incomplete
                # item raises RuntimeError here and is retried, not raised uncaught after the loop.
                out_val = finalize(data) if finalize else data
                _cleanup_prjson_tmp_dirs(tmp_dirs_before, pr_numbers)
                return out_val

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


def _pr_refs(rows: list[dict]) -> str:
    """PR references for the summarizer prompt: bare numbers for oss, full URLs for enterprise repos."""
    repo = rows[0].get("repo", "oss")
    if repo == "oss":
        return ",".join(str(int(r["pr_number"])) for r in rows)
    slug = REPOS[repo]["slug"]
    return ",".join(f"https://github.com/{slug}/pull/{int(r['pr_number'])}" for r in rows)


def _summarize_batch_codex(rows: list[dict], batch_index: int = None, batch_total: int = None) -> dict[int, dict]:
    pr_numbers = [int(r["pr_number"]) for r in rows]
    nums = ",".join(str(n) for n in pr_numbers)
    refs = _pr_refs(rows)
    prompt = f"用 pr-json-summarizer 分析 pr {refs}"
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
    return _run_summarizer_json(cmd, "Codex", nums, pr_numbers, out,
                                finalize=lambda data: _finalize_summaries(rows, data))


def _summarize_batch_gemini(rows: list[dict], batch_index: int = None, batch_total: int = None) -> dict[int, dict]:
    pr_numbers = [int(r["pr_number"]) for r in rows]
    nums = ",".join(str(n) for n in pr_numbers)
    refs = _pr_refs(rows)
    prompt = f"用 pr-json-summarizer 分析 pr {refs}"
    cmd = [
        GEMINI_BIN,
        "--approval-mode", "yolo",
        "--output-format", "text",
    ]
    if GEMINI_MODEL:
        cmd.extend(["--model", GEMINI_MODEL])
    cmd.extend(["-p", prompt])
    print(f"    Summarizing PR batch with Gemini{_format_batch_progress(batch_index, batch_total)}: {nums}")
    return _run_summarizer_json(cmd, "Gemini", nums, pr_numbers,
                                finalize=lambda data: _finalize_summaries(rows, data))


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
    """Execute full workflow: fetch → (link-sync for enterprise) → enrich → load → link-backport."""
    since = args.since or (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    until = args.until or datetime.now().strftime("%Y-%m-%d")

    print(f">>> Starting Pipeline [{since} .. {until}] ...")

    print("\n--- Fetching raw PR data ---")
    cmd_fetch(args)

    if is_enterprise(getattr(args, "repo", "oss")):
        print("\n--- Linking sync mappings ---")
        cmd_link_sync(args)

    print("\n--- Generating AI summaries and embeddings (enrich) ---")
    # cmd_enrich needs file=None to use since/until logic
    args.file = None
    args.output = None
    cmd_enrich(args)

    print("\n--- Loading enriched data into StarRocks ---")
    cmd_load(args)

    print("\n--- Linking backport versions ---")
    cmd_link_backport(args)

    print(f"\n>>> Pipeline Completed Successfully [{since} .. {until}].")

# --- Step 1: fetch → process → save JSON ---

def cmd_fetch(args):
    """Fetch raw PR data from GitHub and save to JSON file."""
    repo = getattr(args, "repo", "oss")
    if args.since:
        since = args.since
    else:
        since = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    prs = fetch_prs(repo, since, args.until)
    if not prs:
        print("No PRs found.")
        return
    rows = []
    kind_counter = {}
    for pr in prs:
        num = pr["number"]
        title = pr.get("title", "")
        body = pr.get("body") or ""
        author = (pr.get("author") or {}).get("login", "unknown")
        labels = ",".join(lb.get("name", "") for lb in (pr.get("labels") or []))
        base_ref = pr.get("baseRefName") or ""

        if is_enterprise(repo):
            pr_kind, sync_source_pr = classify_ent_pr(title, labels, base_ref)
            if pr_kind == "sync" and sync_source_pr is None:
                print(f"  Warning: {repo.upper()} PR #{num} has 'sync' label but no '(sync #N)' in title; no mapping row")
        else:
            pr_kind = "backport" if parse_backport(title) else "exclusive"
            sync_source_pr = None
        kind_counter[pr_kind] = kind_counter.get(pr_kind, 0) + 1

        change_type = "SyncFix" if pr_kind == "conflict_fix" else parse_change_type(title, body)

        row = {
            "pr_number": num,
            "repo": repo,
            "title": title,
            "author": author,
            "labels": labels,
            "base_ref": base_ref,
            "pr_kind": pr_kind,
            "sync_source_pr": sync_source_pr,
            "created_at": parse_dt(pr.get("createdAt")),
            "merged_at": parse_dt(pr.get("mergedAt")),
            "additions": pr.get("additions", 0),
            "deletions": pr.get("deletions", 0),
            "changed_files": len(pr.get("files") or []) or pr.get("changedFiles", 0),
            "module": infer_module(pr),
            "change_type": change_type,
            "version": derive_version(repo, labels, base_ref),
            "body": body[:10000],
        }
        rows.append(row)
    print(f"  Kind breakdown: {kind_counter}")

    # Group by merged date, one file per day
    raw_dir = REPOS[repo]["raw_dir"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    by_date = {}
    for row in rows:
        merged = row.get("merged_at") or row.get("created_at") or ""
        date_key = merged[:10].replace("-", "") if merged else "unknown"
        by_date.setdefault(date_key, []).append(row)

    total_saved = 0
    for date_key, day_rows in sorted(by_date.items()):
        out_file = raw_dir / f"pr_raw_{date_key}.json"

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
    raw_dir = REPOS[getattr(args, "repo", "oss")]["raw_dir"]

    if getattr(args, "file", None):
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = raw_dir / args.file
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
        f = raw_dir / f"pr_raw_{d}.json"
        if f.exists():
            files.append(f)
        else:
            print(f"  Skipping {f.name} (not found)")
    if not files:
        # No files in the window is a no-op, NOT an error: pipeline steps after this
        # (enrich/load/link-backport/link-sync) must still run, and the daemon must not
        # report failure just because a quiet window produced nothing.
        print(f"No raw files found in range [{since} .. {until}].")
        return []
    return files


def _row_skip_kind(row: dict) -> str | None:
    """Return the pr_kind if this raw row must NOT be enriched, else None. sync/backport are
    pure mapping/version rows; conflict_fix is a mechanical sync-conflict-resolution PR (its
    change_type is SyncFix) with no standalone semantic value and no mapping contribution — it
    stays recorded in raw but is kept out of enriched/pr_data. Old raw files (no pr_kind) fall
    back to title-based backport detection."""
    kind = row.get("pr_kind")
    if kind is None:
        kind = "backport" if parse_backport(row.get("title", "")) else "exclusive"
    return kind if kind in ("sync", "backport", "conflict_fix") else None


def _enrich_file(file_path: Path, output: Path = None):
    """Enrich a single raw JSON file."""
    with open(file_path) as f:
        raw_rows = json.load(f)

    # Output file: pr_raw_xxx.json → enriched/pr_enriched_xxx.json
    file_repo = raw_rows[0].get("repo", "oss") if raw_rows else "oss"
    _assert_known_repo(file_repo, file_path.name)
    enriched_dir = REPOS[file_repo]["enriched_dir"]
    enriched_dir.mkdir(parents=True, exist_ok=True)
    out_file = output or enriched_dir / file_path.name.replace("pr_raw_", "pr_enriched_")

    # Load existing enriched file for resume support. Drop any conflict_fix/SyncFix rows on the
    # way in: these are no longer enriched, so a file written before that change may still carry
    # them — filtering here keeps them out of the rewritten output (and forces a fresh re-enrich
    # if such a PR was later reclassified to an enrichable kind). purged_syncfix also forces the
    # final write below, so the removal persists even when the raw file has no enrichable rows.
    existing = {}
    purged_syncfix = 0
    if out_file.exists():
        with open(out_file) as f:
            for r in json.load(f):
                if r.get("pr_kind") == "conflict_fix" or r.get("change_type") == "SyncFix":
                    purged_syncfix += 1
                    continue
                existing[r["pr_number"]] = r

    enriched_rows = []
    pending_rows = []
    total = len(raw_rows)
    skipped = 0
    for i, row in enumerate(raw_rows):
        pr_num = row["pr_number"]

        # Skip sync/backport/conflict_fix PRs: sync/backport only carry mapping/version info, and
        # conflict_fix is a mechanical sync-conflict-resolution PR (SyncFix) with no standalone
        # value. All stay in raw but are kept out of enriched/pr_data.
        skip_kind = _row_skip_kind(row)
        if skip_kind:
            print(f"  [{i+1}/{total}] PR #{pr_num}: skip {skip_kind} - {row['title'][:80]}")
            skipped += 1
            continue

        # Already enriched with the current schema: keep the expensive summary/embedding from
        # the existing record, but refresh every other field (version, base_ref, pr_kind,
        # change_type, module, title, body, ...) from the freshly-fetched raw row. `{**existing,
        # **row}` lets raw overwrite all shared keys while the enrich-only fields — ai_summary,
        # ai_summary_en, diff_keywords, searchable_text, embedding, which are absent from raw —
        # survive. So re-running enrich after a re-fetch propagates metadata fixes without re-
        # summarizing / re-embedding.
        if pr_num in existing and _is_current_enriched(existing[pr_num]):
            enriched_rows.append({**existing[pr_num], **row})
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

    # When there were no pending batches (every kept row was already enriched) the per-batch save
    # above never ran, yet reused rows may have had metadata refreshed from raw, or stale SyncFix
    # rows may have been purged from the existing file — either way write it back here. If batches
    # ran, the last per-batch save already persisted the full set, so skip the redundant (large,
    # embedding-heavy) rewrite. The `enriched_rows or purged_syncfix` guard still stops a
    # spuriously empty/all-skip raw (with nothing to purge) from blanking a good enriched file.
    if not batches and (enriched_rows or purged_syncfix):
        with open(out_file, "w") as f:
            json.dump(enriched_rows, f, ensure_ascii=False, indent=2)

    reused = total - new_count - skipped
    print(f"  {out_file.name}: enriched {new_count}, skipped {skipped} (sync/backport/conflict_fix), "
          f"reused {reused} (summary/embedding kept, metadata refreshed)"
          + (f", purged {purged_syncfix} stale SyncFix from existing" if purged_syncfix else ""))
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

def _pr_data_ddl(table: str) -> str:
    return f"""
CREATE TABLE {table} (
    pr_number       INT            NOT NULL COMMENT 'PR编号',
    repo            VARCHAR(16)    NOT NULL DEFAULT 'oss' COMMENT '仓库: oss/cd/ms',
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
    version         VARCHAR(64)    NOT NULL DEFAULT 'main' COMMENT '版本: main/4.1.1/4.1.4/... (归一后无 -ee)',
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
PRIMARY KEY(pr_number, repo)
DISTRIBUTED BY HASH(pr_number) BUCKETS 1
PROPERTIES("replication_num" = "1")
"""


def _pr_versions_ddl(table: str) -> str:
    return f"""
CREATE TABLE {table} (
    pr_number     INT          NOT NULL COMMENT '主 PR 编号',
    repo          VARCHAR(16)  NOT NULL DEFAULT 'oss' COMMENT '仓库: oss/cd/ms',
    version       VARCHAR(64)  NOT NULL COMMENT '版本',
    backport_pr   INT          COMMENT 'backport PR 编号, 主版本为 NULL'
) ENGINE = OLAP
PRIMARY KEY(pr_number, repo, version)
DISTRIBUTED BY HASH(pr_number) BUCKETS 1
PROPERTIES("replication_num" = "1")
"""


def _pr_sync_ddl() -> str:
    return """
CREATE TABLE pr_sync (
    oss_pr         INT          NOT NULL COMMENT '开源 PR 编号 (sync #N)',
    ent_pr         INT          NOT NULL COMMENT '企业 PR 编号',
    ent_repo       VARCHAR(16)  NOT NULL DEFAULT 'cd' COMMENT '企业仓库短码: cd/ms',
    version        VARCHAR(64)  COMMENT '企业侧落点: main/X.Y/x.y.z (归一后无 -ee)',
    ent_merged_at  DATETIME     COMMENT '企业 PR 合并时间'
) ENGINE = OLAP
PRIMARY KEY(oss_pr, ent_pr, ent_repo)
DISTRIBUTED BY HASH(oss_pr) BUCKETS 1
PROPERTIES("replication_num" = "1")
"""


def _ensure_pr_sync_schema():
    """Create pr_sync if missing; if it exists WITHOUT the ent_repo column
    (pre-mirrorship schema), drop and recreate it — the mapping is re-derivable
    via `link-sync`, so nothing is lost that a re-run cannot rebuild. ent_repo is
    a primary-key column, so it cannot be added by ALTER."""
    if not sr_query("SHOW TABLES LIKE 'pr_sync'"):
        sr_execute_sql(_pr_sync_ddl())
        print("  pr_sync created.")
        return
    cols = sr_query("DESC pr_sync")
    if not any(c.get("Field") == "ent_repo" for c in cols):
        print("  pr_sync lacks 'ent_repo' (pre-mirrorship schema) — dropping and recreating; "
              "re-run `link-sync --repo cd` and `--repo ms` to repopulate.")
        sr_execute_sql("DROP TABLE pr_sync; " + _pr_sync_ddl())


def cmd_init_table(args):
    """Create database and tables with vector index (schema v2: repo-aware)."""
    print("Creating database and tables ...")
    sr_execute_sql(f"CREATE DATABASE IF NOT EXISTS {SR_DB}", database=None)

    if not args.force:
        try:
            if sr_query("SHOW TABLES LIKE 'pr_data'"):
                print(f"Error: Table {SR_DB}.pr_data already exists. Use --force to drop and recreate.")
                sys.exit(1)
        except Exception:
            pass  # Database may not exist yet, proceed

    drops = "DROP TABLE IF EXISTS pr_data; DROP TABLE IF EXISTS pr_versions; DROP TABLE IF EXISTS pr_sync;" if args.force else ""
    ddl = f"""
{drops}
{_pr_data_ddl("pr_data")};
{_pr_versions_ddl("pr_versions")};
{_pr_sync_ddl()};
"""
    sr_execute_sql(ddl)
    print("  Done. Tables pr_data, pr_versions, pr_sync created (repo-aware schema).")


def _table_has_repo(table: str):
    """True/False whether `table` has a 'repo' column; None if the table is missing."""
    try:
        cols = sr_query(f"DESC {table}")
    except Exception:
        return None
    return any(c.get("Field") == "repo" for c in cols)


def cmd_migrate_repo(args):
    """One-shot migration: add repo dimension to pr_data/pr_versions, create pr_sync. Idempotent."""
    d = _table_has_repo("pr_data")
    v = _table_has_repo("pr_versions")
    if d is True and v is True:
        print("pr_data and pr_versions already migrated (have 'repo'); ensuring pr_sync schema ...")
        _ensure_pr_sync_schema()
        print("Nothing else to do.")
        return
    if d is None and v is None:
        print("Error: neither pr_data nor pr_versions exists — run `python3 pr.py init-table` first.")
        sys.exit(1)
    if d is not False or v is not False:
        # anything other than "both tables present and unmigrated" is a partial/inconsistent
        # state — most likely a previous migrate-repo failed mid-rename (see Step 4).
        print(f"Error: inconsistent migration state (pr_data repo-column={d}, "
              f"pr_versions repo-column={v}; None = table missing). A previous migrate-repo may "
              "have failed mid-rename. Inspect pr_data / pr_data_new / pr_data_bak (and the "
              "pr_versions_* tables) and finish the rename by hand before re-running.")
        sys.exit(1)
    # d is False and v is False -> both present and unmigrated -> proceed

    print("Step 1/4: creating pr_data_new / pr_versions_new ...")
    sr_execute_sql(f"""
DROP TABLE IF EXISTS pr_data_new;
DROP TABLE IF EXISTS pr_versions_new;
{_pr_data_ddl("pr_data_new")};
{_pr_versions_ddl("pr_versions_new")};
""")

    print("Step 2/4: copying data (INSERT INTO ... SELECT, repo='oss') ...")
    sr_execute_sql("""
SET query_timeout = 1800;
SET insert_timeout = 1800;
INSERT INTO pr_data_new
SELECT pr_number, 'oss', title, author, labels, created_at, merged_at,
       additions, deletions, changed_files, module, change_type, version,
       ai_summary, ai_summary_en, diff_keywords, searchable_text, body, embedding
FROM pr_data;
INSERT INTO pr_versions_new
SELECT pr_number, 'oss', version, backport_pr FROM pr_versions;
""")

    print("Step 3/4: verifying row counts ...")
    for old, new in (("pr_data", "pr_data_new"), ("pr_versions", "pr_versions_new")):
        c_old = sr_query(f"SELECT COUNT(*) AS c FROM {old}")[0]["c"]
        c_new = sr_query(f"SELECT COUNT(*) AS c FROM {new}")[0]["c"]
        print(f"  {old}: {c_old} -> {new}: {c_new}")
        if int(c_old) != int(c_new):
            print(f"Error: row count mismatch for {old}, aborting before rename. Old tables untouched.")
            sys.exit(1)

    print("Step 4/4: swapping tables (old kept as *_bak) ...")
    try:
        sr_execute_sql("""
ALTER TABLE pr_data RENAME pr_data_bak;
ALTER TABLE pr_data_new RENAME pr_data;
ALTER TABLE pr_versions RENAME pr_versions_bak;
ALTER TABLE pr_versions_new RENAME pr_versions;
""")
    except Exception as e:
        print(f"Error during table rename: {e}\n"
              "Migration is PARTIAL — some renames may have applied. Finish the swap by hand "
              "(skip any already done), then re-run migrate-repo to create pr_sync:\n"
              "  ALTER TABLE pr_data RENAME pr_data_bak;\n"
              "  ALTER TABLE pr_data_new RENAME pr_data;\n"
              "  ALTER TABLE pr_versions RENAME pr_versions_bak;\n"
              "  ALTER TABLE pr_versions_new RENAME pr_versions;")
        sys.exit(1)
    _ensure_pr_sync_schema()
    print("Done. Backup tables pr_data_bak / pr_versions_bak kept; drop them manually after validation.")


# --- Step 3: load JSON into StarRocks ---

def _collect_enriched_files(args) -> list[Path]:
    """Resolve enriched JSON files from --file, --since/--until, or --days."""
    enriched_dir = REPOS[getattr(args, "repo", "oss")]["enriched_dir"]

    if getattr(args, "file", None):
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = enriched_dir / args.file
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
        f = enriched_dir / f"pr_enriched_{d}.json"
        if f.exists():
            files.append(f)
        else:
            print(f"  Skipping {f.name} (not found)")
    if not files:
        # No enriched files is a no-op (e.g. a window of only sync/backport PRs, which are
        # never enriched): return empty so cmd_load skips loading but the pipeline continues
        # to link-backport instead of dying here.
        print(f"No enriched files found in range [{since} .. {until}].")
        return []
    return files


def _assert_pr_data_migrated():
    """Abort if pr_data lacks the repo column (pre-migration schema).
    Loading enterprise rows into a single-column-PK pr_data would upsert-overwrite
    same-numbered oss rows. Run `migrate-repo` first."""
    cols = sr_query("DESC pr_data")
    if not any(c.get("Field") == "repo" for c in cols):
        raise RuntimeError(
            "pr_data has no 'repo' column — database is pre-migration. "
            "Run `python3 pr.py migrate-repo` before loading enterprise data.")


def stream_load_json(table: str, rows: list[dict]):
    """Stream Load a list of dicts into a table (upsert via primary key)."""
    if not rows:
        print(f"  No rows to load into {table}.")
        return
    url = f"http://{SR_HOST}:{SR_HTTP_PORT}/api/{SR_DB}/{table}/_stream_load"
    payload = json.dumps(rows)
    auth = base64.b64encode(f"{SR_USER}:{SR_PASSWORD}".encode()).decode()
    cmd = [
        "curl", "-s", "-L", "--location-trusted",
        "--noproxy", "*",
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
        raise RuntimeError(f"Stream Load {table} failed (rc={result.returncode}): {result.stderr}")
    resp = json.loads(result.stdout)
    status = resp.get("Status", "Unknown")
    loaded = resp.get("NumberLoadedRows", 0)
    msg = resp.get("Message", "")
    print(f"  Stream Load {table}: Status={status}, Loaded={loaded} rows. {msg}")
    if status not in ("Success", "Publish Timeout"):
        print(f"  Full response: {json.dumps(resp, indent=2)}")
        raise RuntimeError(
            f"Stream Load {table} did not succeed (Status={status}) — aborting so the failure "
            f"is visible instead of silently dropping rows. {msg}")


def load_versions(version_rows: list):
    """Load version mappings into pr_versions via Stream Load."""
    stream_load_json("pr_versions", version_rows)


_PR_DATA_EXTRA_KEYS = ("base_ref", "pr_kind", "sync_source_pr")

def _strip_extra_keys(rows: list[dict]) -> list[dict]:
    """Drop raw-only fields that have no matching pr_data column."""
    return [{k: v for k, v in r.items() if k not in _PR_DATA_EXTRA_KEYS} for r in rows]


def cmd_load(args):
    """Load enriched JSON files into StarRocks via Stream Load."""
    files = _collect_enriched_files(args)

    total_loaded = 0
    all_versions = []
    checked_migrated = False
    for file_path in files:
        with open(file_path) as f:
            rows = json.load(f)
        for r in rows:
            r.setdefault("repo", "oss")  # old enriched files lack repo; pr_data.repo is NOT NULL
            _assert_known_repo(r["repo"], file_path.name)
            r["version"] = normalize_version(r.get("version", "main"))  # strip -ee from legacy JSON too
        if not checked_migrated and any(is_enterprise(r.get("repo", "oss")) for r in rows):
            _assert_pr_data_migrated()
            checked_migrated = True
        print(f"Loading {len(rows)} rows from {file_path.name} ...")
        load_to_starrocks(_strip_extra_keys(rows))
        total_loaded += len(rows)

        # Collect version mappings (each PR gets its own version entry)
        for row in rows:
            all_versions.append({
                "pr_number": row["pr_number"],
                "repo": row.get("repo", "oss"),
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
    repo = getattr(args, "repo", "oss")
    files = _collect_raw_files(args)

    version_rows = []
    for file_path in files:
        with open(file_path) as f:
            raw_rows = json.load(f)
        for row in raw_rows:
            kind = row.get("pr_kind")
            if kind is None:
                kind = "backport" if parse_backport(row.get("title", "")) else "exclusive"
            if kind != "backport":
                continue
            source_prs = parse_backport(row.get("title", ""))
            if not source_prs:
                continue
            version = normalize_version(row.get("version", "main"))
            if version == "main":
                # Backport PRs should land on a release branch; skip if unresolved
                continue
            row_repo = row.get("repo", repo)
            _assert_known_repo(row_repo, file_path.name)
            for src_pr in source_prs:
                version_rows.append({
                    "pr_number": src_pr,
                    "repo": row_repo,
                    "version": version,
                    "backport_pr": row["pr_number"],
                })

    if not version_rows:
        print("No backport relationships found.")
        return

    if any(is_enterprise(v["repo"]) for v in version_rows):
        _assert_pr_data_migrated()

    print(f"Found {len(version_rows)} backport version mappings, loading into pr_versions ...")
    load_versions(version_rows)
    print("Done.")


def cmd_link_sync(args):
    """Scan enterprise raw files and load (sync #N) mappings into pr_sync."""
    repo = getattr(args, "repo", "oss")
    if not is_enterprise(repo):
        print("link-sync only applies to an enterprise repo (--repo cd|ms).")
        return
    files = _collect_raw_files(args)

    sync_rows = []
    for file_path in files:
        with open(file_path) as f:
            raw_rows = json.load(f)
        for row in raw_rows:
            kind = row.get("pr_kind")
            src = row.get("sync_source_pr")
            if kind is None:
                # legacy raw file without pr_kind: derive sync directly from the title,
                # mirroring cmd_link_backport's parse_backport fallback
                srcs = parse_sync(row.get("title", ""))
                if srcs:
                    kind, src = "sync", srcs[0]
            if kind != "sync" or not src:
                continue
            er = row.get("repo", repo)
            _assert_known_repo(er, file_path.name)
            sync_rows.append({
                "oss_pr": int(src),
                "ent_pr": int(row["pr_number"]),
                "ent_repo": er,
                "version": normalize_version(row.get("version", "main")),
                "ent_merged_at": row.get("merged_at"),
            })

    if not sync_rows:
        print("No sync mappings found.")
        return
    if not sr_query("SHOW TABLES LIKE 'pr_sync'"):
        raise RuntimeError(
            "pr_sync table does not exist — run `python3 pr.py migrate-repo` first.")
    if not any(c.get("Field") == "ent_repo" for c in sr_query("DESC pr_sync")):
        raise RuntimeError(
            "pr_sync lacks the 'ent_repo' column (pre-mirrorship schema) — "
            "run `python3 pr.py migrate-repo` first to rebuild it.")
    print(f"Found {len(sync_rows)} sync mappings, loading into pr_sync ...")
    stream_load_json("pr_sync", sync_rows)
    print("Done.")


# --- Step 5: semantic search ---

def cmd_search(args):
    """Semantic search PRs using vector index."""
    query = args.query
    top_k = args.top
    repo = args.repo

    print(f"Searching: {query}" + (f"  [repo={repo}]" if repo != "all" else ""))
    print("  Generating query embedding via Ollama ...")
    query_embedding = ollama_embed(query)
    vec_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

    repo_cond = f"AND repo = '{repo}'" if repo != "all" else ""
    sql = f"""
SELECT pr_number, repo, title, author, module, change_type, version, ai_summary, ai_summary_en, merged_at,
       approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) AS score
FROM pr_data
WHERE approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) >= 0.3 {repo_cond}
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
        r_repo = r.get('repo', 'oss')
        pr_url = f"https://github.com/{REPOS.get(r_repo, REPOS['oss'])['slug']}/pull/{pr_num}"
        print(f"\n  [{i+1}] [{r_repo.upper()}] PR #{pr_num}  "
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
    stream_load_json("pr_data", rows)


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(description="StarRocks PR Analytics (Ollama)")
    sub = parser.add_subparsers(dest="command", required=True)

    # pipeline: fetch → (link-sync for enterprise) → enrich → load → link-backport
    p_pipe = sub.add_parser("pipeline", help="Run full pipeline: fetch → (link-sync for enterprise) → enrich → load → link-backport")
    p_pipe.add_argument("--repo", type=str, default="oss", choices=REPO_CHOICES, help="Repository: oss (default), cd, or ms")
    p_pipe.add_argument("--days", type=int, default=1, help="Last N days")
    p_pipe.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_pipe.add_argument("--until", type=str, help="End date, e.g. today")
    p_pipe.add_argument("--reverse", action="store_true", help="Process enrich in reverse order")

    # fetch: pull raw PR data from GitHub
    p_fetch = sub.add_parser("fetch", help="Fetch raw PR data from GitHub → save JSON")
    p_fetch.add_argument("--repo", type=str, default="oss", choices=REPO_CHOICES, help="Repository: oss (default), cd, or ms")
    p_fetch.add_argument("--days", type=int, default=1, help="Fetch PRs from last N days (ignored if --since is set)")
    p_fetch.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_fetch.add_argument("--until", type=str, help="End date, e.g. 2025-04-30")

    # enrich: AI summary + embedding
    p_enrich = sub.add_parser("enrich", help="Generate AI summaries + embeddings for raw PR JSON")
    p_enrich.add_argument("--repo", type=str, default="oss", choices=REPO_CHOICES, help="Repository: oss (default), cd, or ms")
    p_enrich.add_argument("--file", type=str, help="Raw PR JSON file path")
    p_enrich.add_argument("--days", type=int, default=1, help="Process last N days (ignored if --since or --file is set)")
    p_enrich.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_enrich.add_argument("--until", type=str, help="End date (default: today)")
    p_enrich.add_argument("--output", type=str, help="Output enriched JSON path (single file only)")
    p_enrich.add_argument("--reverse", action="store_true", help="Process files from newest to oldest")

    # init-table
    p_init = sub.add_parser("init-table", help="Create StarRocks database and tables (pr_data + pr_versions + pr_sync)")
    p_init.add_argument("--force", action="store_true", help="Drop and recreate all tables if they exist")

    # migrate-repo: one-shot schema migration to repo-aware tables
    sub.add_parser("migrate-repo", help="One-shot: add repo column to pr_data/pr_versions, create pr_sync")

    # load: import into StarRocks
    p_load = sub.add_parser("load", help="Load enriched JSON files into StarRocks")
    p_load.add_argument("--repo", type=str, default="oss", choices=REPO_CHOICES, help="Repository: oss (default), cd, or ms")
    p_load.add_argument("--file", type=str, help="Enriched JSON file path")
    p_load.add_argument("--days", type=int, default=1, help="Process last N days (ignored if --since or --file is set)")
    p_load.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_load.add_argument("--until", type=str, help="End date (default: today)")

    # link-backport: scan raw files and populate pr_versions with backport relationships
    p_link = sub.add_parser("link-backport", help="Scan raw files and load backport version mappings into pr_versions")
    p_link.add_argument("--repo", type=str, default="oss", choices=REPO_CHOICES, help="Repository: oss (default), cd, or ms")
    p_link.add_argument("--file", type=str, help="Raw PR JSON file path")
    p_link.add_argument("--days", type=int, default=1, help="Process last N days (ignored if --since or --file is set)")
    p_link.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_link.add_argument("--until", type=str, help="End date (default: today)")

    # link-sync: scan enterprise raw files and load sync mappings into pr_sync
    p_link_sync = sub.add_parser("link-sync", help="Scan enterprise raw files and load (sync #N) mappings into pr_sync")
    p_link_sync.add_argument("--repo", type=str, default="cd", choices=ENTERPRISE_REPOS, help="Enterprise repo: cd (current source, default) or ms")
    p_link_sync.add_argument("--file", type=str, help="Raw PR JSON file path")
    p_link_sync.add_argument("--days", type=int, default=1, help="Process last N days")
    p_link_sync.add_argument("--since", type=str, help="Start date")
    p_link_sync.add_argument("--until", type=str, help="End date (default: today)")

    # search
    p_search = sub.add_parser("search", help="Semantic search PRs")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--top", type=int, default=10, help="Top K results")
    p_search.add_argument("--repo", type=str, default="all", choices=REPO_CHOICES + ["all"], help="Repository filter (default: all, cross-repo)")

    args = parser.parse_args()

    if args.command == "fetch":
        cmd_fetch(args)
    elif args.command == "enrich":
        cmd_enrich(args)
    elif args.command == "init-table":
        cmd_init_table(args)
    elif args.command == "migrate-repo":
        cmd_migrate_repo(args)
    elif args.command == "load":
        cmd_load(args)
    elif args.command == "link-backport":
        cmd_link_backport(args)
    elif args.command == "link-sync":
        cmd_link_sync(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "pipeline":
        cmd_pipeline(args)


if __name__ == "__main__":
    main()
