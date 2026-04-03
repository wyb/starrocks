#!/usr/bin/env python3
"""
StarRocks PR Analytics - 拉取 PR、AI 摘要、向量 embedding、语义搜索

Prerequisites:
    brew install ollama
    ollama serve
    ollama pull bge-m3         # embedding 模型 (1024维, 多语言)
    ollama pull qwen3.5:9b     # 摘要模型

Usage:
    # Step 1: 拉取 PR 原始数据 (按天存储, 按周分批, 增量去重)
    python3 pr.py fetch --days 1
    python3 pr.py fetch --since 2025-04-01 --until 2025-04-30

    # Step 2: AI 增强 (生成摘要 + embedding, 断点续跑)
    python3 pr.py enrich --file data/raw/pr_raw_20250401.json
    python3 pr.py enrich --since 2025-04-01 --until 2025-04-30

    # Step 3: 建表 (首次, Primary Key + HNSW 向量索引)
    python3 pr.py init-table

    # Step 4: 导入 StarRocks (重复导入自动更新)
    python3 pr.py load --file data/enriched/pr_enriched_20250401.json
    python3 pr.py load --since 2025-04-01 --until 2025-04-30

    # Step 5: 语义搜索
    python3 pr.py search "内存泄漏"
    python3 pr.py search "物化视图刷新" --top 5
"""

import argparse
import base64
import http.client
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
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

REPO = "StarRocks/starrocks"
RAW_DIR = Path(__file__).parent / "data" / "raw"
ENRICHED_DIR = Path(__file__).parent / "data" / "enriched"


# --- GitHub Data Fetching ---

def _fetch_prs_batch(since: str, until: str) -> list[dict]:
    """Fetch a single batch of PRs for a date range."""
    date_range = f"merged:{since}..{until}"
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


def parse_change_type(title: str) -> str:
    m = re.match(r"\[(\w+(?:[-/]\w+)?)\]", title)
    if m:
        tag = m.group(1).lower()
        mapping = {
            "bugfix": "BugFix", "bug": "BugFix", "fix": "BugFix",
            "feature": "Feature", "feat": "Feature",
            "enhancement": "Enhancement", "improve": "Enhancement",
            "refactor": "Refactor",
            "ut": "UT", "test": "UT",
            "doc": "Doc", "docs": "Doc",
            "tool": "Tool", "build": "Tool", "ci": "Tool",
            "chore": "Chore",
        }
        return mapping.get(tag, tag.capitalize())
    return "Other"


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


def parse_dt(s: str | None) -> str | None:
    if not s:
        return None
    return s.replace("T", " ").replace("Z", "")[:19]


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
            "change_type": parse_change_type(title),
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
            json.dump(day_rows, f, ensure_ascii=False)

        if new_rows:
            print(f"\n  {out_file.name}: {len(new_rows)} new, {len(day_rows)} total")
            for r in new_rows:
                print(f"    #{r['pr_number']} [{r['change_type']}] [{r['module']}] {r['title'][:60]}")
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
    """Resolve raw JSON files from --file or --since/--until."""
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = RAW_DIR / args.file
        if not file_path.exists():
            print(f"File not found: {args.file}")
            sys.exit(1)
        return [file_path]

    if not args.since:
        print("Error: must specify --file or --since")
        sys.exit(1)

    until = args.until or datetime.now().strftime("%Y-%m-%d")
    dates = _date_range(args.since, until)
    files = []
    for d in dates:
        f = RAW_DIR / f"pr_raw_{d}.json"
        if f.exists():
            files.append(f)
        else:
            print(f"  Skipping {f.name} (not found)")
    if not files:
        print("No raw files found in the specified date range.")
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
    total = len(raw_rows)
    new_count = 0
    for i, row in enumerate(raw_rows):
        pr_num = row["pr_number"]

        # Skip already enriched
        if pr_num in existing:
            enriched_rows.append(existing[pr_num])
            continue

        title = row["title"]
        body = row.get("body") or ""
        print(f"  [{i+1}/{total}] PR #{pr_num}: {title[:60]}...")

        # AI summary (Chinese + English)
        print(f"    Summarizing ...")
        summaries = ollama_summarize(title, body)
        print(f"    zh: {summaries['zh'][:80]}...")
        print(f"    en: {summaries['en'][:80]}...")

        # Embedding (title + English summary for better semantic matching)
        print(f"    Embedding ...")
        embed_text = f"{title}\n{summaries['en']}"
        embedding = ollama_embed(embed_text)

        enriched_row = {**row,
                        "ai_summary": summaries["zh"],
                        "ai_summary_en": summaries["en"],
                        "embedding": embedding}
        enriched_rows.append(enriched_row)
        new_count += 1

        # Save progress after each PR (resume-friendly)
        with open(out_file, "w") as f:
            json.dump(enriched_rows, f, ensure_ascii=False)

    print(f"  {out_file.name}: enriched {new_count}, skipped {total - new_count}")
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
    if not args.force:
        # Check if table already exists
        try:
            result = sr_execute_sql(f"USE {SR_DB}; SHOW TABLES LIKE 'pr_data';")
            if "pr_data" in result:
                print(f"Error: Table {SR_DB}.pr_data already exists. Use --force to drop and recreate.")
                sys.exit(1)
        except Exception:
            pass  # Database may not exist yet, proceed

    drop_stmt = "DROP TABLE IF EXISTS pr_data;" if args.force else ""
    ddl = f"""
CREATE DATABASE IF NOT EXISTS {SR_DB};
USE {SR_DB};

{drop_stmt}

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
    body            STRING         COMMENT 'PR描述',
    embedding       ARRAY<FLOAT>   NOT NULL COMMENT '向量 dim={EMBEDDING_DIM}',
    INDEX vec_idx (embedding) USING VECTOR (
        "index_type" = "hnsw",
        "metric_type" = "cosine_similarity",
        "is_vector_normed" = "false",
        "M" = "16",
        "dim" = "{EMBEDDING_DIM}"
    )
) ENGINE = OLAP
PRIMARY KEY(pr_number)
DISTRIBUTED BY HASH(pr_number) BUCKETS 1
PROPERTIES("replication_num" = "1");
"""
    print("Creating database and table ...")
    sr_execute_sql(ddl)
    print("  Done. Table pr_analytics.pr_data created.")


# --- Step 3: load JSON into StarRocks ---

def _collect_enriched_files(args) -> list[Path]:
    """Resolve enriched JSON files from --file or --since/--until."""
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = ENRICHED_DIR / args.file
        if not file_path.exists():
            print(f"File not found: {args.file}")
            sys.exit(1)
        return [file_path]

    if not args.since:
        print("Error: must specify --file or --since")
        sys.exit(1)

    until = args.until or datetime.now().strftime("%Y-%m-%d")
    dates = _date_range(args.since, until)
    files = []
    for d in dates:
        f = ENRICHED_DIR / f"pr_enriched_{d}.json"
        if f.exists():
            files.append(f)
        else:
            print(f"  Skipping {f.name} (not found)")
    if not files:
        print("No enriched files found in the specified date range.")
        sys.exit(1)
    return files


def cmd_load(args):
    """Load enriched JSON files into StarRocks via Stream Load."""
    files = _collect_enriched_files(args)

    total_loaded = 0
    for file_path in files:
        with open(file_path) as f:
            rows = json.load(f)
        print(f"Loading {len(rows)} rows from {file_path.name} ...")
        load_to_starrocks(rows)
        total_loaded += len(rows)

    print(f"\nDone! Total loaded: {total_loaded} rows from {len(files)} files")


# --- Step 4: semantic search ---

def cmd_search(args):
    """Semantic search PRs using vector index."""
    query = args.query
    top_k = args.top

    print(f"Searching: {query}")
    print("  Generating query embedding via Ollama ...")
    query_embedding = ollama_embed(query)
    vec_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

    sql = f"""
USE {SR_DB};
SELECT pr_number, title, author, module, change_type, version, ai_summary, ai_summary_en, merged_at,
       approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) AS score
FROM pr_data
WHERE approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) >= 0.3
ORDER BY approx_cosine_similarity(embedding, ARRAY<FLOAT>{vec_str}) DESC
LIMIT {top_k};
"""
    print("  Querying StarRocks ...")
    result = sr_execute_sql(sql)

    # Parse TSV output from mysql CLI
    if not result.strip():
        print("\nNo results found.")
        return

    lines = result.strip().split("\n")
    if len(lines) <= 1:
        print("\nNo results found.")
        return

    headers = lines[0].split("\t")
    rows = [dict(zip(headers, line.split("\t"))) for line in lines[1:]]

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

def sr_execute_sql(sql: str) -> str:
    """Execute SQL via mysql CLI."""
    mysql_bin = os.getenv("MYSQL_BIN",
                          "/usr/local/Cellar/mysql-client@5.7/5.7.29/bin/mysql")
    cmd = [
        mysql_bin,
        f"--host={SR_HOST}",
        f"--port={SR_PORT}",
        f"--user={SR_USER}",
        "--batch", "--raw",
    ]
    if SR_PASSWORD:
        cmd.append(f"--password={SR_PASSWORD}")

    result = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"SQL Error: {result.stderr}")
        raise RuntimeError(result.stderr)
    return result.stdout


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

    # fetch: pull raw PR data from GitHub
    p_fetch = sub.add_parser("fetch", help="Fetch raw PR data from GitHub → save JSON")
    p_fetch.add_argument("--days", type=int, default=1, help="Fetch PRs from last N days (ignored if --since is set)")
    p_fetch.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_fetch.add_argument("--until", type=str, help="End date, e.g. 2025-04-30")

    # enrich: AI summary + embedding
    p_enrich = sub.add_parser("enrich", help="Generate AI summaries + embeddings for raw PR JSON")
    p_enrich.add_argument("--file", type=str, help="Raw PR JSON file path")
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
    p_load.add_argument("--since", type=str, help="Start date, e.g. 2025-04-01")
    p_load.add_argument("--until", type=str, help="End date (default: today)")

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
    elif args.command == "search":
        cmd_search(args)


if __name__ == "__main__":
    main()
