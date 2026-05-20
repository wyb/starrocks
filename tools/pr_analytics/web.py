#!/usr/bin/env python3
"""
PR Analytics Web UI - 基于 StarRocks 向量搜索的 PR 查询界面

Usage:
    python3 web.py
    python3 web.py --port 8888
    # Open http://localhost:8888
"""

import argparse
import base64
import http.client
import http.server
import json
import logging
import os
import re
import tempfile
import urllib.parse
import uuid
from pathlib import Path
import pymysql
import chat

# --- Logging Setup ---
LOG_DIR = Path(__file__).parent / "log"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "web.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8")
    ]
)
logger = logging.getLogger("web_ui")

# --- Config (reuse from pr.py) ---
SR_HOST = os.getenv("SR_HOST", "127.0.0.1")
SR_PORT = os.getenv("SR_PORT", "9030")
SR_HTTP_PORT = os.getenv("SR_HTTP_PORT", "8030")
SR_USER = os.getenv("SR_USER", "root")
SR_PASSWORD = os.getenv("SR_PASSWORD", "")
SR_DB = "pr_analytics"

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "localhost")
OLLAMA_PORT = int(os.getenv("OLLAMA_PORT", "11434"))
EMBED_MODEL = os.getenv("EMBED_MODEL", "bge-m3")

REPO = "StarRocks/starrocks"

IMG_DIR = Path(tempfile.gettempdir()) / "pr_analytics_imgs"
IMG_DIR.mkdir(exist_ok=True)
MAX_IMG_BYTES = 10 * 1024 * 1024  # 10MB


def _detect_image_ext(b: bytes) -> str | None:
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if b.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if b.startswith(b"GIF87a") or b.startswith(b"GIF89a"):
        return "gif"
    if len(b) >= 12 and b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "webp"
    return None


# --- Backend helpers ---

def ollama_embed(text: str) -> list[float]:
    conn = http.client.HTTPConnection(OLLAMA_HOST, OLLAMA_PORT, timeout=120)
    payload = json.dumps({"model": EMBED_MODEL, "input": text[:4000]})
    try:
        conn.request("POST", "/api/embed", body=payload,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        return data["embeddings"][0]
    finally:
        conn.close()


def _get_conn(database=SR_DB):
    return pymysql.connect(
        host=SR_HOST,
        port=int(SR_PORT),
        user=SR_USER,
        password=SR_PASSWORD,
        database=database,
        charset="utf8mb4",
    )


def sr_query(sql: str, database=SR_DB) -> list:
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


SUMMARY_MODEL = os.getenv("SUMMARY_MODEL", "qwen3.5:9b")

def ollama_analyze_fix(query: str, prs: list[dict]) -> str:
    """Analyze if any of the PRs fix the problem described in query."""
    if not prs:
        return "No relevant PRs found to analyze."

    context = ""
    for i, pr in enumerate(prs):
        context += f"--- Candidate {i+1} ---\n"
        context += f"PR #{pr['pr_number']}: {pr['title']}\n"
        context += f"Summary: {pr.get('ai_summary', '')}\n"
        if pr.get("ai_summary_en"):
            context += f"English Summary: {pr['ai_summary_en']}\n"
        if pr.get("diff_keywords"):
            context += f"Diff Keywords: {pr['diff_keywords']}\n"
        context += "\n"

    prompt = f"""
You are an expert software engineer analyzing GitHub Pull Requests for StarRocks.
The user is reporting a problem: "{query}"

Based on the following candidate PRs, analyze if any of them fixed this problem.
If a PR seems to fix it, explain WHY and provide the PR number.
If none of them fix it, state that clearly.

Candidates:
{context}

Response format:
1. A summary sentence: "The problem was [likely/possibly/not] fixed by PR #[number]."
2. Detailed analysis for each relevant PR.
3. If not fixed, suggest keywords for further search.

Answer in Chinese.
"""
    conn = http.client.HTTPConnection(OLLAMA_HOST, OLLAMA_PORT, timeout=300)
    payload = json.dumps({
        "model": SUMMARY_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"num_predict": 1000}
    })
    try:
        conn.request("POST", "/api/chat", body=payload,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        return data.get("message", {}).get("content", "").strip()
    finally:
        conn.close()


def search_vector(query: str, top_k: int, filters: dict) -> list[dict]:
    embedding = ollama_embed(query)
    vec_str = "[" + ",".join(str(v) for v in embedding) + "]"

    where_clauses = [
        f"approx_cosine_similarity(d.embedding, ARRAY<FLOAT>{vec_str}) >= 0.3"
    ]
    join_clause = ""
    if filters.get("pr_number"):
        where_clauses.append(f"d.pr_number = {int(filters['pr_number'])}")
    if filters.get("module"):
        where_clauses.append(f"d.module = '{filters['module']}'")
    if filters.get("change_type"):
        where_clauses.append(f"d.change_type = '{filters['change_type']}'")
    if filters.get("version"):
        join_clause = "JOIN pr_versions v ON d.pr_number = v.pr_number"
        where_clauses.append(f"v.version = '{filters['version']}'")
    if filters.get("author"):
        where_clauses.append(f"d.author = '{filters['author']}'")
    if filters.get("since"):
        where_clauses.append(f"d.merged_at >= '{filters['since']}'")
    if filters.get("until"):
        where_clauses.append(f"d.merged_at <= '{filters['until']} 23:59:59'")

    where = " AND ".join(where_clauses)

    sql = f"""
SELECT d.pr_number, d.title, d.author, d.module, d.change_type, d.version,
       d.ai_summary, d.ai_summary_en, d.diff_keywords,
       d.merged_at, d.additions, d.deletions, d.changed_files,
       approx_cosine_similarity(d.embedding, ARRAY<FLOAT>{vec_str}) AS score
FROM pr_data d
{join_clause}
WHERE {where}
ORDER BY score DESC
LIMIT {top_k};
"""
    return sr_query(sql)


def search_sql(filters: dict, top_k: int) -> list[dict]:
    def build_where_clause(mode, kw):
        clauses = ["1=1"]
        if filters.get("pr_number"):
            clauses.append(f"d.pr_number = {int(filters['pr_number'])}")
        if filters.get("module"):
            clauses.append(f"d.module = '{filters['module']}'")
        if filters.get("change_type"):
            clauses.append(f"d.change_type = '{filters['change_type']}'")
        if filters.get("author"):
            clauses.append(f"d.author = '{filters['author']}'")
        if filters.get("since"):
            clauses.append(f"d.merged_at >= '{filters['since']}'")
        if filters.get("until"):
            clauses.append(f"d.merged_at <= '{filters['until']} 23:59:59'")

        if mode == "like":
            clauses.append(f"lower(d.searchable_text) LIKE lower('%{kw}%')")
        elif mode == "all":
            clauses.append(f"d.searchable_text MATCH_ALL '{kw}'")
        elif mode == "any":
            clauses.append(f"d.searchable_text MATCH_ANY '{kw}'")

        return " AND ".join(clauses)

    def execute(where_stmt):
        join_clause = ""
        v_filter = ""
        if filters.get("version"):
            join_clause = "JOIN pr_versions v ON d.pr_number = v.pr_number"
            v_filter = f" AND v.version = '{filters['version']}'"

        sql = f"""
SELECT d.pr_number, d.title, d.author, d.module, d.change_type, d.version,
       d.ai_summary, d.ai_summary_en, d.diff_keywords,
       d.merged_at, d.additions, d.deletions, d.changed_files
FROM pr_data d
{join_clause}
WHERE {where_stmt} {v_filter}
ORDER BY d.merged_at DESC
LIMIT {top_k};
"""
        return sr_query(sql)

    kw = filters.get("keyword", "").replace("'", "''")
    mode = filters.get("match_mode", "auto")

    if not kw:
        return execute(build_where_clause("none", ""))

    # Mode: LIKE
    if mode == "like":
        return execute(build_where_clause("like", kw))

    # Mode: MATCH ALL
    if mode == "all":
        return execute(build_where_clause("all", kw))

    # Mode: MATCH ANY
    if mode == "any":
        return execute(build_where_clause("any", kw))

    # Mode: AUTO (LIKE -> ALL -> ANY)
    # 1. Try LIKE
    res = execute(build_where_clause("like", kw))
    if res: return res

    # 2. Try MATCH ALL
    res = execute(build_where_clause("all", kw))
    if res: return res

    # 3. Try MATCH ANY
    res = execute(build_where_clause("any", kw))
    if res: return res

    return []



def get_stats() -> dict:
    rows = sr_query(f"""
SELECT COUNT(*) AS total,
       COUNT(DISTINCT author) AS authors,
       MIN(merged_at) AS earliest,
       MAX(merged_at) AS latest
FROM pr_data;
""")
    if rows:
        return rows[0]
    return {}


def fetch_pr_versions(pr_numbers: list) -> dict:
    """Fetch all version mappings for given PR numbers. Returns {pr_number: [{version, backport_pr}, ...]}."""
    if not pr_numbers:
        return {}
    in_list = ",".join(str(n) for n in pr_numbers)
    rows = sr_query(f"SELECT pr_number, version, backport_pr FROM pr_versions WHERE pr_number IN ({in_list}) ORDER BY pr_number, version DESC;")
    result = {}
    for r in rows:
        pn = int(r["pr_number"])
        result.setdefault(pn, []).append({
            "version": r["version"],
            "backport_pr": int(r["backport_pr"]) if r.get("backport_pr") else None,
        })
    return result


def attach_versions(results: list) -> list:
    """Attach version list to each result row."""
    pr_numbers = [int(r["pr_number"]) for r in results]
    versions_map = fetch_pr_versions(pr_numbers)
    for r in results:
        r["versions"] = versions_map.get(int(r["pr_number"]), [])
    return results


def resolve_backport_pr(pr_number: int) -> int | None:
    """If pr_number is a backport PR, return the main PR number. Otherwise return None."""
    rows = sr_query(f"SELECT pr_number FROM pr_versions WHERE backport_pr = {pr_number} LIMIT 1;")
    if rows:
        return int(rows[0]["pr_number"])
    return None


def get_pr_detail(pr_number: int) -> dict | None:
    rows = sr_query(f"""
SELECT d.pr_number, d.title, d.author, d.module, d.change_type, d.version,
       d.ai_summary, d.ai_summary_en, d.diff_keywords, d.searchable_text,
       d.body, d.merged_at, d.additions,
       d.deletions, d.changed_files
FROM pr_data d
WHERE d.pr_number = {pr_number}
LIMIT 1;
""")
    if not rows:
        # Try resolving as a backport PR
        main_pr = resolve_backport_pr(pr_number)
        if main_pr:
            rows = sr_query(f"""
SELECT d.pr_number, d.title, d.author, d.module, d.change_type, d.version,
       d.ai_summary, d.ai_summary_en, d.diff_keywords, d.searchable_text,
       d.body, d.merged_at, d.additions,
       d.deletions, d.changed_files
FROM pr_data d
WHERE d.pr_number = {main_pr}
LIMIT 1;
""")
        if not rows:
            return None

    result = attach_versions(rows)[0]
    result["github_url"] = f"https://github.com/{REPO}/pull/{result['pr_number']}"
    return result


def get_filter_options() -> dict:
    modules = sr_query(f"SELECT DISTINCT module FROM pr_data ORDER BY module;")
    types = sr_query(f"SELECT DISTINCT change_type FROM pr_data ORDER BY change_type;")
    versions = sr_query(f"SELECT DISTINCT version FROM pr_versions ORDER BY version DESC;")
    authors = sr_query(f"SELECT DISTINCT author FROM pr_data ORDER BY author;")
    return {
        "modules": [r["module"] for r in modules],
        "change_types": [r["change_type"] for r in types],
        "versions": [r["version"] for r in versions],
        "authors": [r["author"] for r in authors],
    }


# --- HTML ---

HTML_PAGE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>StarRocks PR Analytics</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f5f7fa; color: #333; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
h1 { text-align: center; margin: 20px 0; color: #1a73e8; font-size: 24px; }
.stats { display: flex; gap: 16px; justify-content: center; margin-bottom: 24px; flex-wrap: wrap; }
.stat-card { background: #fff; padding: 12px 24px; border-radius: 8px;
             box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }
.stat-card .num { font-size: 24px; font-weight: bold; color: #1a73e8; }
.stat-card .label { font-size: 12px; color: #666; margin-top: 4px; }

.search-box { background: #fff; padding: 20px; border-radius: 12px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 24px; }
.search-row { display: flex; gap: 10px; margin-bottom: 12px; }
.search-row input[type="text"] { flex: 1; padding: 10px 16px; border: 1px solid #ddd;
                                  border-radius: 8px; font-size: 15px; outline: none; }
.search-row input[type="text"]:focus { border-color: #1a73e8; }
.search-row button { padding: 10px 24px; background: #1a73e8; color: #fff; border: none;
                     border-radius: 8px; cursor: pointer; font-size: 15px; white-space: nowrap; }
.search-row button:hover { background: #1557b0; }
.search-row button.secondary { background: #34a853; }
.search-row button.secondary:hover { background: #2d8e47; }

.filters { display: flex; gap: 10px; align-items: center; }
.filters-row { display: flex; gap: 10px; align-items: center; margin-top: 10px; }
.filters select, .filters input,
.filters-row select, .filters-row input { padding: 6px 10px; border: 1px solid #ddd;
                                          border-radius: 6px; font-size: 13px; outline: none; }
.filters label, .filters-row label { font-size: 13px; color: #666; font-weight: 500; }

.filter-container { background: #f8f9fa; padding: 16px; border-radius: 8px; border: 1px solid #eee; margin-top: 15px; position: relative; transition: all 0.3s ease; overflow: hidden; }
.filter-container.collapsed { display: none; }
.filter-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
.filter-title { font-size: 13px; font-weight: bold; color: #555; }
.reset-link { font-size: 12px; color: #1a73e8; cursor: pointer; text-decoration: none; }
.reset-link:hover { text-decoration: underline; }

.toggle-filters { font-size: 13px; color: #666; cursor: pointer; display: flex; align-items: center; gap: 4px; padding: 0 10px; }
.toggle-filters:hover { color: #1a73e8; }
.toggle-filters i { border: solid #666; border-width: 0 2px 2px 0; display: inline-block; padding: 3px; transform: rotate(45deg); transition: transform 0.3s; }
.toggle-filters.active i { transform: rotate(-135deg); }

.divider { border-top: 1px solid #e0e0e0; margin: 12px 0; }

.segmented-control { display: inline-flex; background: #eee; padding: 2px; border-radius: 6px; }
.segment { padding: 4px 12px; font-size: 12px; cursor: pointer; border-radius: 4px; color: #666; transition: all 0.2s; }
.segment:hover { color: #333; }
.segment.active { background: #fff; color: #1a73e8; font-weight: bold; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }

.tab-bar { display: flex; gap: 0; margin-bottom: 0; }
.tab { padding: 8px 20px; cursor: pointer; border: 1px solid #ddd; border-bottom: none;
       background: #f0f0f0; border-radius: 8px 8px 0 0; font-size: 14px; }
.tab.active { background: #fff; font-weight: bold; }

.results { margin-top: 0; }
.result-card { background: #fff; padding: 16px 20px; border-radius: 0 8px 8px 8px;
               box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 12px; }
.result-header { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; flex-wrap: wrap; }
.pr-number { font-weight: bold; color: #1a73e8; text-decoration: none; font-size: 15px; }
.pr-number:hover { text-decoration: underline; }
.pr-title { font-size: 15px; font-weight: 500; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
         font-size: 11px; font-weight: 500; }
.badge-module { background: #e8f0fe; color: #1a73e8; }
.badge-type { background: #fce8e6; color: #c5221f; }
.badge-version { background: #e6f4ea; color: #137333; }
.badge-score { background: #fef7e0; color: #b06000; }
.meta { font-size: 13px; color: #666; margin-bottom: 6px; }
.summary { font-size: 14px; color: #444; line-height: 1.5; }
.details-row { margin-top: 8px; }
.details-actions { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
.detail-toggle { color: #666; font-size: 12px; font-weight: 500; }
.detail-toggle summary { cursor: pointer; outline: none; }
.detail-toggle summary:hover { text-decoration: underline; }
.detail-content { margin-top: 6px; }
.detail-content.hidden { display: none; }
.english-detail { font-size: 14px; color: #888; line-height: 1.5; }
.keywords-detail table { width: 100%; border-collapse: collapse; background: #fff;
                         border-top: 1px solid #f1f3f4; border-bottom: 1px solid #f1f3f4;
                         border-left: 3px solid #dadce0; font-size: 14px; }
.keywords-detail th, .keywords-detail td { border-bottom: 1px solid #f1f3f4; padding: 6px 8px;
                                           vertical-align: top; line-height: 1.45; text-align: left; }
.keywords-detail th { width: 92px; color: #888; font-weight: 600; white-space: nowrap; }
.keywords-detail td { color: #888; white-space: pre-wrap; overflow-wrap: anywhere; }
.keywords-detail pre { font-size: 14px; line-height: 1.45; white-space: pre-wrap; background: #fff;
                       border-left: 3px solid #dadce0; padding: 8px 10px;
                       border-radius: 4px; font-family: inherit; color: #888; }
.score-bar-bg { display: inline-block; width: 60px; height: 6px; background: #e8eaed; border-radius: 3px; vertical-align: middle; position: relative; }
.score-bar { display: block; height: 6px; background: #1a73e8; border-radius: 3px; position: absolute; top: 0; left: 0; }

.loading { text-align: center; padding: 40px; color: #666; }
.empty { text-align: center; padding: 40px; color: #999; }
.error { text-align: center; padding: 20px; color: #c5221f; background: #fce8e6;
         border-radius: 8px; margin: 12px 0; }
.analysis-box { background: #fff8e1; border-left: 4px solid #ffc107; padding: 16px; border-radius: 8px; margin-bottom: 20px; font-size: 14px; line-height: 1.6; white-space: pre-wrap; }
.analysis-title { font-weight: bold; margin-bottom: 8px; color: #b06000; display: flex; align-items: center; gap: 8px; }

.search-row button.ai-btn { padding: 10px 16px; background: #0288d1; color: #fff;
          border: none; border-radius: 8px; cursor: pointer; font-size: 14px; white-space: nowrap; }
.search-row button.ai-btn:hover { background: #0277bd; }

.ai-drawer { position: fixed; right: 0; top: 0; bottom: 0; width: 620px; max-width: 85vw;
             background: #fff; box-shadow: -4px 0 24px rgba(0,0,0,0.15);
             transform: translateX(100%); transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1);
             display: flex; flex-direction: column; z-index: 1000; }
.ai-drawer.open { transform: translateX(0); }
.ai-drawer.maximized { width: 100vw; max-width: 100vw; }
.ai-header { padding: 14px 20px; background: #1a73e8; color: #fff;
             display: flex; justify-content: space-between; align-items: center;
             box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.ai-header span:first-child { font-weight: 600; letter-spacing: 0.3px; }
.ai-actions { display: flex; gap: 8px; align-items: center; }
.ai-header-btn { background: rgba(255,255,255,0.15); color: #fff; border: 1px solid rgba(255,255,255,0.3);
                 border-radius: 6px; padding: 5px 12px; font-size: 12px;
                 cursor: pointer; line-height: 1.4; transition: all 0.2s; }
.ai-header-btn:hover { background: rgba(255,255,255,0.25); border-color: rgba(255,255,255,0.5); }
.ai-header-close { padding: 2px 10px; font-size: 16px; line-height: 1; }
.ai-messages { flex: 1; overflow-y: auto; padding: 20px; background: #f8f9fa;
               display: flex; flex-direction: column; gap: 16px; scroll-behavior: smooth; }
.ai-msg { max-width: 92%; padding: 12px 16px; border-radius: 12px;
          font-size: 14px; line-height: 1.6; position: relative;
          box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
.ai-msg.user { align-self: flex-start; background: #1a73e8; color: #fff;
               border-bottom-left-radius: 2px; }
.ai-msg.assistant { align-self: flex-start; background: #fff; color: #333;
                    border-bottom-left-radius: 2px; border: 1px solid #e0e0e0;
                    width: 100%; max-width: 100%; }
.ai-msg.tool { align-self: center; width: 100%; background: #202124; color: #e8eaed;
               font-family: 'Roboto Mono', monospace; font-size: 12px; border-radius: 6px;
               padding: 8px 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
.ai-msg.tool summary { cursor: pointer; color: #8ab4f8; font-weight: 500; }
.ai-msg.tool pre { margin-top: 8px; color: #bdc1c6; overflow-x: auto; white-space: pre-wrap; }
.ai-msg.tool-output { align-self: center; width: 100%; background: #f1f3f4;
                      border-left: 4px solid #34a853; color: #3c4043; border-radius: 4px;
                      font-family: 'Roboto Mono', monospace; font-size: 11px; padding: 8px 12px; }
.ai-msg.error { align-self: center; background: #fce8e6; color: #c5221f; border: 1px solid #fad2cf; }
.ai-msg.thinking { align-self: flex-start; background: transparent; box-shadow: none; color: #5f6368;
                   font-style: italic; display: flex; align-items: center; gap: 10px; padding: 4px 8px; }
.ai-msg.thinking .pulse { width: 8px; height: 8px; border-radius: 50%;
                            background: #1a73e8; animation: ai-pulse 1.2s ease-in-out infinite; }
@keyframes ai-pulse { 0%, 100% { opacity: 0.3; transform: scale(0.8); }
                       50% { opacity: 1; transform: scale(1.2); } }

.ai-input { border-top: 1px solid #e0e0e0; padding: 16px; display: flex; flex-direction: column;
            gap: 12px; background: #fff; }
.ai-input-row { display: flex; gap: 12px; align-items: flex-end; }
.ai-input textarea { flex: 1; resize: none; padding: 12px 14px;
                     border: 1px solid #dadce0; border-radius: 10px;
                     font-size: 14px; line-height: 1.5; font-family: inherit; outline: none;
                     min-height: 44px; max-height: 200px; transition: border-color 0.2s; }
.ai-input textarea:focus { border-color: #1a73e8; box-shadow: 0 0 0 2px rgba(26,115,232,0.1); }
.ai-input button#ai_send { height: 44px; padding: 0 24px; background: #1a73e8; color: #fff;
                   border: none; border-radius: 10px; cursor: pointer; font-weight: 500;
                   transition: background 0.2s; }
.ai-input button#ai_send:hover { background: #1557b0; }
.ai-input button#ai_send:disabled { background: #e8eaed; color: #9aa0a6; cursor: not-allowed; }
.ai-input button#ai_send.stop { background: #d93025; }
.ai-input button#ai_send.stop:hover { background: #b8261b; }

.ai-textarea-wrap { flex: 1; position: relative; display: flex; flex-direction: column; }
.ai-textarea-wrap textarea { padding-left: 12px; }
.ai-input button.attach { position: absolute; right: 8px; top: 8px;
                          width: 28px; height: 28px; padding: 0; line-height: 1;
                          background: transparent; color: #5f6368; border: none;
                          border-radius: 50%; font-size: 18px;
                          display: flex; align-items: center; justify-content: center; cursor: pointer; }
.ai-input button.attach:hover { background: #f1f3f4; color: #1a73e8; }

.ai-thumbs { display: none; flex-wrap: wrap; gap: 8px; margin-bottom: 4px; }
.ai-thumbs.show { display: flex; }
.ai-thumb { position: relative; width: 64px; height: 64px; border: 1px solid #dadce0;
            border-radius: 8px; overflow: hidden; background: #f8f9fa; }
.ai-thumb img { width: 100%; height: 100%; object-fit: cover; display: block; }
.ai-thumb.uploading::after { content: ''; position: absolute; inset: 0;
                              background: rgba(255,255,255,0.7); display: flex;
                              align-items: center; justify-content: center; }
.ai-thumb .x { position: absolute; top: -2px; right: -2px; width: 20px; height: 20px;
               border-radius: 50%; background: #5f6368; color: #fff; font-size: 14px;
               cursor: pointer; display: flex; align-items: center; justify-content: center;
               border: 2px solid #fff; padding: 0; box-shadow: 0 1px 3px rgba(0,0,0,0.2); }
.ai-thumb .x:hover { background: #d93025; }

.ai-copy-btn { position: absolute; top: 10px; right: 10px; padding: 4px 8px;
               background: #fff; border: 1px solid #dadce0;
               border-radius: 6px; font-size: 11px; color: #5f6368; cursor: pointer;
               opacity: 0; transition: all 0.2s; z-index: 10; }
.ai-msg.assistant:hover .ai-copy-btn { opacity: 1; }
.ai-copy-btn:hover { background: #f8f9fa; border-color: #1a73e8; color: #1a73e8; }
.ai-copy-btn.copied { color: #34a853; border-color: #34a853; background: #e6f4ea; }

/* Markdown Styling inside Chat */
.ai-content p { margin: 0 0 12px 0; }
.ai-content p:last-child { margin-bottom: 0; }
.ai-content ul, .ai-content ol { margin: 0 0 12px 24px; }
.ai-content li { margin-bottom: 4px; }
.ai-content pre { background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 8px;
                  padding: 12px; margin: 12px 0; overflow-x: auto; position: relative; }
.ai-content code { font-family: 'Roboto Mono', monospace; font-size: 13px;
                    background: #f1f3f4; padding: 2px 4px; border-radius: 4px; color: #d93025; }
.ai-content pre code { background: transparent; padding: 0; color: inherit; display: block; }
.ai-content table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }
.ai-content th, .ai-content td { border: 1px solid #dadce0; padding: 8px 12px; text-align: left; }
.ai-content th { background: #f8f9fa; font-weight: 600; }
.ai-content blockquote { border-left: 4px solid #1a73e8; background: #f4f8ff;
                          margin: 12px 0; padding: 8px 16px; color: #3c4043; border-radius: 0 4px 4px 0; }
.ai-content h1, .ai-content h2, .ai-content h3 { margin: 20px 0 12px 0; font-weight: 600; line-height: 1.3; }
.ai-content h1 { font-size: 1.4em; border-bottom: 1px solid #eee; padding-bottom: 8px; }
.ai-content h2 { font-size: 1.25em; }
.ai-content h3 { font-size: 1.1em; }
.ai-content hr { border: none; border-top: 1px solid #eee; margin: 16px 0; }
.ai-content a { color: #1a73e8; text-decoration: none; }
.ai-content a:hover { text-decoration: underline; }

</style>
</head>
<body>
<div class="container">
    <h1>StarRocks PR Analytics</h1>
    <div class="stats" id="stats"></div>

    <div class="search-box">
        <div class="search-row">
            <input type="text" id="query" placeholder="搜索描述或关键词...">
            <button onclick="doSearch()">语义搜索</button>
            <button class="secondary" onclick="doFilter()">关键词过滤</button>
            <button class="ai-btn" onclick="openAiDrawer()">✨ AI 分析</button>
            <div class="toggle-filters" id="toggle_btn" onclick="toggleFilters()"><i></i></div>
        </div>

        <div class="filter-container collapsed" id="filter_container">
            <div class="filter-header">
                <span class="filter-title">筛选</span>
                <a class="reset-link" onclick="resetFilters()">重置条件</a>
            </div>

            <div class="filters">
                <label>PR:</label>
                <input type="text" id="f_pr_number" placeholder="66666" style="width:80px;">
                <label>Module:</label>
                <select id="f_module"><option value="">全部</option></select>
                <label>Type:</label>
                <select id="f_type"><option value="">全部</option></select>
                <label>Version:</label>
                <select id="f_version"><option value="">全部</option></select>
                <label>Author:</label>
                <select id="f_author"><option value="">全部</option></select>
            </div>

            <div class="filters-row">
                <label>Since:</label>
                <input type="date" id="f_since">
                <label>Until:</label>
                <input type="date" id="f_until">
                <label>Top:</label>
                <select id="f_top">
                    <option value="10">10</option>
                    <option value="20" selected>20</option>
                    <option value="50">50</option>
                    <option value="100">100</option>
                    <option value="99999">不限</option>
                </select>
            </div>

            <div class="divider"></div>
            <div class="filter-title" style="margin-bottom:10px;">关键字过滤</div>

            <div class="filters-row">
                <label>模式:</label>
                <div class="segmented-control" id="match_mode_control">
                    <div class="segment active" data-value="auto" onclick="setMatchMode('auto')">自动</div>
                    <div class="segment" data-value="like" onclick="setMatchMode('like')">LIKE</div>
                    <div class="segment" data-value="all" onclick="setMatchMode('all')">MATCH ALL</div>
                    <div class="segment" data-value="any" onclick="setMatchMode('any')">MATCH ANY</div>
                </div>
                <input type="hidden" id="f_match_mode" value="auto">
            </div>
        </div>
    </div>

    <div class="results" id="results">
        <div class="empty">输入查询开始搜索</div>
    </div>
</div>

<div id="ai_drawer" class="ai-drawer">
  <div class="ai-header">
    <span>AI 分析 (pr-fix-finder)</span>
    <span class="ai-actions">
      <button class="ai-header-btn" onclick="resetAiSession()">+ 新对话</button>
      <button id="ai_max_btn" class="ai-header-btn ai-header-close" onclick="toggleAiMaximize()" title="最大化">⛶</button>
      <button class="ai-header-btn ai-header-close" onclick="closeAiDrawer()" title="最小化">−</button>
    </span>
  </div>
  <div id="ai_messages" class="ai-messages"></div>
  <div class="ai-input">
    <div id="ai_thumbs" class="ai-thumbs"></div>
    <div class="ai-input-row">
      <div class="ai-textarea-wrap">
        <textarea id="ai_input" rows="3" placeholder="描述问题或追问，支持粘贴/拖拽图片..."></textarea>
        <button class="attach" onclick="aiPickFiles()" title="附加图片">📎</button>
      </div>
      <button id="ai_send" onclick="aiPrimaryClick()">发送</button>
    </div>
    <input type="file" id="ai_file_input" accept="image/*" multiple style="display:none">
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<script>
const REPO = '__REPO__';
function aiRenderMarkdown(text) {
    if (typeof marked === 'undefined') return escHtml(text || '');
    try {
        return marked.parse(text || '');
    } catch (e) {
        console.error('Markdown error:', e);
        return escHtml(text || '');
    }
}


async function api(path, params) {
    const qs = new URLSearchParams(params).toString();
    const resp = await fetch('/api/' + path + '?' + qs);
    const data = await resp.json();
    if (!resp.ok) {
        throw new Error(data.error || resp.statusText);
    }
    return data;
}

function toggleFilters() {
    const container = document.getElementById('filter_container');
    const btn = document.getElementById('toggle_btn');
    const isCollapsed = container.classList.toggle('collapsed');
    btn.classList.toggle('active', !isCollapsed);
    btn.innerHTML = '<i></i>';
}

function setMatchMode(val) {
    document.getElementById('f_match_mode').value = val;
    document.querySelectorAll('#match_mode_control .segment').forEach(el => {
        el.classList.toggle('active', el.dataset.value === val);
    });
}

function resetFilters() {
    ['f_pr_number', 'f_since', 'f_until'].forEach(id => document.getElementById(id).value = '');
    ['f_module', 'f_type', 'f_version', 'f_author'].forEach(id => document.getElementById(id).selectedIndex = 0);
    document.getElementById('f_top').value = '20';
    setMatchMode('auto');
}

function getFilters() {
    return {
        "pr_number": document.getElementById('f_pr_number').value.trim(),
        module: document.getElementById('f_module').value,
        change_type: document.getElementById('f_type').value,
        version: document.getElementById('f_version').value,
        author: document.getElementById('f_author').value,
        since: document.getElementById('f_since').value,
        until: document.getElementById('f_until').value,
        top: document.getElementById('f_top').value,
        match_mode: document.getElementById('f_match_mode').value,
    };
}

async function doSearch() {
    const query = document.getElementById('query').value.trim();
    if (!query) return;
    const el = document.getElementById('results');
    el.innerHTML = '<div class="loading">搜索中... (生成 embedding + 查询 StarRocks)</div>';
    try {
        const filters = getFilters();
        const data = await api('search', { query, ...filters });
        renderResults(data.results, true);
    } catch (e) {
        el.innerHTML = '<div class="error">搜索失败: ' + e.message + '</div>';
    }
}

async function doFilter() {
    const el = document.getElementById('results');
    el.innerHTML = '<div class="loading">查询中...</div>';
    try {
        const filters = getFilters();
        filters.keyword = document.getElementById('query').value.trim();
        const data = await api('filter', filters);
        renderResults(data.results, false);
    } catch (e) {
        el.innerHTML = '<div class="error">查询失败: ' + e.message + '</div>';
    }
}

async function doAnalyze() {
    const query = document.getElementById('query').value.trim();
    if (!query) return;
    const el = document.getElementById('results');
    el.innerHTML = '<div class="loading">AI 分析中... (搜索 PR + LLM 推理)</div>';
    try {
        const filters = getFilters();
        const data = await api('analyze', { query, ...filters });
        if (!data.analysis) {
            el.innerHTML = '<div class="error">AI 未返回分析结果</div>';
            return;
        }
        let html = `
            <div class="analysis-box">
                <div class="analysis-title">✨ AI 修复分析</div>
                <div>${escHtml(data.analysis).replace(/\\n/g, '<br>')}</div>
            </div>
            <div style="margin-bottom:10px; font-weight:bold; color:#666;">分析依据的相关 PR:</div>
        `;
        el.innerHTML = html;
        const resultsEl = document.createElement('div');
        el.appendChild(resultsEl);
        renderResultsIn(data.results, true, resultsEl);
    } catch (e) {
        el.innerHTML = '<div class="error">分析失败: ' + e.message + '</div>';
    }
}

function renderResults(results, showScore) {
    const el = document.getElementById('results');
    el.innerHTML = ''; 
    renderResultsIn(results, showScore, el);
}

function renderResultsIn(results, showScore, container) {
    if (!results || results.length === 0) {
        container.innerHTML = '<div class="empty">没有找到匹配的 PR</div>';
        return;
    }
    let html = `<div style="padding:8px 0;color:#666;font-size:14px;">共 ${results.length} 条结果</div>`;
    results.forEach((r, i) => {
        const prUrl = 'https://github.com/' + REPO + '/pull/' + r.pr_number;
        const scoreHtml = showScore && r.score
            ? `<span class="badge badge-score">score: ${parseFloat(r.score).toFixed(4)}</span>
               <span class="score-bar-bg"><span class="score-bar" style="width:${Math.max(parseFloat(r.score)*60, 4)}px"></span></span>`
            : '';
        html += `
        <div class="result-card">
            <div class="result-header">
                <span style="color:#999;font-size:13px;">${i+1}.</span>
                <a class="pr-number" href="${prUrl}" target="_blank">#${r.pr_number}</a>
                <span class="pr-title">${escHtml(r.title)}</span>
            </div>
            <div class="result-header">
                <span class="badge badge-type">${r.change_type || ''}</span>
                <span class="badge badge-module">${r.module || ''}</span>
                ${(r.versions || []).map(v => {
                    const prLink = v.backport_pr || r.pr_number;
                    const url = 'https://github.com/' + REPO + '/pull/' + prLink;
                    return '<a href="' + url + '" target="_blank" class="badge badge-version" style="text-decoration:none;">' + escHtml(v.version) + '</a>';
                }).join('')}
                ${scoreHtml}
            </div>
            <div class="meta">
                Author: ${r.author || ''}
                &nbsp;|&nbsp; Merged: ${r.merged_at || ''}
                &nbsp;|&nbsp; +${r.additions || 0} -${r.deletions || 0}
                &nbsp;|&nbsp; ${r.changed_files || 0} files
            </div>
            <div class="summary">${escHtml(r.ai_summary || '')}</div>
            ${(r.ai_summary_en || r.diff_keywords) ? renderDetailPanel(r, i) : ''}
        </div>`;
    });
    container.innerHTML += html;
}

function renderDetailPanel(r, i) {
    const prefix = 'detail-' + r.pr_number + '-' + i;
    let actions = '';
    let bodies = '';
    if (r.ai_summary_en) {
        const id = prefix + '-en';
        actions += '<details class="detail-toggle" data-target="' + id + '" ontoggle="toggleDetailFromSummary(this)"><summary>english summary</summary></details>';
        bodies += '<div id="' + id + '" class="detail-content english-detail hidden">' + escHtml(r.ai_summary_en) + '</div>';
    }
    if (r.diff_keywords) {
        const id = prefix + '-kw';
        actions += '<details class="detail-toggle" data-target="' + id + '" ontoggle="toggleDetailFromSummary(this)"><summary>diff keywords</summary></details>';
        bodies += '<div id="' + id + '" class="detail-content keywords-detail hidden">' + renderDiffKeywordsBody(r.diff_keywords) + '</div>';
    }
    return '<div class="details-row"><div class="details-actions">' + actions + '</div>' + bodies + '</div>';
}

function toggleDetailFromSummary(summaryEl) {
    const el = document.getElementById(summaryEl.dataset.target);
    if (el) el.classList.toggle('hidden');
}

function renderDiffKeywordsBody(text) {
    const rows = [];
    (text || '').split(/\\n+/).forEach(line => {
        const m = line.match(/^([A-Za-z_ -]+):\\s*(.*)$/);
        if (m) rows.push({ key: m[1].trim(), value: m[2].trim() });
    });
    if (rows.length === 0) {
        return '<pre>' + escHtml(text) + '</pre>';
    }
    const order = ['symptom', 'cause', 'fix', 'symbols', 'files', 'keywords'];
    rows.sort((a, b) => {
        const ai = order.indexOf(a.key.toLowerCase());
        const bi = order.indexOf(b.key.toLowerCase());
        const av = ai === -1 ? order.length : ai;
        const bv = bi === -1 ? order.length : bi;
        return av - bv || a.key.localeCompare(b.key);
    });
    const table = rows.map(r =>
        '<tr><th>' + escHtml(r.key) + '</th><td>' + escHtml(r.value) + '</td></tr>'
    ).join('');
    return '<table>' + table + '</table>';
}

function escHtml(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

let aiSessionId = null;
let aiCurrentAssistantEl = null;
let aiThinkingEl = null;
let aiThinkingTimer = null;
let aiSendStartedAt = 0;
let aiCurrentEs = null;
let aiSending = false;
const aiImages = [];

function aiPickFiles() {
    document.getElementById('ai_file_input').click();
}

async function aiAddImage(file) {
    if (!file || !file.type || !file.type.startsWith('image/')) return;
    const url = URL.createObjectURL(file);
    const entry = { id: null, url, name: file.name || 'image', uploading: true };
    aiImages.push(entry);
    aiRenderThumbs();
    try {
        const resp = await fetch('/api/ai/upload', {
            method: 'POST',
            headers: { 'Content-Type': file.type || 'application/octet-stream' },
            body: file,
        });
        let j = null;
        try { j = await resp.json(); } catch {}
        if (resp.ok && j && j.id) {
            entry.id = j.id;
            entry.uploading = false;
        } else {
            const msg = (j && j.error) || ('HTTP ' + resp.status);
            console.warn('upload failed:', msg);
            aiAppendMsg('error', '图片上传失败: ' + msg);
            const idx = aiImages.indexOf(entry);
            if (idx >= 0) aiImages.splice(idx, 1);
        }
    } catch (e) {
        console.warn('upload error:', e);
        aiAppendMsg('error', '图片上传异常: ' + (e && e.message || e));
        const idx = aiImages.indexOf(entry);
        if (idx >= 0) aiImages.splice(idx, 1);
    }
    aiRenderThumbs();
}

function aiRemoveImage(idx) {
    const it = aiImages[idx];
    if (it && it.url) { try { URL.revokeObjectURL(it.url); } catch {} }
    aiImages.splice(idx, 1);
    aiRenderThumbs();
}

function aiClearImages() {
    for (const it of aiImages) { if (it.url) { try { URL.revokeObjectURL(it.url); } catch {} } }
    aiImages.length = 0;
    aiRenderThumbs();
}

function aiRenderThumbs() {
    const c = document.getElementById('ai_thumbs');
    if (!c) return;
    c.classList.toggle('show', aiImages.length > 0);
    c.innerHTML = aiImages.map((m, i) =>
        '<div class="ai-thumb' + (m.uploading ? ' uploading' : '') + '" title="' + escHtml(m.name) + '">' +
        '<img src="' + m.url + '" alt="">' +
        '<button class="x" onclick="aiRemoveImage(' + i + ')">×</button>' +
        '</div>'
    ).join('');
}

function openAiDrawer() {
    document.getElementById('ai_drawer').classList.add('open');
    const q = document.getElementById('query').value.trim();
    const input = document.getElementById('ai_input');
    if (q && !input.value) input.value = q;
    input.dispatchEvent(new Event('input'));
    input.focus();
}

function closeAiDrawer() {
    document.getElementById('ai_drawer').classList.remove('open');
}

function toggleAiMaximize() {
    const drawer = document.getElementById('ai_drawer');
    const btn = document.getElementById('ai_max_btn');
    const maxed = drawer.classList.toggle('maximized');
    if (btn) {
        btn.textContent = maxed ? '⧉' : '⛶';
        btn.title = maxed ? '还原' : '最大化';
    }
}

function resetAiSession() {
    aiSessionId = null;
    aiCurrentAssistantEl = null;
    document.getElementById('ai_messages').innerHTML = '';
    aiClearImages();
}

function aiCopyMsg(btn) {
    const raw = btn.parentElement && btn.parentElement.dataset.raw;
    if (!raw) return;
    const restore = () => { btn.classList.remove('copied'); btn.textContent = '复制'; };
    const ok = () => { btn.classList.add('copied'); btn.textContent = '已复制'; setTimeout(restore, 1500); };
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(raw).then(ok).catch(() => {});
    } else {
        const ta = document.createElement('textarea');
        ta.value = raw; document.body.appendChild(ta); ta.select();
        try { document.execCommand('copy'); ok(); } catch {}
        document.body.removeChild(ta);
    }
}

function aiAppendMsg(role, text) {
    const div = document.createElement('div');
    div.className = 'ai-msg ' + role;
    if (role === 'thinking') {
        div.innerHTML = '<div class="pulse"></div><span class="thinking-text"></span>';
    } else {
        div.textContent = text;
    }
    const list = document.getElementById('ai_messages');
    list.appendChild(div);
    list.scrollTop = list.scrollHeight;
    return div;
}

function aiAppendToolMsg(ev) {
    const div = document.createElement('div');
    div.className = 'ai-msg tool';
    const argsStr = typeof ev.args === 'string' ? ev.args : JSON.stringify(ev.args, null, 2);
    div.innerHTML = '<details open><summary>🔧 ' + escHtml(ev.name || 'tool') + '</summary><pre>' + escHtml(argsStr || '') + '</pre></details>';
    const list = document.getElementById('ai_messages');
    list.appendChild(div);
    list.scrollTop = list.scrollHeight;
}

function aiSetSending(sending) {
    aiSending = sending;
    const btn = document.getElementById('ai_send');
    btn.disabled = false;
    btn.textContent = sending ? '停止' : '发送';
    btn.classList.toggle('stop', sending);
}

function aiPrimaryClick() {
    if (aiSending) aiStop(); else sendAi();
}

function aiStop() {
    if (aiCurrentEs) { try { aiCurrentEs.close(); } catch {} aiCurrentEs = null; }
    aiClearThinking();
    aiAppendMsg('error', '已停止');
    aiSetSending(false);
}

function aiClearThinking() {
    if (aiThinkingTimer) { clearInterval(aiThinkingTimer); aiThinkingTimer = null; }
    if (aiThinkingEl) { aiThinkingEl.remove(); aiThinkingEl = null; }
}

function aiSetThinking(text) {
    if (!aiThinkingEl) aiThinkingEl = aiAppendMsg('thinking', text);
    aiThinkingEl.dataset.base = text;
    aiRenderThinking();
    if (!aiThinkingTimer) aiThinkingTimer = setInterval(aiRenderThinking, 1000);
}

function aiRenderThinking() {
    if (!aiThinkingEl) return;
    const elapsed = Math.floor((Date.now() - aiSendStartedAt) / 1000);
    const txt = aiThinkingEl.querySelector('.thinking-text');
    if (txt) txt.textContent = (aiThinkingEl.dataset.base || '') + ' (' + elapsed + 's)';
}

function sendAi() {
    const input = document.getElementById('ai_input');
    let prompt = input.value.trim();
    const hasReadyImg = aiImages.some(m => m.id);
    if (!prompt && !hasReadyImg) return;
    if (aiImages.some(m => m.uploading)) {
        aiAppendMsg('error', '图片还在上传中，请稍候');
        return;
    }
    if (!prompt) prompt = '请看图分析';
    const imgIds = aiImages.filter(m => m.id).map(m => m.id).join(',');
    input.value = '';
    input.dispatchEvent(new Event('input'));
    aiAppendMsg('user', prompt + (imgIds ? ' [+' + aiImages.length + ' 图片]' : ''));
    aiClearImages();
    aiCurrentAssistantEl = null;
    aiClearThinking();
    aiSendStartedAt = Date.now();
    aiSetThinking('AI 正在思考...');
    aiSetSending(true);

    const imgQs = imgIds ? '&images=' + encodeURIComponent(imgIds) : '';
    const url = aiSessionId
        ? '/api/ai/chat?session=' + encodeURIComponent(aiSessionId) + '&prompt=' + encodeURIComponent(prompt) + imgQs
        : '/api/ai/start?prompt=' + encodeURIComponent(prompt) + imgQs;

    const es = new EventSource(url);
    aiCurrentEs = es;

    es.onmessage = e => {
        let ev;
        try { ev = JSON.parse(e.data); } catch { return; }
        switch (ev.type) {
            case 'session':
                aiSessionId = ev.session_id;
                break;
            case 'message': {
                aiClearThinking();
                const list = document.getElementById('ai_messages');
                const isAtBottom = list.scrollHeight - list.scrollTop <= list.clientHeight + 100;

                if (!aiCurrentAssistantEl) {
                    aiCurrentAssistantEl = aiAppendMsg('assistant', '');
                    aiCurrentAssistantEl.dataset.raw = '';
                    aiCurrentAssistantEl.innerHTML = '<button class="ai-copy-btn" onclick="aiCopyMsg(this)">复制</button><div class="ai-content"></div>';
                }
                aiCurrentAssistantEl.dataset.raw = (aiCurrentAssistantEl.dataset.raw || '') + (ev.text || '');
                const content = aiCurrentAssistantEl.querySelector('.ai-content');
                content.innerHTML = aiRenderMarkdown(aiCurrentAssistantEl.dataset.raw);
                
                if (isAtBottom) list.scrollTop = list.scrollHeight;
                break;
            }
            case 'tool':
                if (aiThinkingEl) aiSetThinking('正在调用工具...');
                aiAppendToolMsg(ev);
                aiCurrentAssistantEl = null;
                break;
            case 'tool_output':
                aiClearThinking();
                aiAppendMsg('tool-output', '↳ ' + (ev.text || '').slice(0, 500));
                aiCurrentAssistantEl = null;
                aiSetThinking('处理工具输出...');
                break;
            case 'error':
                aiClearThinking();
                aiAppendMsg('error', ev.text || 'unknown error');
                break;
            case 'done':
                aiClearThinking();
                es.close();
                aiCurrentEs = null;
                aiSetSending(false);
                break;
        }
    };
    es.onerror = () => {
        if (aiCurrentEs !== es) return;
        aiClearThinking();
        aiAppendMsg('error', '连接中断');
        es.close();
        aiCurrentEs = null;
        aiSetSending(false);
    };
}

document.addEventListener('DOMContentLoaded', () => {
    const inp = document.getElementById('ai_input');
    if (inp) {
        inp.addEventListener('keydown', e => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
                e.preventDefault();
                sendAi();
            }
        });
        const autoResize = () => {
            inp.style.height = 'auto';
            inp.style.height = Math.min(inp.scrollHeight, 240) + 'px';
        };
        inp.addEventListener('input', autoResize);
        autoResize();
        inp.addEventListener('paste', e => {
            if (!e.clipboardData) return;
            let used = false;
            for (const it of e.clipboardData.items) {
                if (it.kind === 'file' && it.type && it.type.startsWith('image/')) {
                    const f = it.getAsFile();
                    if (f) { aiAddImage(f); used = true; }
                }
            }
            if (used) e.preventDefault();
        });
    }
    const fi = document.getElementById('ai_file_input');
    if (fi) {
        fi.addEventListener('change', e => {
            for (const f of e.target.files) aiAddImage(f);
            e.target.value = '';
        });
    }
    const drawer = document.getElementById('ai_drawer');
    if (drawer) {
        drawer.addEventListener('dragover', e => {
            if (e.dataTransfer && Array.from(e.dataTransfer.types || []).includes('Files')) {
                e.preventDefault();
            }
        });
        drawer.addEventListener('drop', e => {
            if (!e.dataTransfer || !e.dataTransfer.files.length) return;
            e.preventDefault();
            for (const f of e.dataTransfer.files) {
                if (f.type && f.type.startsWith('image/')) aiAddImage(f);
            }
        });
    }
});

// Init: load stats and filter options
async function init() {
    try {
        const [stats, options] = await Promise.all([api('stats', {}), api('options', {})]);
        const se = document.getElementById('stats');
        if (stats) {
            se.innerHTML = `
                <div class="stat-card"><div class="num">${stats.total || 0}</div><div class="label">Total PRs</div></div>
                <div class="stat-card"><div class="num">${stats.authors || 0}</div><div class="label">Contributors</div></div>
                <div class="stat-card"><div class="num">${stats.earliest || '-'}</div><div class="label">Earliest</div></div>
                <div class="stat-card"><div class="num">${stats.latest || '-'}</div><div class="label">Latest</div></div>`;
        }
        if (options) {
            const addOpts = (id, items) => {
                const sel = document.getElementById(id);
                if (sel) {
                    (items || []).forEach(v => { const o = document.createElement('option'); o.value = v; o.textContent = v; sel.appendChild(o); });
                }
            };
            addOpts('f_module', options.modules);
            addOpts('f_type', options.change_types);
            addOpts('f_version', options.versions);
            addOpts('f_author', options.authors);
        }
    } catch(e) { 
        console.error('Init failed:', e);
        const resultsEl = document.getElementById('results');
        if (resultsEl) {
            resultsEl.innerHTML = '<div class="error">初始化失败 (请检查 StarRocks 是否启动): ' + escHtml(e.message || e) + '</div>';
        }
    }
}

// Enter key triggers search
document.getElementById('query').addEventListener('keydown', e => {
    if (e.key === 'Enter') doSearch();
});

init();
</script>
</body>
</html>"""


# --- HTTP Server ---

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/ai/upload":
            self._handle_upload()
        else:
            self._json({"error": "not found"}, 404)

    def _handle_upload(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._json({"error": "invalid content-length"}, 400)
            return
        if length <= 0:
            self._json({"error": "empty body"}, 400)
            return
        if length > MAX_IMG_BYTES:
            self._json({"error": f"image too large (max {MAX_IMG_BYTES} bytes)"}, 413)
            return
        body = self.rfile.read(length)
        ext = _detect_image_ext(body)
        if not ext:
            self._json({"error": "unsupported image type (png/jpeg/gif/webp only)"}, 400)
            return
        img_id = uuid.uuid4().hex
        IMG_DIR.mkdir(parents=True, exist_ok=True)
        path = IMG_DIR / f"{img_id}.{ext}"
        path.write_bytes(body)
        self.log_message("[chat] upload id=%s size=%d ext=%s", img_id, length, ext)
        self._json({"id": img_id, "size": length, "ext": ext})

    def _resolve_images(self, params) -> list[str]:
        raw = params.get("images", "")
        if not raw:
            return []
        out = []
        for ident in raw.split(","):
            ident = ident.strip()
            if not re.fullmatch(r"[0-9a-f]{32}", ident):
                continue
            for p in IMG_DIR.glob(f"{ident}.*"):
                out.append(str(p))
                break
        return out

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = dict(urllib.parse.parse_qsl(parsed.query))

        if path == "/" or path == "/index.html":
            self._html(HTML_PAGE.replace("__REPO__", REPO))
        elif path == "/api/agent/search":
            self._handle_search(params)
        elif path == "/api/agent/filter":
            self._handle_filter(params)
        elif path.startswith("/api/agent/pr/"):
            self._handle_agent_pr(path)
        elif path == "/api/search":
            self._handle_search(params)
        elif path == "/api/analyze":
            self._handle_analyze(params)
        elif path == "/api/filter":
            self._handle_filter(params)
        elif path == "/api/stats":
            self._handle_stats()
        elif path == "/api/ai/start":
            self._handle_ai_start(params)
        elif path == "/api/ai/chat":
            self._handle_chat(params)
        elif path == "/api/options":
            self._handle_options()
        else:
            self._json({"error": "not found"}, 404)

    def _parse_top_k(self, params, default=50):
        try:
            top_k = int(params.get("top", default))
        except (TypeError, ValueError):
            raise ValueError("top must be an integer")
        if top_k <= 0:
            raise ValueError("top must be positive")
        return top_k

    def _parse_pr_number_filter(self, params):
        pr_number = params.get("pr_number", "")
        if not pr_number:
            return ""
        try:
            pn = int(pr_number)
        except (TypeError, ValueError):
            raise ValueError("pr_number must be an integer")
        # If the number is a backport PR, resolve to the main PR
        main_pr = resolve_backport_pr(pn)
        if main_pr:
            return str(main_pr)
        return str(pn)

    def _search_filters(self, params):
        filters = {k: params.get(k, "") for k in
                   ("module", "change_type", "version", "author", "since", "until")}
        filters["pr_number"] = self._parse_pr_number_filter(params)
        return filters

    def _filter_filters(self, params):
        filters = {k: params.get(k, "") for k in
                   ("module", "change_type", "version", "author", "since", "until", "keyword", "match_mode")}
        filters["pr_number"] = self._parse_pr_number_filter(params)
        return filters

    def _resolve_text_arg(self, params, primary: str) -> str:
        aliases = [primary]
        for name in ("query", "keyword", "q"):
            if name not in aliases:
                aliases.append(name)
        for name in aliases:
            value = params.get(name, "")
            if value:
                return value
        return ""

    def _handle_search(self, params):
        query = self._resolve_text_arg(params, "query")
        if not query:
            self._json({"error": "query, keyword, or q required"}, 400)
            return
        try:
            top_k = self._parse_top_k(params)
            filters = self._search_filters(params)
            results = attach_versions(search_vector(query, top_k, filters))
            self._json({"results": results})
        except ValueError as e:
            self._json({"error": str(e)}, 400)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_analyze(self, params):
        query = self._resolve_text_arg(params, "query")
        if not query:
            self._json({"error": "query, keyword, or q required"}, 400)
            return
        # Analyze top 5 most relevant PRs
        top_k = 5
        try:
            filters = self._search_filters(params)
            results = attach_versions(search_vector(query, top_k, filters))
            analysis = ollama_analyze_fix(query, results)
            self._json({"analysis": analysis, "results": results})
        except ValueError as e:
            self._json({"error": str(e)}, 400)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_filter(self, params):
        try:
            top_k = self._parse_top_k(params)
            filters = self._filter_filters(params)
            filters["keyword"] = self._resolve_text_arg(params, "keyword")
            results = attach_versions(search_sql(filters, top_k))
            self._json({"results": results})
        except ValueError as e:
            self._json({"error": str(e)}, 400)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_agent_pr(self, path):
        try:
            pr_number = int(path.rsplit("/", 1)[-1])
        except ValueError:
            self._json({"error": "invalid pr number"}, 400)
            return

        try:
            result = get_pr_detail(pr_number)
            if not result:
                self._json({"error": "pr not found"}, 404)
                return
            # Indicate if the queried number was a backport PR
            if int(result["pr_number"]) != pr_number:
                result["resolved_from_backport_pr"] = pr_number
            self._json({"result": result})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_stats(self):
        try:
            self._json(get_stats())
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_options(self):
        try:
            self._json(get_filter_options())
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse_stream(self, iterator):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            for event in iterator:
                payload = json.dumps(event, ensure_ascii=False, default=str)
                self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            logger.info("SSE client disconnected")
            iterator.close()

    def _handle_ai_start(self, params):
        prompt = params.get("prompt", "").strip()
        images = self._resolve_images(params)
        self.log_message("[chat] start prompt_len=%d images=%d prompt=%r", len(prompt), len(images), prompt)
        if not prompt:
            self._json({"error": "prompt required"}, 400)
            return

        def _tap():
            for ev in chat.start_session(prompt, images):
                if ev.get("type") == "session":
                    self.log_message("[chat] start session=%s", ev.get("session_id"))
                yield ev
        self._sse_stream(_tap())

    def _handle_chat(self, params):
        session = params.get("session", "").strip()
        prompt = params.get("prompt", "").strip()
        images = self._resolve_images(params)
        self.log_message("[chat] resume session=%s prompt_len=%d images=%d prompt=%r", session, len(prompt), len(images), prompt)
        if not session or not prompt:
            self._json({"error": "session and prompt required"}, 400)
            return
        self._sse_stream(chat.resume_session(session, prompt, images))

    def _html(self, content):
        body = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        logger.info(f"{self.address_string()} {format % args}")


def main():
    parser = argparse.ArgumentParser(description="PR Analytics Web UI")
    parser.add_argument("--port", type=int, default=8888, help="Server port (default: 8888)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    args = parser.parse_args()

    server = http.server.ThreadingHTTPServer((args.host, args.port), Handler)
    logger.info(f"PR Analytics Web UI running at http://localhost:{args.port}")
    logger.info(f"  StarRocks: {SR_HOST}:{SR_PORT}")
    logger.info(f"  Ollama:    {OLLAMA_HOST}:{OLLAMA_PORT}")
    logger.info(f"  Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
