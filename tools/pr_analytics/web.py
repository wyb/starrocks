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
import urllib.parse
from pathlib import Path
import pymysql

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
       d.ai_summary, d.ai_summary_en, d.merged_at, d.additions, d.deletions, d.changed_files,
       approx_cosine_similarity(d.embedding, ARRAY<FLOAT>{vec_str}) AS score
FROM pr_data d
{join_clause}
WHERE {where}
ORDER BY score DESC
LIMIT {top_k};
"""
    return sr_query(sql)


def search_sql(filters: dict, top_k: int) -> list[dict]:
    def build_where_clause(mode, field, kw):
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
            clauses.append(f"(lower(d.title) LIKE lower('%{kw}%') OR lower(d.ai_summary) LIKE lower('%{kw}%') OR lower(d.ai_summary_en) LIKE lower('%{kw}%'))")
        elif mode == "all":
            clauses.append(f"d.{field} MATCH_ALL '{kw}'")
        elif mode == "any":
            clauses.append(f"d.{field} MATCH_ANY '{kw}'")

        return " AND ".join(clauses)

    def execute(where_stmt):
        join_clause = ""
        v_filter = ""
        if filters.get("version"):
            join_clause = "JOIN pr_versions v ON d.pr_number = v.pr_number"
            v_filter = f" AND v.version = '{filters['version']}'"

        sql = f"""
SELECT d.pr_number, d.title, d.author, d.module, d.change_type, d.version,
       d.ai_summary, d.ai_summary_en, d.merged_at, d.additions, d.deletions, d.changed_files
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
        return execute(build_where_clause("none", None, ""))

    # Mode: LIKE
    if mode == "like":
        return execute(build_where_clause("like", None, kw))

    # Mode: MATCH ALL
    if mode == "all":
        for field in ["title", "ai_summary", "ai_summary_en"]:
            res = execute(build_where_clause("all", field, kw))
            if res: return res
        return []

    # Mode: MATCH ANY
    if mode == "any":
        for field in ["title", "ai_summary", "ai_summary_en"]:
            res = execute(build_where_clause("any", field, kw))
            if res: return res
        return []

    # Mode: AUTO (LIKE -> ALL -> ANY)
    # 1. Try LIKE
    res = execute(build_where_clause("like", None, kw))
    if res: return res

    # 2. Try MATCH ALL (Priority)
    for field in ["title", "ai_summary", "ai_summary_en"]:
        res = execute(build_where_clause("all", field, kw))
        if res: return res

    # 3. Try MATCH ANY (Priority)
    for field in ["title", "ai_summary", "ai_summary_en"]:
        res = execute(build_where_clause("any", field, kw))
        if res: return res

    return []



def get_stats() -> dict:
    rows = sr_query(f"""
SELECT COUNT(*) AS total,
       COUNT(DISTINCT author) AS authors,
       COUNT(DISTINCT module) AS modules,
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
.score-bar-bg { display: inline-block; width: 60px; height: 6px; background: #e8eaed; border-radius: 3px; vertical-align: middle; position: relative; }
.score-bar { display: block; height: 6px; background: #1a73e8; border-radius: 3px; position: absolute; top: 0; left: 0; }

.loading { text-align: center; padding: 40px; color: #666; }
.empty { text-align: center; padding: 40px; color: #999; }
.error { text-align: center; padding: 20px; color: #c5221f; background: #fce8e6;
         border-radius: 8px; margin: 12px 0; }
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
            <div class="toggle-filters" id="toggle_btn" onclick="toggleFilters()">展开筛选<i></i></div>
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

<script>
const REPO = '__REPO__';

async function api(path, params) {
    const qs = new URLSearchParams(params).toString();
    const resp = await fetch('/api/' + path + '?' + qs);
    return resp.json();
}

function toggleFilters() {
    const container = document.getElementById('filter_container');
    const btn = document.getElementById('toggle_btn');
    const isCollapsed = container.classList.toggle('collapsed');
    btn.classList.toggle('active', !isCollapsed);
    btn.innerHTML = (isCollapsed ? '展开筛选' : '收起筛选') + '<i></i>';
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

function renderResults(results, showScore) {
    const el = document.getElementById('results');
    if (!results || results.length === 0) {
        el.innerHTML = '<div class="empty">没有找到匹配的 PR</div>';
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
            ${r.ai_summary_en ? '<div class="summary" style="color:#888;margin-top:4px;">' + escHtml(r.ai_summary_en) + '</div>' : ''}
        </div>`;
    });
    el.innerHTML = html;
}

function escHtml(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

// Init: load stats and filter options
async function init() {
    try {
        const [stats, options] = await Promise.all([api('stats', {}), api('options', {})]);
        const se = document.getElementById('stats');
        if (stats) {
            se.innerHTML = `
                <div class="stat-card"><div class="num">${stats.total || 0}</div><div class="label">Total PRs</div></div>
                <div class="stat-card"><div class="num">${stats.authors || 0}</div><div class="label">Contributors</div></div>
                <div class="stat-card"><div class="num">${stats.modules || 0}</div><div class="label">Modules</div></div>
                <div class="stat-card"><div class="num">${stats.earliest || '-'}</div><div class="label">Earliest</div></div>
                <div class="stat-card"><div class="num">${stats.latest || '-'}</div><div class="label">Latest</div></div>`;
        }
        if (options) {
            const addOpts = (id, items) => {
                const sel = document.getElementById(id);
                (items || []).forEach(v => { const o = document.createElement('option'); o.value = v; o.textContent = v; sel.appendChild(o); });
            };
            addOpts('f_module', options.modules);
            addOpts('f_type', options.change_types);
            addOpts('f_version', options.versions);
            addOpts('f_author', options.authors);
        }
    } catch(e) { console.error('Init failed:', e); }
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
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = dict(urllib.parse.parse_qsl(parsed.query))

        if path == "/" or path == "/index.html":
            self._html(HTML_PAGE.replace("__REPO__", REPO))
        elif path == "/api/search":
            self._handle_search(params)
        elif path == "/api/filter":
            self._handle_filter(params)
        elif path == "/api/stats":
            self._handle_stats()
        elif path == "/api/options":
            self._handle_options()
        else:
            self._json({"error": "not found"}, 404)

    def _handle_search(self, params):
        query = params.get("query", "")
        if not query:
            self._json({"error": "query required"}, 400)
            return
        top_k = int(params.get("top", 50))
        filters = {k: params.get(k, "") for k in
                   ("pr_number", "module", "change_type", "version", "author", "since", "until")}
        try:
            results = attach_versions(search_vector(query, top_k, filters))
            self._json({"results": results})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _handle_filter(self, params):
        top_k = int(params.get("top", 50))
        filters = {k: params.get(k, "") for k in
                   ("pr_number", "module", "change_type", "version", "author", "since", "until", "keyword", "match_mode")}
        try:
            results = attach_versions(search_sql(filters, top_k))
            self._json({"results": results})
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

    server = http.server.HTTPServer((args.host, args.port), Handler)
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
