#!/usr/bin/env python3
"""codex CLI 子进程封装 + JSONL 事件解析。"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Iterator

CODEX_BIN = os.getenv("CODEX_BIN", "codex")
CODEX_MODEL = os.getenv("CODEX_MODEL", "")
REPO_ROOT = os.getenv("STARROCKS_REPO", os.path.join(os.getcwd(), "starrocks"))

SESSIONS: dict[str, dict] = {}  # session_id -> {created_at, prompt_count}


def _dispatch_event(evt: dict) -> dict | None:
    """把 codex JSONL 事件转换成 SSE 事件字典。返回 None 表示忽略。"""
    etype = evt.get("type")
    item = evt.get("item") or {}
    itype = item.get("type")

    if etype == "thread.started":
        sid = evt.get("thread_id")
        if sid:
            return {"type": "session", "session_id": sid}
        return None

    if etype == "item.completed" and itype == "agent_message":
        text = item.get("text") or ""
        if text:
            return {"type": "message", "text": text, "delta": False}
        return None

    if etype == "item.started" and itype == "command_execution":
        cmd = item.get("command") or ""
        return {"type": "tool", "name": "bash", "args": cmd}

    if etype == "item.completed" and itype == "command_execution":
        out = item.get("aggregated_output") or ""
        return {"type": "tool_output", "text": str(out)[:1024]}

    if etype == "turn.completed":
        return None  # 由调用方处理 done 信号

    return None


def _fallback_session_id() -> str | None:
    """Codex 没推 thread.started 时，从 ~/.codex/sessions/ 取最新 rollout 文件名解析 UUID。"""
    base = Path.home() / ".codex" / "sessions"
    if not base.exists():
        return None
    files = list(base.rglob("rollout-*.jsonl"))
    if not files:
        return None
    newest = max(files, key=lambda p: p.stat().st_mtime)
    m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", newest.name)
    return m.group(1) if m else None


def _run_codex(cmd: list[str], register_session: bool) -> Iterator[dict]:
    """Spawn codex, parse JSONL, yield SSE event dicts."""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=REPO_ROOT,
        )
    except FileNotFoundError:
        yield {"type": "error", "text": f"codex CLI 未找到，检查 CODEX_BIN (current: {CODEX_BIN})"}
        return

    captured_session_id = None
    turn_completed = False
    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue

            if evt.get("type") == "turn.completed":
                turn_completed = True

            sse = _dispatch_event(evt)
            if sse is None:
                continue

            if sse["type"] == "session":
                captured_session_id = sse["session_id"]
                if register_session:
                    SESSIONS[sse["session_id"]] = {
                        "created_at": time.time(),
                        "prompt_count": 1,
                    }
            yield sse

        proc.wait(timeout=5)
        if proc.returncode and proc.returncode != 0:
            err = (proc.stderr.read() or "").strip()
            yield {"type": "error", "text": err or f"codex exited with code {proc.returncode}"}
            return

        # Fallback if no thread.started seen
        if register_session and captured_session_id is None:
            fallback = _fallback_session_id()
            if fallback:
                SESSIONS[fallback] = {"created_at": time.time(), "prompt_count": 1}
                yield {"type": "session", "session_id": fallback}
            else:
                yield {"type": "error", "text": "无法获取 session id，追问将不可用"}

        yield {"type": "done"}
    except GeneratorExit:
        proc.terminate()
        raise
    finally:
        if proc.poll() is None:
            proc.terminate()


def start_session(prompt: str, images: list[str] | None = None) -> Iterator[dict]:
    """首轮：起新 codex 会话调用 pr-fix-finder skill。"""
    cmd = [CODEX_BIN, "exec",
           "--sandbox", "workspace-write",
           "-c", "sandbox_workspace_write.network_access=true",
           "--json", "-C", REPO_ROOT]
    if CODEX_MODEL:
        cmd.extend(["--model", CODEX_MODEL])
    for img in (images or []):
        cmd.append(f"--image={img}")
    cmd.append(f"用 pr-fix-finder 分析: {prompt}")
    yield from _run_codex(cmd, register_session=True)


def resume_session(session_id: str, prompt: str, images: list[str] | None = None) -> Iterator[dict]:
    """续会话。session_id 必须先存在于 SESSIONS。"""
    if session_id not in SESSIONS:
        yield {"type": "error", "text": f"session {session_id} not found"}
        return
    cmd = [CODEX_BIN, "exec", "resume", session_id,
           "-c", 'sandbox_mode="workspace-write"',
           "-c", "sandbox_workspace_write.network_access=true",
           "--json"]
    if CODEX_MODEL:
        cmd.extend(["--model", CODEX_MODEL])
    for img in (images or []):
        cmd.append(f"--image={img}")
    cmd.append(prompt)
    SESSIONS[session_id]["prompt_count"] += 1
    yield from _run_codex(cmd, register_session=False)
