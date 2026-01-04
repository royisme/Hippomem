# Input: 钩子事件载荷（stdin JSON）与 MEMLAYER_* 环境变量。
# Output: 供 Claude/Codex 使用的可选 stdout 上下文与 MemLayer 副作用。
# Position: 记忆自动化的 Hook 入口脚本。
import json
import os
import subprocess
import sys
import textwrap
import uuid
from datetime import datetime, timezone

def _read_stdin_json():
    data = sys.stdin.read()
    if not data.strip():
        return {}
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return {}

def _env(name, default=None):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value

def _scope_from_env():
    cwd_name = os.path.basename(os.getcwd())
    scope = {
        "tenant_id": _env("MEMLAYER_TENANT_ID", "default"),
        "workspace_id": _env("MEMLAYER_WORKSPACE_ID", cwd_name),
    }
    optional = {
        "repo_id": _env("MEMLAYER_REPO_ID"),
        "session_id": _env("MEMLAYER_SESSION_ID"),
        "task_id": _env("MEMLAYER_TASK_ID"),
        "user_id": _env("MEMLAYER_USER_ID"),
        "module": _env("MEMLAYER_MODULE"),
        "environment": _env("MEMLAYER_ENVIRONMENT"),
    }
    for key, value in optional.items():
        if value:
            scope[key] = value
    return scope

def _run_cli(args, payload=None):
    cmd = [sys.executable, "-m", "memlayer.cli"] + args
    env = os.environ.copy()
    if payload is None:
        payload = {}
    result = subprocess.run(cmd, input=json.dumps(payload), text=True, capture_output=True, env=env)
    return result

def _init_db(db_path):
    args = ["init", "--db-path", db_path]
    return _run_cli(args)

def _event_upsert(scope, content, metadata, idempotency_key, db_path):
    payload = {
        "scope": json.dumps(scope),
        "payload": json.dumps({"content": content, "metadata": metadata}),
        "idempotency_key": idempotency_key,
        "db_path": db_path,
    }
    args = [
        "event", "upsert",
        "--scope", payload["scope"],
        "--payload", payload["payload"],
        "--idempotency-key", payload["idempotency_key"],
        "--db-path", payload["db_path"],
    ]
    return _run_cli(args)

def _search(scope, query, db_path, view="index", budget=1000, top_k=8):
    args = [
        "search",
        "--scope", json.dumps(scope),
        "--query", query,
        "--view", view,
        "--budget", str(budget),
        "--top-k", str(top_k),
        "--db-path", db_path,
    ]
    return _run_cli(args)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _truncate(text, limit=1000):
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "..."

def _extract_prompt(payload):
    for key in ("prompt", "input", "message", "user_prompt"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def _extract_tool(payload):
    tool_name = payload.get("tool_name") or payload.get("toolName") or payload.get("tool")
    output = payload.get("output") or payload.get("result") or payload.get("response")
    return tool_name, output

def _emit_search_context(result):
    if not result.stdout:
        return
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError:
        return
    data = parsed.get("data") or {}
    items = data.get("results") or []
    if not items:
        return
    lines = ["MemLayer Search Results:"]
    for item in items:
        title = item.get("title") or ""
        summary = item.get("summary") or ""
        memory_id = item.get("id") or item.get("memory_id") or ""
        line = f"- [{memory_id}] {title} :: {summary}".strip()
        lines.append(line)
    output = "\n".join(lines)
    sys.stdout.write(output + "\n")

def main():
    event = _env("MEMLAYER_HOOK_EVENT", "")
    db_path = _env("MEMLAYER_DB_PATH", os.path.expanduser("~/.local/share/memlayer/memlayer.db"))
    payload = _read_stdin_json()
    scope = _scope_from_env()
    idempotency_key = str(uuid.uuid4())
    timestamp = _now_iso()

    if event == "SessionStart":
        _init_db(db_path)
        content = f"Session started at {timestamp}."
        _event_upsert(scope, content, {"event": event}, idempotency_key, db_path)
        return 0

    if event == "UserPromptSubmit":
        prompt = _extract_prompt(payload)
        if prompt:
            _event_upsert(scope, prompt, {"event": event}, idempotency_key, db_path)
            search_result = _search(scope, prompt, db_path)
            _emit_search_context(search_result)
        return 0

    if event == "PostToolUse":
        tool_name, output = _extract_tool(payload)
        if tool_name:
            content = f"Tool used: {tool_name}"
            metadata = {
                "event": event,
                "output": _truncate(str(output)),
            }
            _event_upsert(scope, content, metadata, idempotency_key, db_path)
        return 0

    if event == "PreCompact":
        content = f"Context compaction triggered at {timestamp}."
        _event_upsert(scope, content, {"event": event}, idempotency_key, db_path)
        return 0

    if event == "Stop":
        content = f"Session ended at {timestamp}."
        _event_upsert(scope, content, {"event": event}, idempotency_key, db_path)
        return 0

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
