"""Who am I? Read off claude-code's own session registry, not configured.

Claude Code writes ~/.claude/sessions/<pid>.json for every live session
(name, sessionId, cwd, status). This MCP server is a descendant of that
pid, so walking up our parent chain finds it. Re-read on every use: the
name changes when the operator renames a session, and the handle follows.
"""

from __future__ import annotations

import json
import os
import re
import socket
from dataclasses import dataclass
from pathlib import Path

SESSIONS_DIR = Path(os.environ.get("ORCHESTRA_SESSIONS_DIR", Path.home() / ".claude" / "sessions"))
HANDLE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9._-]+", "-", name.lower()).strip("-._")
    return slug[:64] or "anon"


@dataclass(frozen=True)
class Identity:
    handle: str
    session_id: str | None
    name: str | None
    cwd: str
    status: str | None
    pid: int | None
    host: str
    source: str  # "env" | "session-registry" | "fallback"


def _ppid(pid: int) -> int | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # comm may contain spaces/parens; fields after the last ')' are fixed.
    rest = stat.rsplit(")", 1)[-1].split()
    try:
        return int(rest[1])
    except (IndexError, ValueError):
        return None


def find_session_file(start: int | None = None, max_hops: int = 12) -> Path | None:
    pid: int | None = start if start is not None else os.getppid()
    for _ in range(max_hops):
        if pid is None or pid <= 1:
            return None
        f = SESSIONS_DIR / f"{pid}.json"
        if f.exists():
            return f
        pid = _ppid(pid)
    return None


def whoami() -> Identity:
    host = socket.gethostname()
    override = os.environ.get("ORCHESTRA_HANDLE")
    data: dict = {}
    f = find_session_file()
    if f is not None:
        try:
            data = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            data = {}
    cwd = data.get("cwd") or os.getcwd()
    if override:
        handle, source = slugify(override), "env"
    elif data.get("name"):
        handle, source = slugify(data["name"]), "session-registry"
    else:
        handle, source = f"{slugify(Path(cwd).name)}-{os.getppid()}", "fallback"
    return Identity(
        handle=handle,
        session_id=data.get("sessionId"),
        name=data.get("name"),
        cwd=cwd,
        status=data.get("status"),
        pid=data.get("pid"),
        host=host,
        source=source,
    )
