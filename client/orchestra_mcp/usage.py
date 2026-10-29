"""Token usage for this session, read from Claude Code's own transcript.

Claude Code appends every API turn to ~/.claude/projects/<cwd-slug>/<sessionId>.jsonl,
and assistant rows carry `message.usage`. Streaming can write the same message id
more than once, so each id counts once (its latest usage wins). The file is read
incrementally from a byte offset; only complete lines are consumed.
"""

from __future__ import annotations

import json
import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path

PROJECTS_DIR = Path(os.environ.get("ORCHESTRA_CLAUDE_PROJECTS", Path.home() / ".claude" / "projects"))
_KEEP_IDS = 512  # duplicates of a message id arrive close together; this bounds memory


def project_slug(cwd: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "-", cwd)


@dataclass
class UsageTotals:
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    thinking: int = 0
    turns: int = 0
    context: int = 0  # prompt size of the latest turn ~= current context window use
    model: str | None = None
    last_ts: str | None = None

    def wire(self) -> dict:
        return {k: v for k, v in vars(self).items() if v is not None}


def _row_usage(u: dict) -> tuple[int, int, int, int, int]:
    thinking = (u.get("output_tokens_details") or {}).get("thinking_tokens") or 0
    return (int(u.get("input_tokens") or 0), int(u.get("output_tokens") or 0),
            int(u.get("cache_read_input_tokens") or 0), int(u.get("cache_creation_input_tokens") or 0), int(thinking))


@dataclass
class UsageTracker:
    session_id: str | None = None
    path: Path | None = None
    offset: int = 0
    totals: UsageTotals = field(default_factory=UsageTotals)
    _seen: OrderedDict[str, tuple[int, int, int, int, int]] = field(default_factory=OrderedDict)

    def _locate(self, session_id: str, cwd: str) -> Path | None:
        p = PROJECTS_DIR / project_slug(cwd) / f"{session_id}.jsonl"
        if p.exists():
            return p
        hits = list(PROJECTS_DIR.glob(f"*/{session_id}.jsonl"))
        return hits[0] if hits else None

    def poll(self, session_id: str | None, cwd: str) -> UsageTotals | None:
        """Read whatever is new; None when there is no transcript to read."""
        if not session_id:
            return None
        if session_id != self.session_id:  # first poll, or the session was swapped under us
            self.session_id, self.path, self.offset = session_id, None, 0
            self.totals, self._seen = UsageTotals(), OrderedDict()
        if self.path is None:
            self.path = self._locate(session_id, cwd)
            if self.path is None:
                return None
        try:
            size = self.path.stat().st_size
            if size < self.offset:  # truncated/rewritten: start over
                self.offset, self.totals, self._seen = 0, UsageTotals(), OrderedDict()
            with self.path.open("rb") as f:
                f.seek(self.offset)
                chunk = f.read()
        except OSError:
            return self.totals
        end = chunk.rfind(b"\n")
        if end < 0:
            return self.totals
        self.offset += end + 1
        for raw in chunk[: end + 1].splitlines():
            if b'"usage"' not in raw:
                continue
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            msg = row.get("message")
            if not isinstance(msg, dict) or not isinstance(msg.get("usage"), dict):
                continue
            self._add(msg.get("id") or row.get("uuid") or str(self.offset), _row_usage(msg["usage"]),
                      msg.get("model"), row.get("timestamp"))
        return self.totals

    def _add(self, mid: str, u: tuple[int, int, int, int, int], model: str | None, ts: str | None) -> None:
        t = self.totals
        old = self._seen.pop(mid, None)
        if old is not None:
            t.input -= old[0]; t.output -= old[1]; t.cache_read -= old[2]; t.cache_write -= old[3]; t.thinking -= old[4]
        else:
            t.turns += 1
        t.input += u[0]; t.output += u[1]; t.cache_read += u[2]; t.cache_write += u[3]; t.thinking += u[4]
        self._seen[mid] = u
        while len(self._seen) > _KEEP_IDS:
            self._seen.popitem(last=False)
        t.context = u[0] + u[2] + u[3]
        t.model = model or t.model
        t.last_ts = ts or t.last_ts
