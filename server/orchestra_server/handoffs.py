"""Handoff notes written at compaction (by the claude-continuity plugin), kept for the operator.

A session's MCP uploads its newest note; the server keeps the last KEEP per handle in
handoffs.json. Notes quote the conversation, so only the operator can read them.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from .models import HANDLE_RE

KEEP = 5


class HandoffIn(BaseModel):
    handle: str
    session_id: str | None = Field(default=None, max_length=64)
    why: str | None = Field(default=None, max_length=120)  # "before a manual compaction", "on request"
    written_at: datetime
    text: str = Field(min_length=1, max_length=64_000)

    @field_validator("handle")
    @classmethod
    def _handle(cls, v: str) -> str:
        if not HANDLE_RE.match(v):
            raise ValueError(f"bad handle {v!r}")
        return v


class Handoffs:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.by_handle: dict[str, list[HandoffIn]] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text())
                self.by_handle = {h: [HandoffIn.model_validate(x) for x in xs] for h, xs in raw.items()}
            except Exception as exc:
                print(f"orchestra: handoffs file unreadable ({exc}); starting empty", file=sys.stderr)

    def add(self, h: HandoffIn) -> int:
        notes = [x for x in self.by_handle.get(h.handle, []) if x.written_at != h.written_at]
        notes.append(h)
        notes.sort(key=lambda x: x.written_at)
        self.by_handle[h.handle] = notes[-KEEP:]
        tmp = self.path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps({k: [x.model_dump(mode="json") for x in v] for k, v in self.by_handle.items()}))
            tmp.chmod(0o600)
            tmp.replace(self.path)
        except OSError as exc:
            print(f"orchestra: handoffs save failed: {exc}", file=sys.stderr)
        return len(self.by_handle[h.handle])

    def notes_for(self, handles: list[str]) -> list[HandoffIn]:
        """A session's notes under all its names (a rename, e.g. after a restart, keeps its earlier notes), oldest first."""
        seen: dict[datetime, HandoffIn] = {}
        for h in handles:
            for n in self.by_handle.get(h, []):
                seen.setdefault(n.written_at, n)
        return sorted(seen.values(), key=lambda n: n.written_at)[-KEEP:]

    def latest_at(self, handles: list[str]) -> str | None:
        notes = self.notes_for(handles)
        return notes[-1].written_at.isoformat() if notes else None
