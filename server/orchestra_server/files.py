"""Files sessions hand to the operator: stored briefly, outside the bus, readable only with the operator token.

A session uploads a file (POST /api/files) and then posts a message whose `attachments` name it by sha256.
What happens to the bytes depends on that message:
  - a note / request / ask to the operator: kept while the thread is open (at most MAX_OPEN), then for
    AFTER_CLOSE once the operator closes it (answer, ack, take default, withdraw), then deleted;
  - anything else: deleted at once (peers share the laptop; they can read the path themselves).
An upload no message claims within ORPHAN is deleted. Deleting keeps the record (name, size, sha256), so
the bus still says what was sent; only the bytes go.
"""

from __future__ import annotations

import base64
import binascii
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from .bus import now
from .models import CLOSING_KINDS, THREAD_KINDS, Event, is_to

MAX_FILE = 10 * 1024 * 1024
MAX_TOTAL = 500 * 1024 * 1024
AFTER_CLOSE = timedelta(hours=24)
MAX_OPEN = timedelta(days=7)
ORPHAN = timedelta(minutes=15)

# how a file is served back: shown inline only where the browser can't run anything from it
_TEXTY = ("text/", "application/json", "application/x-yaml", "application/yaml", "application/toml", "application/xml")
_IMAGES = ("image/png", "image/jpeg", "image/gif", "image/webp")


class FileIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    mime: str = Field(default="application/octet-stream", max_length=100)
    data_b64: str

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = v.replace("\\", "/").rsplit("/", 1)[-1].strip()
        if not v or v in (".", ".."):
            raise ValueError("a file needs a name")
        return v


class FileRec(BaseModel):
    sha256: str
    name: str
    mime: str
    size: int
    uploaded_at: datetime
    expires_at: datetime
    event_id: str | None = None
    thread_id: str | None = None
    deleted_at: datetime | None = None
    why_deleted: str | None = None


class Files:
    def __init__(self, root: Path, operator: str) -> None:
        self.root = root
        self.blobs = root / "blobs"
        self.blobs.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.index_path = root / "index.json"
        self.operator = operator
        self.recs: dict[str, FileRec] = {}
        if self.index_path.exists():
            try:
                self.recs = {k: FileRec.model_validate(v) for k, v in json.loads(self.index_path.read_text()).items()}
            except Exception as exc:
                print(f"orchestra: files index unreadable ({exc}); starting empty", file=sys.stderr)

    # -- store --------------------------------------------------------------

    def _save(self) -> None:
        tmp = self.index_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps({k: v.model_dump(mode="json") for k, v in self.recs.items()}))
            tmp.chmod(0o600)
            tmp.replace(self.index_path)
        except OSError as exc:
            print(f"orchestra: files index save failed: {exc}", file=sys.stderr)

    def stored_bytes(self) -> int:
        return sum(r.size for r in self.recs.values() if r.deleted_at is None)

    def upload(self, f: FileIn) -> FileRec:
        import hashlib
        try:
            data = base64.b64decode(f.data_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"data_b64 isn't base64: {exc}") from exc
        if len(data) > MAX_FILE:
            raise OverflowError(f"{f.name} is {len(data)} bytes; the limit is {MAX_FILE}")
        sha = hashlib.sha256(data).hexdigest()
        old = self.recs.get(sha)
        if old and old.deleted_at is None:
            return old  # the same bytes again: one copy
        self.sweep()
        if self.stored_bytes() + len(data) > MAX_TOTAL:
            raise OverflowError("the file store is full (500 MB); wait for older files to expire")
        p = self.blobs / sha
        p.write_bytes(data)
        p.chmod(0o600)
        rec = FileRec(sha256=sha, name=f.name, mime=f.mime, size=len(data), uploaded_at=now(), expires_at=now() + ORPHAN)
        self.recs[sha] = rec
        self._save()
        return rec

    def _delete(self, rec: FileRec, why: str) -> None:
        if rec.deleted_at is not None:
            return
        try:
            (self.blobs / rec.sha256).unlink(missing_ok=True)
        except OSError as exc:
            print(f"orchestra: couldn't delete file {rec.sha256[:12]}: {exc}", file=sys.stderr)
            return
        rec.deleted_at, rec.why_deleted = now(), why

    def known(self, sha: str) -> bool:
        r = self.recs.get(sha)
        return r is not None and r.deleted_at is None

    def path(self, sha: str) -> tuple[Path, FileRec] | None:
        r = self.recs.get(sha)
        if r is None or r.deleted_at is not None or not (self.blobs / sha).exists():
            return None
        return self.blobs / sha, r

    # -- lifecycle, driven by the bus ----------------------------------------

    def on_event(self, ev: Event, threads: dict) -> None:
        """Claim attachments (keep or clear them), and start the 24h clock when the operator closes a thread."""
        changed = False
        for a in ev.attachments or []:
            r = self.recs.get(a.sha256)
            if r is None or r.deleted_at is not None:
                continue
            for_operator = ev.kind in THREAD_KINDS and is_to(ev.to, self.operator)
            if for_operator:
                r.event_id = r.thread_id = ev.id
                r.expires_at = ev.ts + MAX_OPEN
            elif r.thread_id is None:
                self._delete(r, "not addressed to the operator: hashed, not kept")
            changed = True
        if ev.kind in CLOSING_KINDS and ev.ref in threads:
            for r in self.recs.values():
                if r.thread_id == ev.ref and r.deleted_at is None:
                    r.expires_at = min(r.expires_at, ev.ts + AFTER_CLOSE)
                    changed = True
        if changed:
            self._save()

    def sweep(self) -> int:
        t = now()
        gone = 0
        for r in self.recs.values():
            if r.deleted_at is None and r.expires_at <= t:
                self._delete(r, "expired" if r.thread_id else "never attached to a message")
                gone += 1
        # records of deleted files are kept 30 days, then dropped
        old = [k for k, r in self.recs.items() if r.deleted_at and t - r.deleted_at > timedelta(days=30)]
        for k in old:
            del self.recs[k]
        if gone or old:
            self._save()
        return gone

    def decorate(self, items: list[dict]) -> list[dict]:
        """Add availability to each attachment of each thread/event dict (for the page)."""
        for it in items:
            for a in it.get("attachments") or []:
                r = self.recs.get(a.get("sha256", ""))
                a["available"] = bool(r and r.deleted_at is None)
                if r:
                    a["expires_at"] = (r.deleted_at or r.expires_at).isoformat()
        return items

    @staticmethod
    def serve_as(mime: str) -> tuple[str, bool]:
        """(content type, inline?) to serve a file with: text as text/plain, raster images and PDF as
        themselves, anything else (HTML and SVG included) as a download."""
        m = (mime or "").split(";")[0].strip().lower()
        if m in ("text/html", "image/svg+xml"):
            return "text/plain; charset=utf-8", True
        if m.startswith(_TEXTY) or m in ("text/markdown", "application/x-sh"):
            return "text/plain; charset=utf-8", True
        if m in _IMAGES:
            return m, True
        if m == "application/pdf":
            return m, True
        return "application/octet-stream", False
