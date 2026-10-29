"""The bus: one append-only JSONL file, replayed into memory at startup.

Single process owns the file (uvicorn, one worker), so an asyncio lock is
the writer discipline; flock is taken anyway so a stray second writer
fails loudly instead of interleaving lines.
"""

from __future__ import annotations

import asyncio
import bisect
import fcntl
import json
import os
import secrets
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from .models import CLOSING_KINDS, SYSTEM, THREAD_KINDS, Event, PostIn, Presence, PresenceIn, Thread

PRESENCE_STALE_S = 180


def now() -> datetime:
    return datetime.now(UTC)


class Bus:
    def __init__(self, path: Path, operator: str = "operator") -> None:
        self.path = path
        self.operator = operator
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.events: list[Event] = []
        self.threads: dict[str, Thread] = {}
        self.presence: dict[str, Presence] = {}
        self._by_client_id: dict[str, Event] = {}
        self._lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue[Event]] = set()
        self.on_append: list[Callable[[Event], None]] = []
        self._load()

    # -- replay -------------------------------------------------------------

    def _load(self) -> None:
        if not self.path.exists():
            return
        bad = 0
        with self.path.open() as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = Event.model_validate(json.loads(line))
                except (json.JSONDecodeError, ValidationError) as exc:
                    bad += 1
                    print(f"orchestra: skipping bus line {lineno}: {exc}", file=sys.stderr)
                    continue
                self._apply(ev)
        if bad:
            print(f"orchestra: {bad} unreadable bus line(s) left in place, not replayed", file=sys.stderr)

    @property
    def head(self) -> int:
        return self.events[-1].seq if self.events else 0

    def _apply(self, ev: Event) -> None:
        self.events.append(ev)
        if ev.client_id:
            self._by_client_id[ev.client_id] = ev
        if ev.kind in THREAD_KINDS:
            self.threads[ev.id] = Thread.model_validate(
                {
                    "id": ev.id, "kind": ev.kind, "from": ev.from_, "to": ev.to, "ts": ev.ts,
                    "body": ev.body, "topic": ev.topic, "default": ev.default,
                    "deadline": ev.deadline, "revert_cost": ev.revert_cost,
                    "options": ev.options, "multi": ev.multi, "context": ev.context,
                }
            )
        elif ev.kind in CLOSING_KINDS and ev.ref in self.threads:
            t = self.threads[ev.ref]
            if t.to == "@all" and ev.kind == "answer" and ev.from_ != t.from_:
                # A broadcast request collects one answer per session; only its opener closes it.
                t.answers.append({"from": ev.from_, "body": ev.body, "ts": ev.ts.isoformat(), "id": ev.id, "kind": "answer"})
                return
            if t.status == "open":
                t.status = {"answer": "answered", "ack": "acked", "defaulted": "defaulted", "withdraw": "withdrawn"}[ev.kind]  # type: ignore[assignment]
                t.resolution = ev.body or (t.default if ev.kind == "defaulted" else None)
                t.choice = ev.choice if ev.kind == "answer" else ([t.default] if ev.kind == "defaulted" and t.options else None)
                t.resolved_at = ev.ts
                t.resolved_by = ev.from_
        elif ev.kind == "reply" and ev.ref in self.threads:
            # Replies are comments, not resolutions, but they belong with the thread they answer
            # (a broadcast's responses, or a back-and-forth asking for more info).
            self.threads[ev.ref].answers.append(
                {"from": ev.from_, "body": ev.body, "ts": ev.ts.isoformat(), "id": ev.id, "kind": "reply"})

    # -- write --------------------------------------------------------------

    def _new_id(self, kind: str) -> str:
        prefix = {"ask": "a", "request": "r", "note": "n"}.get(kind, "m")
        while True:
            i = prefix + secrets.token_hex(3)
            if i not in self.threads:
                return i

    def resolve_to(self, post: PostIn) -> str:
        """Closing events go back to whoever opened the thread."""
        if post.kind in CLOSING_KINDS | {"reply"} and post.ref in self.threads and post.to == "@all":
            opener = self.threads[post.ref].from_
            return f"@{opener}"
        return post.to

    async def append(self, post: PostIn) -> Event:
        async with self._lock:
            if post.client_id and post.client_id in self._by_client_id:
                return self._by_client_id[post.client_id]  # offline replay, already have it
            ev = Event.model_validate(
                {
                    **post.model_dump(by_alias=True),
                    "to": self.resolve_to(post),
                    "seq": self.head + 1,
                    "id": self._new_id(post.kind),
                    "ts": now(),
                }
            )
            line = json.dumps(ev.wire(), ensure_ascii=False) + "\n"
            with self.path.open("a") as f:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    f.write(line)
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
            self._apply(ev)
        for q in list(self._subscribers):
            q.put_nowait(ev)
        for hook in self.on_append:
            try:
                hook(ev)
            except Exception as exc:  # a broken hook must never break the bus
                print(f"orchestra: on_append hook failed: {exc!r}", file=sys.stderr)
        return ev

    # -- read ---------------------------------------------------------------

    def since(self, after: int, limit: int = 500) -> list[Event]:
        # seq is monotonic but may have gaps if a damaged line was skipped at replay.
        start = bisect.bisect_right(self.events, after, key=lambda e: e.seq)
        return self.events[start : start + limit]

    def search(self, q: str, limit: int = 200) -> list[Event]:
        """Exact id/ref matches first (a thread and everything that points at it), then text."""
        q = q.strip()
        if not q:
            return []
        ql = q.lower().lstrip("#")
        exact = [e for e in self.events if e.id == ql or e.ref == ql]
        if exact:
            return exact[-limit:][::-1]
        handle = ql[1:] if ql.startswith("@") else None
        def hit(e: Event) -> bool:
            if handle is not None:
                return e.from_ == handle or e.to == "@" + handle
            return (ql in e.body.lower() or ql in e.from_ or ql in e.to or ql == e.kind
                    or (e.topic is not None and ql in e.topic.lower()))
        out = [e for e in reversed(self.events) if hit(e)]
        return out[:limit]

    def subscribe(self, after: int) -> asyncio.Queue[Event]:
        """Backlog from `after` preloaded, then live. Same tick, so nothing slips between."""
        q: asyncio.Queue[Event] = asyncio.Queue()
        for ev in self.since(after, limit=10_000):
            q.put_nowait(ev)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[Event]) -> None:
        self._subscribers.discard(q)

    # -- presence -----------------------------------------------------------

    def touch(self, p: PresenceIn) -> Presence:
        pres = Presence(**p.model_dump(), last_seen=now())
        self.presence[p.handle] = pres
        return pres

    def live_presence(self, operator_view: bool = False) -> list[dict]:
        cutoff = now().timestamp() - PRESENCE_STALE_S
        out = []
        for p in sorted(self.presence.values(), key=lambda p: p.handle):
            # usage is telemetry and the bridge id opens the session: both are the operator's to see
            d = p.model_dump(mode="json", exclude=None if operator_view else {"usage", "bridge"})
            d["live"] = p.last_seen.timestamp() >= cutoff
            out.append(d)
        return out

    # -- deadlines ----------------------------------------------------------

    def overdue(self) -> list[Thread]:
        t_now = now()
        return [
            t for t in self.threads.values()
            if t.kind == "ask" and t.status == "open" and t.deadline is not None and t.deadline <= t_now
        ]

    async def sweep_deadlines(self) -> list[Event]:
        out = []
        for t in self.overdue():
            out.append(
                await self.append(
                    PostIn.model_validate(
                        {"from": SYSTEM, "to": f"@{t.from_}", "kind": "defaulted", "ref": t.id,
                         "body": t.default or "(no default given)"}
                    )
                )
            )
        return out

    def inbox(self, recent: int = 50) -> dict:
        mine = [t for t in self.threads.values() if t.to == f"@{self.operator}"]
        open_ = sorted(
            (t for t in mine if t.status == "open"),
            key=lambda t: (t.kind != "ask", t.deadline or datetime.max.replace(tzinfo=UTC), t.ts),
        )
        closed = sorted((t for t in mine if t.status != "open"), key=lambda t: t.resolved_at or t.ts, reverse=True)
        sent = sorted((t for t in self.threads.values() if t.from_ == self.operator),
                      key=lambda t: (t.status != "open", -t.ts.timestamp()))
        sent = [t for t in sent if t.status == "open"] + [t for t in sent if t.status != "open"][:10]
        return {"open": [t.wire() for t in open_], "closed": [t.wire() for t in closed[:recent]],
                "sent": [t.wire() for t in sent]}
