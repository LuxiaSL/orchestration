"""The link to the bus: HTTP posts, the SSE stream, and offline survival.

Invariant 1 (nothing hard-blocks on the human operator) must hold even when the bus is
unreachable, so this side keeps its own copy of every open ask and takes
the default locally if the deadline passes while the server is dark.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from mcp.server.mcpserver.exceptions import ToolError

from .identity import Identity, whoami
from .usage import UsageTracker

CONFIG_PATH = Path(os.environ.get("ORCHESTRA_CONFIG", Path.home() / ".config" / "orchestra" / "config.json"))
STATE_ROOT = Path(os.environ.get("ORCHESTRA_STATE", Path.home() / ".cache" / "orchestra"))

# Peer broadcasts that are worth interrupting a session for. `status`,
# `hello`, `goodbye` to @all stay on the bus and are read on demand.
PEER_BROADCAST_PUSH = frozenset({"info", "request"})
LOCAL_DEFAULT_GRACE = timedelta(seconds=90)
# More missed messages than this on catch-up become one summary push, not a flood of turns.
CATCHUP_PUSH_LIMIT = 5
# Pushes that arrive this close together become one turn (one wrapper, one interruption).
PUSH_BATCH_S = 1.5
PUSH_BATCH_MAX_S = 6.0


class ConfigError(ToolError):
    """ToolError subclasses reach the model with their message; anything else is masked."""


@dataclass(frozen=True)
class Config:
    url: str
    token: str
    operator: str = "operator"

    @staticmethod
    def saved() -> dict:
        if not CONFIG_PATH.exists():
            return {}
        try:
            return json.loads(CONFIG_PATH.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigError(f"could not read {CONFIG_PATH}: {exc}") from exc

    @classmethod
    def load(cls) -> Config:
        saved = cls.saved()
        url = os.environ.get("ORCHESTRA_URL") or saved.get("url")
        token = os.environ.get("ORCHESTRA_TOKEN") or saved.get("token")
        operator = os.environ.get("ORCHESTRA_OPERATOR") or saved.get("operator") or "operator"
        if not (url and token):
            raise ConfigError(
                f"no orchestra config: set ORCHESTRA_URL and ORCHESTRA_TOKEN or write {CONFIG_PATH} "
                '({"url": "https://orchestra.example.com", "token": "<agent token>", "operator": "<your handle>"})'
            )
        return cls(url=url.rstrip("/"), token=token, operator=operator.lower())


def is_for_me(ev: dict, me: str, operator: str = "operator") -> bool:
    """Should this event interrupt my session?"""
    if ev.get("from") == me:
        return False
    to = ev.get("to") or ""
    if f"@{me}" in to.split(","):  # named, alone or among several: a direct message
        return True
    if to == "@all":
        return ev.get("from") == operator or ev.get("kind") in PEER_BROADCAST_PUSH
    return False


def _client_build() -> str:
    """Hash of this package's source as loaded: tells a stale client (needs /mcp reconnect) from a current one."""
    h = hashlib.sha256()
    for f in sorted(Path(__file__).parent.glob("*.py")):
        try:
            h.update(f.name.encode() + f.read_bytes())
        except OSError:
            pass
    return h.hexdigest()[:8]


CLIENT_BUILD = _client_build()

_ID_RE = re.compile(r"\b[amnr][0-9a-f]{6}\b")


def mention_hint(ev: dict) -> str:
    """Point at bus ids a message mentions, so 'read n3a2e52' becomes one get() away."""
    own = {ev.get("id"), ev.get("ref")}
    ids = [i for i in dict.fromkeys(_ID_RE.findall(ev.get("body") or "")) if i not in own][:6]
    return f"\n(mentions {', '.join(ids)}: get(id) reads one in full)" if ids else ""


def _one_line(text: str | None, n: int) -> str:
    """Collapse whitespace so a multi-line body can't break a list it's quoted in."""
    flat = " ".join((text or "").split())
    return flat if len(flat) <= n else flat[: n - 1] + "…"


def render(ev: dict, operator: str = "operator") -> str:
    """One event as the line the model sees."""
    kind, frm, body = ev.get("kind"), ev.get("from"), ev.get("body", "")
    ref = f" re {ev['ref']}" if ev.get("ref") else ""
    # Name the thread being closed/answered, so the push makes sense after a compaction.
    about = f" (your '{ev['ref_excerpt']}')" if ev.get("ref_excerpt") else ""
    topic = f" [{ev['topic']}]" if ev.get("topic") else ""
    if kind == "answer":
        if ev.get("choice") is not None:
            picked = ", ".join(repr(c) for c in ev["choice"]) or "(none of the options)"
            note = f"\nTheir comment: {body}" if body else ""
            return (f"{frm} answered {ev.get('ref')}{about} → chose {picked}.{note}\n"
                    "(Act on this choice; it supersedes the default.)")
        return f"{frm} answered {ev.get('ref')}{about}: {body}\n(Act on this answer; it supersedes the default.)"
    if kind == "reply" and frm == operator and ev.get("ref"):
        return (f"{frm} replied re {ev['ref']}{about}: {body}\n"
                f"(If {ev['ref']} is your open question, they want more before deciding: answer with "
                f"post(kind='reply', ref='{ev['ref']}', to='@{frm}'). It stays open, and any deadline still applies.)")
    if kind == "defaulted":
        return (f"ask {ev.get('ref')}{about} reached its deadline unanswered → take the DEFAULT: {body}\n"
                "(Proceed with the default and say so in your next status.)")
    if kind == "ack":
        return f"{frm} acknowledged {ev.get('ref')}{about}" + (f": {body}" if body else "")
    if kind == "halt":
        return (f"HALT from {frm}{topic}: {body or 'pause'}\n"
                "Finish only the current atomic step, post a `status` saying where you stopped, "
                "start nothing new, and end your turn. A `resume` message will wake you.")
    if kind == "resume":
        return f"RESUME from {frm}{topic}: {body or 'carry on'}"
    if kind == "notice":
        return f"notice from {frm}{topic} → {ev.get('to')}: {body}"
    if ev.get("to") == "@all" and frm != operator and kind in ("info", "request"):
        # A peer's broadcast is usually not for you: one line, full text on demand.
        first = (body.strip().splitlines() or [""])[0]
        first = first if len(first) <= 140 else first[:139] + "…"
        more = f" get('{ev.get('id')}') for the full text." if len(body) > len(first) else ""
        ask = f" Answer with post(kind='answer', ref='{ev.get('id')}') if it's yours to answer." if kind == "request" else ""
        return f"{kind} {ev.get('id')} from {frm}{topic} → @all: {first}{more}{ask}"
    if kind == "request":
        return (f"request {ev.get('id')} from {frm}{topic}: {body}\n"
                f"(Answer when it suits you with post(kind='answer', ref='{ev.get('id')}').)")
    return f"{kind} from {frm}{ref}{topic} → {ev.get('to')}: {body}"


@dataclass
class OpenAsk:
    client_id: str
    id: str | None
    question: str
    default: str
    deadline: str
    resolved: str | None = None


@dataclass
class LocalState:
    root: Path
    cursor: int | None = None
    read_cursor: int = 0
    handles: list[str] = field(default_factory=list)  # every handle this session has gone by
    peer_broadcasts: bool = True  # push peers' @all info/request (off: read() them instead)
    asks: dict[str, OpenAsk] = field(default_factory=dict)  # by client_id

    @classmethod
    def load(cls, session_key: str) -> LocalState:
        root = STATE_ROOT / session_key
        root.mkdir(parents=True, exist_ok=True)
        st = cls(root=root)
        f = root / "state.json"
        if f.exists():
            try:
                d = json.loads(f.read_text())
                st.cursor = d.get("cursor")
                st.read_cursor = d.get("read_cursor", 0)
                st.handles = list(d.get("handles", []))
                st.peer_broadcasts = bool(d.get("peer_broadcasts", True))
                st.asks = {k: OpenAsk(**v) for k, v in d.get("asks", {}).items()}
            except (OSError, json.JSONDecodeError, TypeError) as exc:
                print(f"orchestra-mcp: local state unreadable ({exc}); starting fresh", file=sys.stderr)
        return st

    def save(self) -> None:
        tmp = self.root / "state.json.tmp"
        tmp.write_text(json.dumps({
            "cursor": self.cursor, "read_cursor": self.read_cursor, "handles": self.handles[-10:],
            "peer_broadcasts": self.peer_broadcasts,
            "asks": {k: vars(v) for k, v in self.asks.items()},
        }, indent=1))
        tmp.replace(self.root / "state.json")

    # spool: posts that could not be delivered, replayed in order on reconnect
    @property
    def spool_path(self) -> Path:
        return self.root / "spool.jsonl"

    def spool(self, body: dict) -> None:
        with self.spool_path.open("a") as f:
            f.write(json.dumps(body) + "\n")

    def drain_spool(self) -> list[dict]:
        if not self.spool_path.exists():
            return []
        lines = [json.loads(l) for l in self.spool_path.read_text().splitlines() if l.strip()]
        return lines


Notify = Callable[[str, dict[str, str]], Awaitable[None]]


class Link:
    def __init__(self, config: Config | None, notify: Notify | None = None) -> None:
        self.config = config
        self.notify = notify
        # The server is the authority on the operator's handle; config is the boot-time guess.
        self.operator = config.operator if config else "operator"
        ident = whoami()
        self.state = LocalState.load(ident.session_id or ident.handle)
        self.connected = False
        self.last_error: str | None = None
        self.halted: dict | None = None
        self.pushed = 0
        self.usage = UsageTracker()
        self._outbox: list[tuple[str, dict[str, str]]] = []
        self._flusher: asyncio.Task | None = None
        self._beat_lock = asyncio.Lock()  # the usage tracker's file offset must not be read twice at once
        self._http: httpx.AsyncClient | None = None
        self._tasks: list[asyncio.Task] = []

    @property
    def me(self) -> Identity:
        return whoami()

    def http(self) -> httpx.AsyncClient:
        if self.config is None:
            raise ConfigError("orchestra is not configured")
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self.config.url,
                headers={"Authorization": f"Bearer {self.config.token}"},
                timeout=httpx.Timeout(15.0, read=60.0),
            )
        return self._http

    # -- posting -------------------------------------------------------------

    async def post(self, body: dict) -> dict:
        """Post now, or spool for later. Returns the event or a 'queued' marker."""
        ident = self.me
        body = {"from": ident.handle, "session_id": ident.session_id,
                "client_id": secrets.token_hex(8), **body}
        try:
            r = await self.http().post("/api/events", json=body)
            if r.status_code in (502, 503, 504):  # mid-deploy: one patient retry before spooling
                await asyncio.sleep(3.0)
                r = await self.http().post("/api/events", json=body)  # client_id makes this idempotent
        except httpx.TransportError as exc:
            self.connected = False
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.state.spool(body)
            return {"queued": True, "client_id": body["client_id"],
                    "note": "bus unreachable; spooled and will be sent on reconnect"}
        if r.status_code >= 500:
            self.state.spool(body)
            return {"queued": True, "client_id": body["client_id"], "note": f"server error {r.status_code}; spooled"}
        if r.status_code >= 400:
            try:
                detail = r.json().get("detail")
            except Exception:
                detail = r.text
            raise ToolError(f"bus refused the post ({r.status_code}): {detail}")
        return r.json()

    async def flush_spool(self) -> int:
        pending = self.state.drain_spool()
        if not pending:
            return 0
        sent = 0
        for i, body in enumerate(pending):
            try:
                r = await self.http().post("/api/events", json=body)
            except httpx.TransportError:
                break
            if r.status_code >= 500:
                break
            sent = i + 1
            if r.status_code < 400:
                ev = r.json()
                for a in self.state.asks.values():
                    if a.client_id == body.get("client_id") and a.id is None:
                        a.id = ev["id"]
            else:
                print(f"orchestra-mcp: spooled post dropped ({r.status_code}): {r.text[:200]}", file=sys.stderr)
        rest = pending[sent:]
        self.state.spool_path.write_text("".join(json.dumps(b) + "\n" for b in rest))
        self.state.save()
        return sent

    async def get(self, path: str, **params: Any) -> dict:
        # A deploy restarts the server for ~3 s (the proxy answers 502 meanwhile): retry through it.
        for attempt, wait in enumerate((0.0, 1.0, 2.5, 4.0)):
            if wait:
                await asyncio.sleep(wait)
            try:
                r = await self.http().get(path, params=params)
            except httpx.TransportError as exc:
                if attempt == 3:
                    raise ToolError(f"bus unreachable ({type(exc).__name__}); try again shortly") from exc
                continue
            if r.status_code not in (502, 503, 504) or attempt == 3:
                break
        if r.status_code >= 400:
            try:
                detail = r.json().get("detail")
            except Exception:
                detail = r.text[:300]
            raise ToolError(f"bus refused ({r.status_code}): {detail}")
        return r.json()

    # -- background loops ----------------------------------------------------

    def start(self) -> None:
        if self.config is None:
            return
        loop = asyncio.get_running_loop()
        self._tasks = [
            loop.create_task(self._stream_forever()),
            loop.create_task(self._heartbeat_forever()),
            loop.create_task(self._deadline_watch_forever()),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if self._http is not None:
            await self._http.aclose()

    async def _push(self, content: str, meta: dict[str, str]) -> None:
        """Queue a push. Pushes landing within PUSH_BATCH_S of each other go out as one turn."""
        if self.notify is None:
            return
        self._outbox.append((content, meta))
        if self._flusher is None or self._flusher.done():
            self._flusher = asyncio.get_running_loop().create_task(self._flush_pushes())

    async def _flush_pushes(self) -> None:
        start = asyncio.get_running_loop().time()
        seen = -1
        while len(self._outbox) != seen and asyncio.get_running_loop().time() - start < PUSH_BATCH_MAX_S:
            seen = len(self._outbox)
            await asyncio.sleep(PUSH_BATCH_S)
        batch, self._outbox = self._outbox, []
        if not batch or self.notify is None:
            return
        if len(batch) == 1:
            content, meta = batch[0]
        else:
            content = f"{len(batch)} orchestra messages:\n\n" + "\n\n".join(f"[{i}] {c}" for i, (c, _) in enumerate(batch, 1))
            meta = {"source": "orchestra", "kind": "batch", "count": str(len(batch)),
                    "ids": ",".join(m.get("id", "") for _, m in batch if m.get("id"))}
        try:
            await self.notify(content, meta)
            self.pushed += len(batch)
        except Exception as exc:
            print(f"orchestra-mcp: channel push failed ({exc!r})", file=sys.stderr)

    async def _stream_forever(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._stream_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.last_error = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _stream_once(self) -> None:
        if self.state.cursor is None:
            # No local cursor: a fresh session id. If this handle was here before (a restart,
            # a resume under a new id), pick up where it left off so nothing addressed to it
            # is skipped; otherwise start at the head (history is for `read`).
            head = (await self.get("/api/events", after=10**12, limit=1))["head"]
            start = head
            try:
                prior = await self.get(f"/api/presence/{self.me.handle}")
                if isinstance(prior.get("cursor"), int) and prior["cursor"] <= head:
                    start = prior["cursor"]
            except ToolError:
                pass  # never seen: fresh start
            self.state.cursor = start
            self.state.read_cursor = max(self.state.read_cursor, start)
            self.state.save()
        async with self.http().stream("GET", "/api/stream", params={"after": self.state.cursor}) as r:
            r.raise_for_status()
            self.connected = True
            try:
                self.operator = (await self.get("/api/state")).get("operator") or self.operator
            except Exception:
                pass
            self.last_error = None
            await self.flush_spool()
            asyncio.get_running_loop().create_task(self._beat_once())  # report the cursor promptly
            data: list[str] = []
            # The stream opens with ": orchestra head=N": events up to N are backlog (what we
            # missed), and their pushes are held and delivered together once we reach N.
            backlog_head: int | None = None
            held: list[dict] = []
            async for line in r.aiter_lines():
                if line.startswith(": orchestra head="):
                    try:
                        backlog_head = int(line.split("=", 1)[1].split()[0])
                    except (ValueError, IndexError):
                        backlog_head = None
                    if backlog_head is not None and backlog_head <= (self.state.cursor or 0):
                        backlog_head = None  # nothing missed
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif line == "" and data:
                    raw, data = "\n".join(data), []
                    try:
                        ev = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    in_backlog = backlog_head is not None and int(ev.get("seq", 0)) <= backlog_head
                    await self._on_event(ev, hold=held if in_backlog else None)
                    if in_backlog and int(ev.get("seq", 0)) >= backlog_head:
                        await self._deliver_catchup(held)
                        held, backlog_head = [], None
                elif line.startswith(":") and backlog_head is not None:
                    # a ping before the backlog's last event arrived: flush what we have
                    await self._deliver_catchup(held)
                    held, backlog_head = [], None

    async def _deliver_catchup(self, held: list[dict]) -> None:
        if not held:
            return
        if len(held) <= CATCHUP_PUSH_LIMIT:
            for ev in held:
                await self._push_event(ev)
            return
        lines = [f"- #{e.get('seq')} {e.get('kind')} {e.get('id')} from {e.get('from')}"
                 + (f" re {e['ref']}" if e.get("ref") else "") + f": {_one_line(e.get('body'), 90)}" for e in held]
        await self._push(
            f"while you were away, {len(held)} messages for you arrived on the bus:\n" + "\n".join(lines[-15:])
            + ("\n(older ones omitted)" if len(lines) > 15 else "")
            + "\nRead them in full with read(); answers to your asks and any HALT/RESUME are among them.",
            {"source": "orchestra", "kind": "catchup", "count": str(len(held))},
        )

    async def _push_event(self, ev: dict) -> None:
        digest = ev.get("to") == "@all" and ev.get("from") != self.operator and ev.get("kind") in ("info", "request")
        await self._push(render(ev, self.operator) + ("" if digest else mention_hint(ev)), {
            "source": "orchestra", "kind": str(ev.get("kind")), "from": str(ev.get("from")),
            "id": str(ev.get("id")), "ref": str(ev.get("ref") or ""), "seq": str(ev.get("seq")),
        })

    def my_handles(self) -> set[str]:
        """The current handle plus earlier ones: after a rename, mail to the old name is still ours."""
        cur = self.me.handle
        if cur not in self.state.handles:
            self.state.handles.append(cur)
            self.state.save()
        return set(self.state.handles[-10:]) | {cur}

    def _mine(self, ev: dict) -> bool:
        if ev.get("from") in self.my_handles():
            return False
        if not self.state.peer_broadcasts and ev.get("to") == "@all" and ev.get("from") != self.operator:
            return False  # muted by this session: still on the bus for read()
        return any(is_for_me(ev, h, self.operator) for h in self.my_handles())

    async def _on_event(self, ev: dict, hold: list[dict] | None = None) -> None:
        self.state.cursor = max(self.state.cursor or 0, int(ev.get("seq", 0)))
        if ev.get("kind") in ("answer", "defaulted", "ack", "withdraw"):
            for a in self.state.asks.values():
                if a.id and a.id == ev.get("ref"):
                    a.resolved = ev.get("kind")
            self.state.asks = {k: v for k, v in self.state.asks.items() if v.resolved is None}
        mine = self._mine(ev)
        if ev.get("kind") == "halt" and mine:
            self.halted = ev
        if ev.get("kind") == "resume" and mine:
            self.halted = None
        self.state.save()
        if mine:
            if hold is not None:
                hold.append(ev)
            else:
                await self._push_event(ev)

    async def _beat_once(self) -> None:
        async with self._beat_lock:
            await self._beat_unlocked()

    async def _beat_unlocked(self) -> None:
        ident = self.me
        try:
            usage = await asyncio.to_thread(self.usage.poll, ident.session_id, ident.cwd)
        except Exception as exc:  # telemetry must never take the heartbeat down
            print(f"orchestra-mcp: usage read failed ({exc!r})", file=sys.stderr)
            usage = None
        try:
            await self.http().post("/api/presence", json={
                "handle": ident.handle, "session_id": ident.session_id, "name": ident.name,
                "cwd": ident.cwd, "status": ident.status, "host": ident.host,
                "channel": self.notify is not None,
                **({"usage": usage.wire()} if usage else {}),
                **({"bridge": ident.bridge} if ident.bridge else {}),
                **({"cursor": self.state.cursor} if self.state.cursor is not None else {}),
                "build": CLIENT_BUILD,
            })
        except Exception:
            pass  # the stream loop owns connectivity reporting

    async def _heartbeat_forever(self) -> None:
        while True:
            await self._beat_once()
            await asyncio.sleep(60)

    async def _deadline_watch_forever(self) -> None:
        """Take defaults locally if the server can't tell us (invariant 1, offline half)."""
        while True:
            await asyncio.sleep(20)
            t_now = datetime.now(UTC)
            changed = False
            for a in list(self.state.asks.values()):
                if a.resolved:
                    continue
                if datetime.fromisoformat(a.deadline) + LOCAL_DEFAULT_GRACE > t_now:
                    continue
                if self.connected and a.id:
                    try:
                        t = await self.get(f"/api/threads/{a.id}")
                        if t.get("status") != "open":
                            a.resolved = t["status"]
                            changed = True
                            continue
                    except Exception:
                        pass
                    else:
                        continue  # server is up and will sweep; don't race it
                a.resolved = "defaulted-locally"
                changed = True
                await self._push(
                    f"ask {a.id or a.client_id} passed its deadline and the bus is unreachable → "
                    f"take the DEFAULT locally: {a.default}\n(Question was: {a.question})",
                    {"source": "orchestra", "kind": "defaulted", "ref": a.id or a.client_id, "local": "true"},
                )
            if changed:
                self.state.asks = {k: v for k, v in self.state.asks.items() if v.resolved is None}
                self.state.save()

    # -- helpers for tools ---------------------------------------------------

    def remember_ask(self, ev: dict, question: str, default: str, deadline: datetime) -> None:
        cid = ev.get("client_id") or secrets.token_hex(8)
        self.state.asks[cid] = OpenAsk(client_id=cid, id=ev.get("id"), question=question,
                                       default=default, deadline=deadline.isoformat())
        self.state.save()
