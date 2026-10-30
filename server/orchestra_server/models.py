"""Wire types. One event schema for everything on the bus."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SYSTEM = "orchestra"

# Kinds a session may post.
AgentKind = Literal["hello", "goodbye", "status", "info", "reply", "request", "ask", "note", "withdraw"]
# Kinds only the operator (or the server) may post.
OperatorKind = Literal["notice", "answer", "ack", "halt", "resume"]
SystemKind = Literal["defaulted"]
Kind = Literal[AgentKind, OperatorKind, SystemKind]

AGENT_KINDS: frozenset[str] = frozenset(AgentKind.__args__)  # type: ignore[attr-defined]
OPERATOR_KINDS: frozenset[str] = frozenset(OperatorKind.__args__) | {"info", "reply", "request"}  # type: ignore[attr-defined]

# Things that open a thread in the operator's inbox. A peer `request` (to @session)
# is a thread too, but only one addressed to the operator lands in their inbox.
THREAD_KINDS: frozenset[str] = frozenset({"ask", "request", "note"})
# Things that close one.
CLOSING_KINDS: frozenset[str] = frozenset({"answer", "ack", "defaulted", "withdraw"})

HANDLE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def recipients(to: str) -> list[str]:
    """'@a,@b' -> ['@a', '@b']; '@all' -> [] (everyone, not a list)."""
    return [] if to == "@all" else [r for r in to.split(",") if r]


def is_to(to: str, handle: str) -> bool:
    """Is `handle` one of the named recipients (not merely covered by @all)?"""
    return f"@{handle}" in recipients(to)


def is_group(to: str) -> bool:
    """@all or several named recipients: a request to a group collects one answer per member."""
    return to == "@all" or "," in to


def slugify(name: str) -> str:
    """'Route Planning Service' -> 'route-planning-service'."""
    slug = re.sub(r"[^a-z0-9._-]+", "-", name.lower()).strip("-._")
    return slug[:64] or "anon"


class Attachment(BaseModel):
    """A file a message carries, named by its content hash. The bytes live in the file store, not the bus."""

    model_config = ConfigDict(extra="forbid")

    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    name: str = Field(min_length=1, max_length=200)
    size: int = Field(ge=0)
    mime: str = Field(default="application/octet-stream", max_length=100)


class Option(BaseModel):
    """One presented choice on an ask/request (AskUserQuestion-style)."""

    model_config = ConfigDict(extra="forbid")

    label: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1_000)
    recommended: bool = False


class PostIn(BaseModel):
    """What a client sends. The server stamps seq/id/ts and checks `from`."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    from_: str = Field(alias="from")
    to: str = "@all"
    kind: Kind
    body: str = Field(default="", max_length=16_384)
    topic: str | None = Field(default=None, max_length=120)
    ref: str | None = None
    # ask-only fields
    default: str | None = Field(default=None, max_length=2_000)
    deadline: datetime | None = None
    revert_cost: str | None = Field(default=None, max_length=500)
    # presented choices (ask/request), and the pick (answer)
    options: list[Option] | None = Field(default=None, min_length=2, max_length=8)
    multi: bool = False
    context: str | None = Field(default=None, max_length=4_000)
    choice: list[str] | None = Field(default=None, max_length=8)
    attachments: list[Attachment] | None = Field(default=None, max_length=8)
    # provenance, informational
    session_id: str | None = None
    client_id: str | None = Field(default=None, max_length=64, description="idempotency key for offline replays")

    @model_validator(mode="after")
    def _choices(self) -> PostIn:
        if self.options is not None:
            if self.kind not in ("ask", "request"):
                raise ValueError("options only go on an ask or a request")
            labels = [o.label for o in self.options]
            if len(set(labels)) != len(labels):
                raise ValueError("option labels must be unique")
            if self.kind == "ask" and self.default not in labels:
                raise ValueError(f"an ask with options needs default = one of {labels}")
        if self.choice is not None and self.kind != "answer":
            raise ValueError("choice only goes on an answer")
        return self

    @field_validator("to")
    @classmethod
    def _to(cls, v: str) -> str:
        parts = [p for p in re.split(r"[,\s]+", v.strip()) if p]
        if not parts:
            raise ValueError("empty address")
        out: list[str] = []
        for p in parts:
            p = p if p.startswith("@") else "@" + p
            if p != "@all" and not HANDLE_RE.match(p[1:]):
                raise ValueError(f"bad address {p!r}: use @all, @<handle>, or several handles separated by commas")
            if p not in out:
                out.append(p)
        if "@all" in out and len(out) > 1:
            raise ValueError("@all already includes everyone; don't combine it with handles")
        if len(out) > 32:
            raise ValueError("at most 32 recipients")
        return ",".join(out)


class Event(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    seq: int
    id: str
    ts: datetime
    from_: str = Field(alias="from")
    to: str
    kind: Kind
    body: str = ""
    topic: str | None = None
    ref: str | None = None
    default: str | None = None
    deadline: datetime | None = None
    revert_cost: str | None = None
    options: list[Option] | None = None
    multi: bool = False
    context: str | None = None
    choice: list[str] | None = None
    attachments: list[Attachment] | None = None
    ref_excerpt: str | None = None  # first line of the thread this closes/replies to, so a push stands alone
    session_id: str | None = None
    client_id: str | None = None

    def wire(self) -> dict:
        d = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        if not d.get("multi"):
            d.pop("multi", None)  # keep plain events plain
        return d


ThreadStatus = Literal["open", "answered", "acked", "defaulted", "withdrawn"]


class Thread(BaseModel):
    """An ask/request/note and how it ended, derived by replaying the bus."""

    id: str
    kind: Literal["ask", "request", "note"]
    from_: str = Field(alias="from")
    to: str
    ts: datetime
    body: str
    topic: str | None = None
    default: str | None = None
    deadline: datetime | None = None
    revert_cost: str | None = None
    options: list[Option] | None = None
    multi: bool = False
    context: str | None = None
    choice: list[str] | None = None  # what was picked, when answered with options
    attachments: list[Attachment] | None = None
    status: ThreadStatus = "open"
    resolution: str | None = None
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    answers: list[dict] = Field(default_factory=list)  # broadcast answers + every reply (the discussion)

    model_config = ConfigDict(populate_by_name=True)

    def wire(self) -> dict:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


class UsageIn(BaseModel):
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    thinking: int = 0
    turns: int = 0
    context: int = 0
    model: str | None = Field(default=None, max_length=80)
    last_ts: str | None = Field(default=None, max_length=40)


class CostUsage(BaseModel):
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cache_read_input_tokens: int = Field(default=0, ge=0)
    cache_creation_input_tokens: int = Field(default=0, ge=0)


class CostRow(BaseModel):
    """One model request continuity made (a note's fork, its transcript fallback, a compaction's summary), from costs.jsonl."""

    ts: datetime
    what: Literal["note", "note-transcript", "compaction"]
    why: str | None = Field(default=None, max_length=120)
    model: str | None = Field(default=None, max_length=80)
    usage: CostUsage


class PresenceIn(BaseModel):
    handle: str
    session_id: str | None = None
    name: str | None = None
    cwd: str | None = None
    status: str | None = None
    host: str | None = None
    channel: bool = False
    usage: UsageIn | None = None
    continuity: list[CostRow] | None = Field(default=None, max_length=200, description="continuity's own model requests, newest last")
    cursor: int | None = Field(default=None, ge=0, description="last bus seq this session has taken in")
    build: str | None = Field(default=None, pattern=r"^[0-9a-f]{6,16}$", description="client source hash")
    bridge: str | None = Field(default=None, pattern=r"^session_[A-Za-z0-9]{8,64}$",
                               description="Remote Control session id: lets the operator open it in the Claude app")

    @field_validator("handle")
    @classmethod
    def _handle(cls, v: str) -> str:
        if not HANDLE_RE.match(v):
            raise ValueError(f"bad handle {v!r}")
        return v


class Presence(PresenceIn):
    last_seen: datetime
    renamed_to: str | None = None  # this handle's session now goes by another name
    renamed_from: list[str] = Field(default_factory=list)  # earlier handles of this session


class LoginIn(BaseModel):
    token: str


class PushSubscription(BaseModel):
    endpoint: str
    keys: dict[str, str]
    expirationTime: float | None = None
