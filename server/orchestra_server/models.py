"""Wire types. One event schema for everything on the bus."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

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


def slugify(name: str) -> str:
    """'steering and serving engine optimization' -> 'steering-and-serving-engine-optimization'."""
    slug = re.sub(r"[^a-z0-9._-]+", "-", name.lower()).strip("-._")
    return slug[:64] or "anon"


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
    # provenance, informational
    session_id: str | None = None
    client_id: str | None = Field(default=None, max_length=64, description="idempotency key for offline replays")

    @field_validator("to")
    @classmethod
    def _to(cls, v: str) -> str:
        if not v.startswith("@"):
            v = "@" + v
        if v != "@all" and not HANDLE_RE.match(v[1:]):
            raise ValueError(f"bad address {v!r}: use @all or @<handle>")
        return v


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
    session_id: str | None = None
    client_id: str | None = None

    def wire(self) -> dict:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


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
    status: ThreadStatus = "open"
    resolution: str | None = None
    resolved_at: datetime | None = None
    resolved_by: str | None = None

    model_config = ConfigDict(populate_by_name=True)

    def wire(self) -> dict:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


class PresenceIn(BaseModel):
    handle: str
    session_id: str | None = None
    name: str | None = None
    cwd: str | None = None
    status: str | None = None
    host: str | None = None
    channel: bool = False

    @field_validator("handle")
    @classmethod
    def _handle(cls, v: str) -> str:
        if not HANDLE_RE.match(v):
            raise ValueError(f"bad handle {v!r}")
        return v


class Presence(PresenceIn):
    last_seen: datetime


class LoginIn(BaseModel):
    token: str


class PushSubscription(BaseModel):
    endpoint: str
    keys: dict[str, str]
    expirationTime: float | None = None
