"""HTTP surface: the bus API (for sessions), the inbox page (for the operator), push."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import secrets
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from .bus import Bus, now
from .models import AGENT_KINDS, CLOSING_KINDS, HANDLE_RE, OPERATOR_KINDS, SYSTEM, LoginIn, PostIn, PresenceIn, PushSubscription
from .push import Pusher

STATIC = Path(__file__).parent / "static"
COOKIE = "orc"
Principal = Literal["operator", "agent"]


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    agent_token: str
    operator_token: str
    operator: str = "operator"
    vapid_sub: str = "mailto:orchestra@example.com"
    digest_delay_s: float = 180.0
    sweep_interval_s: float = 10.0
    cookie_secure: bool = True
    max_ask_minutes: int = 7 * 24 * 60

    @classmethod
    def from_env(cls) -> Settings:
        data = Path(os.environ.get("ORCHESTRA_DATA", "./data")).resolve()
        data.mkdir(parents=True, exist_ok=True)
        tokens_path = data / "tokens.json"
        agent = os.environ.get("ORCHESTRA_AGENT_TOKEN")
        op_tok = os.environ.get("ORCHESTRA_OPERATOR_TOKEN")
        if not (agent and op_tok):
            # First boot: mint both and keep them beside the bus (0600).
            if tokens_path.exists():
                saved = json.loads(tokens_path.read_text())
            else:
                saved = {"agent": secrets.token_urlsafe(32), "operator": secrets.token_urlsafe(32)}
                tokens_path.write_text(json.dumps(saved, indent=1))
                tokens_path.chmod(0o600)
                print(f"orchestra: minted tokens in {tokens_path}", file=sys.stderr)
            agent = agent or saved["agent"]
            op_tok = op_tok or saved["operator"]
        if secrets.compare_digest(agent, op_tok):
            raise SystemExit("orchestra: agent and operator tokens must differ")
        operator = os.environ.get("ORCHESTRA_OPERATOR", "operator").lower()
        if not HANDLE_RE.match(operator) or operator in (SYSTEM, "all"):
            raise SystemExit(f"orchestra: bad ORCHESTRA_OPERATOR {operator!r}")
        return cls(
            data_dir=data, agent_token=agent, operator_token=op_tok, operator=operator,
            vapid_sub=os.environ.get("ORCHESTRA_VAPID_SUB", "mailto:orchestra@example.com"),
            digest_delay_s=float(os.environ.get("ORCHESTRA_DIGEST_S", "180")),
            cookie_secure=os.environ.get("ORCHESTRA_COOKIE_SECURE", "1") != "0",
        )


def create_app(settings: Settings) -> FastAPI:
    bus = Bus(settings.data_dir / "bus.jsonl", operator=settings.operator)
    pusher = Pusher(settings.data_dir, digest_delay_s=settings.digest_delay_s,
                    operator=settings.operator, vapid_sub=settings.vapid_sub)
    bus.on_append.append(pusher.on_event)

    async def sweeper() -> None:
        while True:
            try:
                await bus.sweep_deadlines()
            except Exception as exc:
                print(f"orchestra: deadline sweep failed: {exc!r}", file=sys.stderr)
            await asyncio.sleep(settings.sweep_interval_s)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(sweeper())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="orchestra", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.bus = bus
    app.state.pusher = pusher
    app.state.settings = settings

    # -- auth ---------------------------------------------------------------

    def principal(request: Request) -> Principal:
        token = None
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
        token = token or request.cookies.get(COOKIE)
        if token and secrets.compare_digest(token, settings.operator_token):
            return "operator"
        if token and secrets.compare_digest(token, settings.agent_token):
            return "agent"
        raise HTTPException(401, "missing or bad token")

    def operator_only(who: Principal = Depends(principal)) -> Principal:
        if who != "operator":
            raise HTTPException(403, "operator only")
        return who

    @app.post("/api/login")
    async def login(body: LoginIn, response: Response) -> dict:
        if not secrets.compare_digest(body.token, settings.operator_token):
            raise HTTPException(401, "bad token")
        response.set_cookie(
            COOKIE, body.token, max_age=365 * 24 * 3600, httponly=True,
            secure=settings.cookie_secure, samesite="strict",
        )
        return {"ok": True}

    @app.post("/api/logout")
    async def logout(response: Response) -> dict:
        response.delete_cookie(COOKIE)
        return {"ok": True}

    # -- bus ----------------------------------------------------------------

    def check_post(post: PostIn, who: Principal) -> PostIn:
        if who == "operator":
            post.from_ = settings.operator
            if post.kind not in OPERATOR_KINDS:
                raise HTTPException(422, f"the operator posts {sorted(OPERATOR_KINDS)}, not {post.kind!r}")
        else:
            if post.from_ in (settings.operator, SYSTEM, "all") or not HANDLE_RE.match(post.from_):
                raise HTTPException(422, f"bad sender handle {post.from_!r}")
            if post.kind not in AGENT_KINDS | {"answer"}:
                raise HTTPException(403, f"sessions may not post {post.kind!r}")
        if post.kind in CLOSING_KINDS:
            thread = bus.threads.get(post.ref or "")
            if thread is None:
                raise HTTPException(422, f"{post.kind} needs ref= an existing ask/request/note id")
            if thread.status != "open" and not (thread.to == "@all" and post.kind == "answer"):
                raise HTTPException(409, f"{thread.id} is already {thread.status}")
            if who == "agent":
                if post.kind == "withdraw" and thread.from_ != post.from_:
                    raise HTTPException(403, "only the opener may withdraw a thread")
                if post.kind == "answer" and thread.to not in (f"@{post.from_}", "@all"):
                    raise HTTPException(403, f"{thread.id} is addressed to {thread.to}, not you; use kind=reply to comment")
        if post.kind == "ask":
            if post.deadline is None or post.default is None:
                raise HTTPException(422, "an ask needs default and deadline (invariant 1: nothing hard-blocks on a human)")
            if post.deadline.tzinfo is None:
                raise HTTPException(422, "deadline must carry a timezone")
            if post.deadline > now() + timedelta(minutes=settings.max_ask_minutes):
                raise HTTPException(422, "deadline too far out")
        return post

    @app.post("/api/events")
    async def post_event(post: PostIn, who: Principal = Depends(principal)) -> dict:
        ev = await bus.append(check_post(post, who))
        return ev.wire()

    @app.get("/api/events")
    async def list_events(after: int = 0, limit: int = 200, _: Principal = Depends(principal)) -> dict:
        evs = bus.since(after, limit=min(max(limit, 1), 2000))
        return {"head": bus.head, "events": [e.wire() for e in evs]}

    @app.get("/api/stream")
    async def stream(request: Request, after: int | None = None, _: Principal = Depends(principal)) -> StreamingResponse:
        if after is None:
            last_id = request.headers.get("last-event-id")
            after = int(last_id) if last_id and last_id.isdigit() else bus.head

        async def gen() -> AsyncIterator[bytes]:
            q = bus.subscribe(after)
            try:
                yield f": orchestra head={bus.head}\nretry: 3000\n\n".encode()
                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        ev = await asyncio.wait_for(q.get(), timeout=15)
                    except TimeoutError:
                        yield b": ping\n\n"
                        continue
                    yield f"id: {ev.seq}\nevent: bus\ndata: {json.dumps(ev.wire(), ensure_ascii=False)}\n\n".encode()
            finally:
                bus.unsubscribe(q)

        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    @app.get("/api/search")
    async def search(q: str, limit: int = 200, _: Principal = Depends(principal)) -> dict:
        evs = bus.search(q[:200], limit=min(max(limit, 1), 1000))
        return {"q": q, "count": len(evs), "events": [e.wire() for e in evs]}

    @app.get("/api/threads/{thread_id}")
    async def get_thread(thread_id: str, _: Principal = Depends(principal)) -> dict:
        t = bus.threads.get(thread_id)
        if t is None:
            raise HTTPException(404, "no such thread")
        return t.wire()

    @app.get("/api/state")
    async def state(who: Principal = Depends(principal)) -> dict:
        return {
            "head": bus.head, "you": who, "operator": settings.operator, "inbox": bus.inbox(),
            "presence": bus.live_presence(),
            "push": {"public_key": pusher.public_key, "subscriptions": len(pusher.subs)} if who == "operator" else None,
        }

    @app.post("/api/presence")
    async def presence(p: PresenceIn, who: Principal = Depends(principal)) -> dict:
        if who != "agent":
            raise HTTPException(403, "presence is for sessions")
        bus.touch(p)
        return {"ok": True, "head": bus.head}

    # -- push ---------------------------------------------------------------

    @app.post("/api/push/subscribe")
    async def push_subscribe(sub: PushSubscription, _: Principal = Depends(operator_only)) -> dict:
        pusher.subscribe(sub)
        return {"ok": True, "subscriptions": len(pusher.subs)}

    @app.post("/api/push/unsubscribe")
    async def push_unsubscribe(sub: PushSubscription, _: Principal = Depends(operator_only)) -> dict:
        pusher.unsubscribe(sub.endpoint)
        return {"ok": True}

    @app.post("/api/push/test")
    async def push_test(_: Principal = Depends(operator_only)) -> dict:
        n = await pusher.send({"title": "orchestra", "body": "push works 🔥", "tag": "test", "kind": "test", "url": "/"}, urgency="high")
        return {"delivered": n}

    # -- page ---------------------------------------------------------------

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/sw.js")
    async def sw() -> FileResponse:
        return FileResponse(STATIC / "sw.js", media_type="text/javascript",
                            headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})

    @app.get("/manifest.webmanifest")
    async def manifest() -> FileResponse:
        return FileResponse(STATIC / "manifest.webmanifest", media_type="application/manifest+json")

    @app.get("/icon.svg")
    async def icon() -> FileResponse:
        return FileResponse(STATIC / "icon.svg", media_type="image/svg+xml")

    # Optional per-deployment branding: <data>/brand/brand.css (+ any fonts/images it
    # references as /brand/<file>). Nothing ships in the repo; absent means the default look.
    brand_dir = (settings.data_dir / "brand").resolve()

    @app.get("/brand.css")
    async def brand_css() -> Response:
        f = brand_dir / "brand.css"
        if f.is_file():
            return FileResponse(f, media_type="text/css", headers={"Cache-Control": "no-cache"})
        return Response("/* no brand.css */\n", media_type="text/css", headers={"Cache-Control": "no-cache"})

    @app.get("/brand/{name}")
    async def brand_asset(name: str) -> FileResponse:
        f = (brand_dir / name).resolve()
        if f.parent != brand_dir or not f.is_file() or name.startswith("."):
            raise HTTPException(404)
        return FileResponse(f, headers={"Cache-Control": "public, max-age=604800"})

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"ok": True, "head": bus.head})

    return app


def main() -> None:
    import uvicorn

    settings = Settings.from_env()
    host = os.environ.get("ORCHESTRA_HOST", "127.0.0.1")
    port = int(os.environ.get("ORCHESTRA_PORT", "7460"))
    # SSE streams never close on their own, so graceful shutdown must be bounded or every
    # restart waits out systemd's stop timeout. Clients reconnect (EventSource retry, mcp backoff).
    uvicorn.run(create_app(settings), host=host, port=port, workers=1, log_level="info",
                timeout_graceful_shutdown=3)


if __name__ == "__main__":
    main()
