"""Phone push via Web Push (VAPID). No third party sees plaintext: payloads
are encrypted end-to-end to the subscribed browser.

Tiering (ORCHESTRA.md §5, trimmed for v0):
  ask -> @operator      immediate, high urgency, with a "take default" action
  request/note/defaulted/answer  batched into one high-urgency digest push after DIGEST_DELAY_S
  everything else        never leaves the page
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
from pathlib import Path

from .models import Event, PushSubscription

try:
    from cryptography.hazmat.primitives import serialization
    from py_vapid import Vapid
    from pywebpush import WebPushException, webpush

    PUSH_AVAILABLE = True
except ImportError as exc:  # pragma: no cover - deps are declared; this is belt and braces
    print(f"orchestra: web push disabled ({exc})", file=sys.stderr)
    PUSH_AVAILABLE = False



class Pusher:
    def __init__(
        self, data_dir: Path, digest_delay_s: float = 180.0, base_url: str = "/",
        operator: str = "operator", vapid_sub: str = "mailto:orchestra@example.com",
    ) -> None:
        self.operator = operator
        self.vapid_sub = vapid_sub
        self.data_dir = data_dir
        self.key_path = data_dir / "vapid_private.pem"
        self.subs_path = data_dir / "push_subscriptions.json"
        self.digest_delay_s = digest_delay_s
        self.base_url = base_url
        self.subs: list[PushSubscription] = []
        self.public_key: str | None = None
        self._pending: list[Event] = []
        self._digest_task: asyncio.Task[None] | None = None
        self.sent: list[dict] = []  # last few payloads, for /api/push/test and tests
        if PUSH_AVAILABLE:
            self._load_keys()
            self._load_subs()

    # -- keys & subscriptions ----------------------------------------------

    def _load_keys(self) -> None:
        if not self.key_path.exists():
            v = Vapid()
            v.generate_keys()
            v.save_key(str(self.key_path))
            self.key_path.chmod(0o600)
        v = Vapid.from_file(str(self.key_path))
        raw = v.public_key.public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        self.public_key = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    def _load_subs(self) -> None:
        if self.subs_path.exists():
            try:
                self.subs = [PushSubscription.model_validate(s) for s in json.loads(self.subs_path.read_text())]
            except Exception as exc:
                print(f"orchestra: could not read push subscriptions ({exc}); starting empty", file=sys.stderr)
                self.subs = []

    def _save_subs(self) -> None:
        tmp = self.subs_path.with_suffix(".tmp")
        tmp.write_text(json.dumps([s.model_dump() for s in self.subs], indent=1))
        tmp.chmod(0o600)
        tmp.replace(self.subs_path)

    def subscribe(self, sub: PushSubscription) -> None:
        self.subs = [s for s in self.subs if s.endpoint != sub.endpoint] + [sub]
        self._save_subs()

    def unsubscribe(self, endpoint: str) -> None:
        self.subs = [s for s in self.subs if s.endpoint != endpoint]
        self._save_subs()

    # -- policy -------------------------------------------------------------

    def on_event(self, ev: Event) -> None:
        """Sync hook called by the bus after every append."""
        if ev.to != f"@{self.operator}" and ev.kind != "defaulted":
            return
        if ev.kind == "ask":
            when = f" · default at {ev.deadline:%H:%M}Z" if ev.deadline else ""
            self._spawn(self.send({
                "title": f"⏳ {ev.from_} asks",
                "body": f"{ev.body}\n"
                        + ("".join(f"{'★' if o.recommended else '·'} {o.label}\n" for o in ev.options) if ev.options else "")
                        + f"→ default: {ev.default or '—'}{when}",
                "tag": ev.id, "thread": ev.id, "kind": "ask", "default": ev.default or "", "options": bool(ev.options),
                "url": f"{self.base_url}#t={ev.id}",
                "requireInteraction": True,
                "actions": [{"action": "default", "title": "Take default"}, {"action": "open", "title": "Open"}],
            }, urgency="high"))
        elif ev.kind in ("request", "note", "defaulted", "answer"):
            self._pending.append(ev)
            if self._digest_task is None or self._digest_task.done():
                self._digest_task = self._spawn(self._digest_later())

    async def _digest_later(self) -> None:
        print(f"orchestra: digest push scheduled in {self.digest_delay_s:.0f}s", file=sys.stderr, flush=True)
        await asyncio.sleep(self.digest_delay_s)
        batch, self._pending = self._pending, []
        if not batch:
            return
        counts: dict[str, int] = {}
        for ev in batch:
            counts[ev.kind] = counts.get(ev.kind, 0) + 1
        label = {"request": "request", "note": "note", "defaulted": "ask defaulted", "answer": "answer"}
        summary = ", ".join(f"{n} {label[k]}{'s' if n > 1 and k != 'defaulted' else ''}" for k, n in counts.items())
        lines = [f"{ev.from_ if ev.kind != 'defaulted' else ev.to[1:]}: {ev.body[:80]}" for ev in batch[-4:]]
        await self.send({
            "title": f"orchestra · {summary}", "body": "\n".join(lines),
            "tag": "digest", "kind": "digest", "url": self.base_url,
        }, urgency="high")  # "normal" lets Android Doze hold it until the phone wakes; digests are rare by design

    def _spawn(self, coro) -> asyncio.Task | None:
        try:
            return asyncio.get_running_loop().create_task(coro)
        except RuntimeError:
            coro.close()  # no loop (sync test context): drop silently
            return None

    # -- delivery -----------------------------------------------------------

    async def send(self, payload: dict, urgency: str = "normal") -> int:
        self.sent = (self.sent + [payload])[-20:]
        if not PUSH_AVAILABLE or not self.subs:
            return 0
        delivered = 0
        dead: list[str] = []
        for sub in list(self.subs):
            try:
                await asyncio.to_thread(
                    webpush,
                    subscription_info=sub.model_dump(exclude_none=True),
                    data=json.dumps(payload),
                    vapid_private_key=str(self.key_path),
                    vapid_claims={"sub": self.vapid_sub},
                    ttl=24 * 3600,
                    headers={"Urgency": urgency},
                )
                delivered += 1
            except WebPushException as exc:
                status = getattr(exc.response, "status_code", None)
                if status in (404, 410):
                    dead.append(sub.endpoint)
                else:
                    print(f"orchestra: push failed ({status}): {exc}", file=sys.stderr)
            except Exception as exc:
                print(f"orchestra: push failed: {exc!r}", file=sys.stderr)
        for ep in dead:
            self.unsubscribe(ep)
        print(f"orchestra: push {payload.get('kind')} {payload.get('tag')!r} -> {delivered}/{len(self.subs) + len(dead)} device(s)"
              + (f", {len(dead)} expired and dropped" if dead else ""), file=sys.stderr, flush=True)
        return delivered
