from __future__ import annotations

import json
import os
from pathlib import Path

from orchestra_mcp import identity
from orchestra_mcp.link import is_for_me, render


def test_push_filter():
    me = "builder"
    assert is_for_me({"from": "ada", "to": "@all", "kind": "notice"}, me, operator="ada")
    assert not is_for_me({"from": "mallory", "to": "@all", "kind": "notice"}, me, operator="ada")
    assert is_for_me({"from": "atlas", "to": "@builder", "kind": "status"}, me)
    assert is_for_me({"from": "atlas", "to": "@all", "kind": "info"}, me)
    assert not is_for_me({"from": "atlas", "to": "@all", "kind": "status"}, me)
    assert not is_for_me({"from": "atlas", "to": "@hermes", "kind": "info"}, me)
    assert not is_for_me({"from": me, "to": "@all", "kind": "info"}, me)


def test_render_defaulted_says_take_default():
    s = render({"kind": "defaulted", "from": "orchestra", "ref": "a1", "body": "keep preview", "to": "@x"})
    assert "DEFAULT" in s and "keep preview" in s


def test_identity_walks_parent_chain(tmp_path: Path, monkeypatch):
    # Our own parent chain includes this pytest's parent; plant a session file for it.
    ppid = os.getppid()
    (tmp_path / f"{ppid}.json").write_text(json.dumps(
        {"pid": ppid, "sessionId": "s-1", "cwd": "/x", "name": "Route Planning Service", "status": "busy"}))
    monkeypatch.setattr(identity, "SESSIONS_DIR", tmp_path)
    monkeypatch.delenv("ORCHESTRA_HANDLE", raising=False)
    me = identity.whoami()
    assert me.handle == "route-planning-service" and me.session_id == "s-1" and me.source == "session-registry"


def test_identity_finds_real_session():
    # Running under claude-code: the real registry should resolve us.
    if not any((Path.home() / ".claude" / "sessions").glob("*.json")):
        return
    me = identity.whoami()
    assert me.handle


def test_render_choice_and_clarify():
    s = render({"kind": "answer", "from": "ada", "ref": "a1", "choice": ["ship now"], "body": "after lunch"}, operator="ada")
    assert "chose 'ship now'" in s and "after lunch" in s
    s = render({"kind": "reply", "from": "ada", "ref": "a1", "body": "what about the migration?"}, operator="ada")
    assert "post(kind='reply', ref='a1'" in s and "stays open" in s


def test_render_excerpt_and_peer_digest():
    s = render({"kind": "ack", "from": "ada", "ref": "n1", "ref_excerpt": "standard-1b is on main", "to": "@me"}, operator="ada")
    assert "your 'standard-1b is on main'" in s
    long = "GPU plan for tonight\n" + "detail " * 300
    s = render({"kind": "info", "from": "peer", "to": "@all", "id": "m9", "body": long}, operator="ada")
    assert s.startswith("info m9 from peer") and "get('m9')" in s and len(s) < 300
    s = render({"kind": "notice", "from": "ada", "to": "@all", "id": "m8", "body": long}, operator="ada")
    assert "detail detail" in s  # the operator's broadcasts arrive whole


def test_pushes_batch(monkeypatch):
    import asyncio
    from orchestra_mcp import link as L
    monkeypatch.setattr(L, "PUSH_BATCH_S", 0.05)
    got = []
    async def notify(c, m): got.append((c, m))
    async def go():
        lk = L.Link(None, notify)
        for i in range(3):
            await lk._push(f"msg {i}", {"id": f"m{i}"})
        await asyncio.sleep(0.3)
        await lk._push("later", {"id": "m9"})
        await asyncio.sleep(0.3)
    asyncio.run(go())
    assert len(got) == 2 and got[0][1]["kind"] == "batch" and got[0][1]["ids"] == "m0,m1,m2" and "[3] msg 2" in got[0][0]
    assert got[1][0] == "later"


def test_catchup_summary_keeps_one_line_per_message():
    import asyncio
    from orchestra_mcp import link as L
    got = []
    async def notify(c, m): got.append(c)
    async def go():
        lk = L.Link(None, notify)
        held = [{"seq": i, "kind": "info", "id": f"m{i}", "from": "peer", "body": "GPU plan:\n- Now: node1\n- Later: node2"} for i in range(7)]
        await lk._deliver_catchup(held)
        await asyncio.sleep(L.PUSH_BATCH_S + 0.3)
    asyncio.run(go())
    lines = got[0].splitlines()
    assert sum(1 for l in lines if l.startswith("- #")) == 7 and not any(l.startswith("- Now") for l in lines)


def test_mention_hint_and_old_handles(tmp_path, monkeypatch):
    from orchestra_mcp import link as L
    h = L.mention_hint({"id": "m111111", "ref": "r222222", "body": "read n3a2e52 and m4958c9, not r222222"})
    assert "n3a2e52, m4958c9" in h and "r222222" not in h
    assert L.mention_hint({"body": "no ids here, just 'abcdefg'"}) == ""
    monkeypatch.setattr(L, "STATE_ROOT", tmp_path)
    monkeypatch.setenv("ORCHESTRA_HANDLE", "scout-82")
    lk = L.Link(None, None)
    assert lk.my_handles() == {"scout-82"}
    monkeypatch.setenv("ORCHESTRA_HANDLE", "scout-ops")
    assert lk.my_handles() == {"scout-82", "scout-ops"}
    assert lk._mine({"from": "operator", "to": "@scout-82", "kind": "notice"})
    assert not lk._mine({"from": "scout-82", "to": "@all", "kind": "info"})  # my own old-name posts aren't pushed to me


def test_get_retries_through_a_deploy(monkeypatch, tmp_path):
    import asyncio
    import httpx
    from orchestra_mcp import link as L
    monkeypatch.setattr(L, "STATE_ROOT", tmp_path)
    calls = {"n": 0}
    def handler(req):
        calls["n"] += 1
        return httpx.Response(502) if calls["n"] < 3 else httpx.Response(200, json={"ok": True})
    lk = L.Link(L.Config(url="http://bus", token="t"), None)
    lk._http = httpx.AsyncClient(base_url="http://bus", transport=httpx.MockTransport(handler))
    real_sleep = asyncio.sleep
    monkeypatch.setattr(L.asyncio, "sleep", lambda s: real_sleep(0))
    assert asyncio.run(lk.get("/api/state")) == {"ok": True} and calls["n"] == 3


def test_peer_broadcast_mute(tmp_path, monkeypatch):
    from orchestra_mcp import link as L
    monkeypatch.setattr(L, "STATE_ROOT", tmp_path)
    monkeypatch.setenv("ORCHESTRA_HANDLE", "quiet-one")
    lk = L.Link(L.Config(url="http://x", token="t", operator="ada"), None)
    peer = {"from": "peer", "to": "@all", "kind": "info", "body": "lane news"}
    assert lk._mine(peer)
    lk.state.peer_broadcasts = False
    assert not lk._mine(peer)
    assert lk._mine({"from": "peer", "to": "@quiet-one", "kind": "info"})       # direct still pushes
    assert lk._mine({"from": "ada", "to": "@all", "kind": "notice"})            # operator still pushes


def test_multi_address_is_direct_even_when_muted(tmp_path, monkeypatch):
    from orchestra_mcp import link as L
    assert L.is_for_me({"from": "peer", "to": "@a,@me,@b", "kind": "status"}, "me")
    assert not L.is_for_me({"from": "peer", "to": "@a,@b", "kind": "info"}, "me")
    assert not L.is_for_me({"from": "peer", "to": "@meow", "kind": "info"}, "me")  # no substring matches
    monkeypatch.setattr(L, "STATE_ROOT", tmp_path)
    monkeypatch.setenv("ORCHESTRA_HANDLE", "me")
    lk = L.Link(L.Config(url="http://x", token="t", operator="ada"), None)
    lk.state.peer_broadcasts = False
    assert lk._mine({"from": "peer", "to": "@a,@me", "kind": "info"})


def test_read_handoff(tmp_path, monkeypatch):
    from orchestra_mcp import link as L
    monkeypatch.setattr(L, "CONTINUITY_DIR", tmp_path)
    d = tmp_path / "sid-1"; d.mkdir()
    (d / "latest.md").write_text("<!-- continuity handoff, before a manual compaction, 2026-10-29T04:20:10.731Z; transcript: /home/x/t.jsonl -->\n\n## Where we are\nlantern, teal\n")
    h = L.read_handoff("sid-1", 0.0)
    assert h["why"] == "before a manual compaction" and h["written_at"] == "2026-10-29T04:20:10.731Z"
    assert h["text"].startswith("## Where we are") and "/home/x" not in h["text"]  # local transcript path stripped
    assert L.read_handoff("sid-1", h["mtime"]) is None  # already sent
    assert L.read_handoff("nope", 0.0) is None and L.read_handoff(None, 0.0) is None
    (d / "latest.md").write_text("<!-- continuity handoff, on request, from the transcript text, 2026-10-29T08:35:50.210Z; transcript: /home/x/t.jsonl -->\n\n## Where we are\nx\n")
    h = L.read_handoff("sid-1", 0.0)
    assert h["why"] == "on request, from the transcript text" and h["text"].startswith("## Where") and "/home/x" not in h["text"]


def test_seen_never_wakes_a_session(monkeypatch):
    from orchestra_mcp.link import Link
    import types
    lk = Link.__new__(Link)
    lk.state = types.SimpleNamespace(handles=["scout"], peer_broadcasts=True, save=lambda: None)
    lk.operator = "ada"
    monkeypatch.setattr(Link, "me", property(lambda self: types.SimpleNamespace(handle="scout")))
    assert lk._mine({"from": "ada", "to": "@scout", "kind": "ack", "ref": "n1"})
    assert not lk._mine({"from": "ada", "to": "@scout", "kind": "seen", "ref": "n1"})
    assert not lk._mine({"from": "ada", "to": "@ada", "kind": "seen", "ref": "n1"})


def test_seen_marks_ride_along_with_the_next_wake(tmp_path, monkeypatch):
    import asyncio, types
    from orchestra_mcp import link as L
    monkeypatch.setattr(L, "STATE_ROOT", tmp_path)
    monkeypatch.setattr(L, "PUSH_BATCH_S", 0.05)
    monkeypatch.setattr(L.Link, "me", property(lambda self: types.SimpleNamespace(handle="scout", session_id="sid-s")))
    got = []
    async def notify(c, m): got.append(c)
    async def go():
        lk = L.Link(None, notify)
        lk.operator = "ada"
        seen = lambda ref, ex: {"seq": 1, "id": "m1" + ref, "kind": "seen", "from": "ada", "to": "@ada", "ref": ref, "ref_from": "scout", "ref_excerpt": ex}
        await lk._on_event(seen("n111111", "fyi: eval done"))
        await lk._on_event(seen("r222222", "which split?"))
        await lk._on_event(seen("r333333", "deploy now?"))
        await lk._on_event({**seen("r444444", "not mine"), "ref_from": "atlas"})
        await asyncio.sleep(0.2)
        assert got == []  # seen alone wakes nobody
        # the operator answers r333333 and sends a notice: one push, the seen marks along, minus the answered one
        await lk._on_event({"seq": 5, "id": "m5", "kind": "answer", "from": "ada", "to": "@scout", "ref": "r333333", "body": "yes"})
        await asyncio.sleep(L.PUSH_BATCH_S + 0.3)
        assert len(got) == 1
        assert "n111111 note 'fyi: eval done' (closed)" in got[0] and "r222222 request 'which split?' (still open" in got[0]
        assert "r333333 request" not in got[0] and "r444444" not in got[0]
        assert lk.state.seen_pending == []  # told once
        await lk._on_event({"seq": 6, "id": "m6", "kind": "notice", "from": "ada", "to": "@scout", "body": "next"})
        await asyncio.sleep(L.PUSH_BATCH_S + 0.3)
        assert "marked these" not in got[1]
    asyncio.run(go())
