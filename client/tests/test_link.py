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
        {"pid": ppid, "sessionId": "s-1", "cwd": "/x", "name": "Steering and Serving", "status": "busy"}))
    monkeypatch.setattr(identity, "SESSIONS_DIR", tmp_path)
    monkeypatch.delenv("ORCHESTRA_HANDLE", raising=False)
    me = identity.whoami()
    assert me.handle == "steering-and-serving" and me.session_id == "s-1" and me.source == "session-registry"


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
