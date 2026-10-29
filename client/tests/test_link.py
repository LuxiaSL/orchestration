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
