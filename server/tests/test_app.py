from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from orchestra_server.app import Settings, create_app

AGENT = {"Authorization": "Bearer agent-tok"}
OP = {"Authorization": "Bearer op-tok"}


@pytest.fixture
def client(tmp_path: Path):
    s = Settings(data_dir=tmp_path, agent_token="agent-tok", operator_token="op-tok", operator="ada",
                 digest_delay_s=0.05, sweep_interval_s=0.05, cookie_secure=False)
    with TestClient(create_app(s)) as c:
        yield c


def soon(minutes: float = 30) -> str:
    return (datetime.now(UTC) + timedelta(minutes=minutes)).isoformat()


def test_auth_required(client):
    assert client.get("/api/state").status_code == 401
    assert client.get("/api/state", headers=AGENT).json()["you"] == "agent"
    assert client.get("/api/state", headers=OP).json()["you"] == "operator"


def test_ask_answer_roundtrip(client):
    r = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "ask", "body": "deploy?",
        "default": "no", "deadline": soon(), "revert_cost": "low"})
    assert r.status_code == 200, r.text
    ask = r.json()
    assert ask["id"].startswith("a")
    inbox = client.get("/api/state", headers=OP).json()["inbox"]
    assert [t["id"] for t in inbox["open"]] == [ask["id"]]

    # agent cannot answer the operator's ask; the operator can; answer routes back to asker
    assert client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "kind": "answer", "ref": ask["id"], "body": "yes"}).status_code == 403
    ans = client.post("/api/events", headers=OP, json={
        "from": "whoever", "kind": "answer", "ref": ask["id"], "body": "yes deploy"}).json()
    assert ans["from"] == "ada" and ans["to"] == "@atlas"
    t = client.get(f"/api/threads/{ask['id']}", headers=AGENT).json()
    assert t["status"] == "answered" and t["resolution"] == "yes deploy"
    # double answer refused
    assert client.post("/api/events", headers=OP, json={
        "from": "ada", "kind": "answer", "ref": ask["id"], "body": "no"}).status_code == 409


def test_ask_requires_default_and_deadline(client):
    r = client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@ada", "kind": "ask", "body": "x"})
    assert r.status_code == 422


def test_deadline_defaults(client):
    ask = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "ask", "body": "q", "default": "keep preview",
        "deadline": (datetime.now(UTC) + timedelta(seconds=0.2)).isoformat()}).json()
    for _ in range(40):
        t = client.get(f"/api/threads/{ask['id']}", headers=AGENT).json()
        if t["status"] != "open":
            break
        asyncio.run(asyncio.sleep(0.05))
    assert t["status"] == "defaulted" and t["resolution"] == "keep preview"
    evs = client.get("/api/events", headers=AGENT).json()["events"]
    d = [e for e in evs if e["kind"] == "defaulted"][0]
    assert d["to"] == "@atlas" and d["from"] == "orchestra"


def test_agents_cannot_impersonate_or_halt(client):
    assert client.post("/api/events", headers=AGENT, json={"from": "ada", "kind": "info", "body": "x"}).status_code == 422
    assert client.post("/api/events", headers=AGENT, json={"from": "atlas", "kind": "halt", "body": "x"}).status_code == 403
    assert client.post("/api/events", headers=OP, json={"from": "ada", "kind": "halt", "to": "@all"}).status_code == 200


def test_peer_request_answer(client):
    req = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@hermes", "kind": "request", "body": "which port?"}).json()
    assert client.post("/api/events", headers=AGENT, json={
        "from": "zeus", "kind": "answer", "ref": req["id"], "body": "no"}).status_code == 403
    ans = client.post("/api/events", headers=AGENT, json={
        "from": "hermes", "kind": "answer", "ref": req["id"], "body": "7460"}).json()
    assert ans["to"] == "@atlas"
    # peer requests do not land in the operator's inbox
    assert client.get("/api/state", headers=OP).json()["inbox"]["open"] == []


def test_idempotent_client_id(client):
    body = {"from": "atlas", "kind": "note", "to": "@ada", "body": "hi", "client_id": "c1"}
    a = client.post("/api/events", headers=AGENT, json=body).json()
    b = client.post("/api/events", headers=AGENT, json=body).json()
    assert a["seq"] == b["seq"]


def test_replay_survives_restart(tmp_path):
    s = Settings(data_dir=tmp_path, agent_token="a", operator_token="l", operator="ada", cookie_secure=False)
    with TestClient(create_app(s)) as c:
        n = c.post("/api/events", headers={"Authorization": "Bearer a"},
                   json={"from": "atlas", "to": "@ada", "kind": "note", "body": "remember me"}).json()
    (tmp_path / "bus.jsonl").open("a").write("not json\n")
    with TestClient(create_app(s)) as c:
        st = c.get("/api/state", headers={"Authorization": "Bearer l"}).json()
        assert st["head"] == n["seq"]
        assert st["inbox"]["open"][0]["body"] == "remember me"


def test_stream_backlog(client):
    for i in range(3):
        client.post("/api/events", headers=AGENT, json={"from": "atlas", "kind": "info", "body": f"m{i}"})
    with client.stream("GET", "/api/stream?after=1", headers=AGENT) as r:
        seen = []
        for line in r.iter_lines():
            if line.startswith("data: "):
                seen.append(line)
            if len(seen) == 2:
                break
    assert '"m1"' in seen[0] and '"m2"' in seen[1]


def test_login_cookie(client):
    assert client.post("/api/login", json={"token": "nope"}).status_code == 401
    assert client.post("/api/login", json={"token": "op-tok"}).status_code == 200
    assert client.get("/api/state").json()["you"] == "operator"


def test_push_policy_records_payloads(client):
    pusher = client.app.state.pusher
    client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "ask", "body": "q", "default": "d", "deadline": soon()})
    client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@ada", "kind": "note", "body": "fyi"})
    client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@all", "kind": "info", "body": "chatter"})
    for _ in range(40):
        if any(p["kind"] == "digest" for p in pusher.sent):
            break
        asyncio.run(asyncio.sleep(0.05))
    kinds = [p["kind"] for p in pusher.sent]
    assert kinds.count("ask") == 1 and kinds.count("digest") == 1
    assert pusher.public_key


def test_operator_name_is_reported(client):
    assert client.get("/api/state", headers=AGENT).json()["operator"] == "ada"


def test_broadcast_request_collects_answers(client):
    req = client.post("/api/events", headers=OP, json={"from": "ada", "to": "@all", "kind": "request", "body": "roundup?"}).json()
    for who in ("atlas", "hermes"):
        r = client.post("/api/events", headers=AGENT, json={"from": who, "kind": "answer", "ref": req["id"], "body": f"{who} here"})
        assert r.status_code == 200, r.text
        assert r.json()["to"] == "@ada"
    t = client.get(f"/api/threads/{req['id']}", headers=OP).json()
    assert t["status"] == "open" and [a["from"] for a in t["answers"]] == ["atlas", "hermes"]
    sent = client.get("/api/state", headers=OP).json()["inbox"]["sent"]
    assert sent[0]["id"] == req["id"] and len(sent[0]["answers"]) == 2
    # the operator closes it; a late answer is still recorded
    assert client.post("/api/events", headers=OP, json={"from": "ada", "kind": "ack", "ref": req["id"]}).status_code == 200
    assert client.post("/api/events", headers=AGENT, json={"from": "zeus", "kind": "answer", "ref": req["id"], "body": "late"}).status_code == 200
    t = client.get(f"/api/threads/{req['id']}", headers=OP).json()
    assert t["status"] == "acked" and len(t["answers"]) == 3


def test_refusal_names_the_reason(client):
    req = client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@hermes", "kind": "request", "body": "?"}).json()
    r = client.post("/api/events", headers=AGENT, json={"from": "zeus", "kind": "answer", "ref": req["id"], "body": "x"})
    assert r.status_code == 403 and "@hermes" in r.json()["detail"]


def test_replies_collect_on_thread_and_search(client):
    req = client.post("/api/events", headers=OP, json={"from": "ada", "to": "@all", "kind": "request", "body": "roundup?"}).json()
    rep = client.post("/api/events", headers=AGENT, json={"from": "writer", "kind": "reply", "ref": req["id"], "body": "drained; held bench a99ef"}).json()
    t = client.get(f"/api/threads/{req['id']}", headers=OP).json()
    assert [(a["from"], a["kind"]) for a in t["answers"]] == [("writer", "reply")]
    # exact id finds the message and everything pointing at it, newest first
    hits = client.get("/api/search", params={"q": req["id"]}, headers=OP).json()["events"]
    assert [h["id"] for h in hits] == [rep["id"], req["id"]]
    assert [h["id"] for h in client.get("/api/search", params={"q": rep["id"]}, headers=OP).json()["events"]] == [rep["id"]]
    assert client.get("/api/search", params={"q": "A99EF"}, headers=OP).json()["count"] == 1
    assert client.get("/api/search", params={"q": "@writer"}, headers=OP).json()["count"] == 1
    assert client.get("/api/search", params={"q": "nothing-like-this"}, headers=OP).json()["count"] == 0


def test_options_ask_choose_and_clarify(client):
    opts = [{"label": "ship now", "description": "tests green", "recommended": True},
            {"label": "wait for review"}, {"label": "abandon"}]
    bad = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "ask", "body": "ship?", "options": opts,
        "default": "not an option", "deadline": soon()})
    assert bad.status_code == 422
    ask = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "ask", "body": "ship?", "options": opts, "context": "v2 is ready",
        "default": "wait for review", "deadline": soon()}).json()
    t = client.get(f"/api/threads/{ask['id']}", headers=OP).json()
    assert [o["label"] for o in t["options"]] == ["ship now", "wait for review", "abandon"] and t["context"] == "v2 is ready"
    # asking for more info keeps it open and reaches the asker
    more = client.post("/api/events", headers=OP, json={"from": "ada", "kind": "reply", "ref": ask["id"], "body": "what about the migration?"}).json()
    assert more["to"] == "@atlas"
    client.post("/api/events", headers=AGENT, json={"from": "atlas", "kind": "reply", "ref": ask["id"], "body": "migration is reversible"})
    t = client.get(f"/api/threads/{ask['id']}", headers=OP).json()
    assert t["status"] == "open" and [a["from"] for a in t["answers"]] == ["ada", "atlas"]
    # single choice enforced, labels checked
    assert client.post("/api/events", headers=OP, json={"from": "ada", "kind": "answer", "ref": ask["id"], "choice": ["ship now", "abandon"]}).status_code == 422
    assert client.post("/api/events", headers=OP, json={"from": "ada", "kind": "answer", "ref": ask["id"], "choice": ["yolo"]}).status_code == 422
    ans = client.post("/api/events", headers=OP, json={"from": "ada", "kind": "answer", "ref": ask["id"], "choice": ["ship now"], "body": "after lunch"}).json()
    assert ans["choice"] == ["ship now"] and ans["to"] == "@atlas"
    t = client.get(f"/api/threads/{ask['id']}", headers=OP).json()
    assert t["status"] == "answered" and t["choice"] == ["ship now"] and t["resolution"] == "after lunch"


def test_options_multi_and_default(client):
    req = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "request", "body": "which?", "multi": True,
        "options": [{"label": "a"}, {"label": "b"}, {"label": "c"}]}).json()
    ans = client.post("/api/events", headers=OP, json={"from": "ada", "kind": "answer", "ref": req["id"], "choice": ["a", "c"]})
    assert ans.status_code == 200 and ans.json()["choice"] == ["a", "c"]
    ask = client.post("/api/events", headers=AGENT, json={
        "from": "atlas", "to": "@ada", "kind": "ask", "body": "q", "options": [{"label": "x"}, {"label": "y"}],
        "default": "y", "deadline": (datetime.now(UTC) + timedelta(seconds=0.2)).isoformat()}).json()
    for _ in range(40):
        t = client.get(f"/api/threads/{ask['id']}", headers=OP).json()
        if t["status"] != "open":
            break
        asyncio.run(asyncio.sleep(0.05))
    assert t["status"] == "defaulted" and t["choice"] == ["y"]
    # options only where they belong
    assert client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@ada", "kind": "note", "body": "x",
                                                          "options": [{"label": "a"}, {"label": "b"}]}).status_code == 422


def test_telemetry_usage_and_volume(client):
    def beat(h, sid, out, ctx, turns):
        r = client.post("/api/presence", headers=AGENT, json={"handle": h, "session_id": sid, "channel": True,
            "usage": {"output": out, "context": ctx, "turns": turns, "input": 1, "model": "claude-opus-5-5"}})
        assert r.status_code == 200
    beat("atlas", "s1", 100, 1000, 2)
    beat("atlas", "s1", 100, 1000, 2)   # unchanged: not recorded
    beat("atlas", "s1", 350, 1800, 5)
    beat("atlas", "s1", 345, 0, 5)      # a compaction's small dip: jitter, counts nothing (and context 0 is unknown)
    beat("atlas", "s1", 400, 900, 6)    # +55 from the dipped value
    beat("atlas", "s1", 40, 300, 1)     # reset (new run): counts as 40, never negative
    beat("hermes", "s2", 10, 50, 1)
    client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@hermes", "kind": "info", "body": "hi"})
    client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@hermes", "kind": "info", "body": "again"})
    req = client.post("/api/events", headers=AGENT, json={"from": "hermes", "to": "@ada", "kind": "request", "body": "?"}).json()
    client.post("/api/events", headers=OP, json={"from": "ada", "kind": "answer", "ref": req["id"], "body": "yes"})
    assert client.get("/api/telemetry", headers=AGENT).status_code == 403
    t = client.get("/api/telemetry", params={"hours": 1}, headers=OP).json()
    assert t["totals"]["messages"] == 4 and t["totals"]["to_operator"] == 1
    assert {"from": "atlas", "to": "@hermes", "count": 2} in t["edges"]
    atlas = next(s for s in t["sessions"] if s["handle"] == "atlas")
    assert atlas["window"]["output"] == 250 + 55 + 40 and 0 not in atlas["context"] and atlas["latest"]["context"] == 300
    assert t["latency"]["request"]["n"] == 1
    assert sum(b["total"] for b in t["volume"]) == 4


def test_bridge_and_usage_are_operator_only(client):
    client.post("/api/presence", headers=AGENT, json={"handle": "atlas", "session_id": "s1", "channel": True,
        "bridge": "session_01AbCdEfGhIjKlMn", "usage": {"output": 5, "context": 1234, "turns": 1}})
    op = client.get("/api/state", headers=OP).json()["presence"][0]
    ag = client.get("/api/state", headers=AGENT).json()["presence"][0]
    assert op["bridge"] == "session_01AbCdEfGhIjKlMn" and op["usage"]["context"] == 1234
    assert "bridge" not in ag and "usage" not in ag
    bad = client.post("/api/presence", headers=AGENT, json={"handle": "atlas", "bridge": "javascript:alert(1)"})
    assert bad.status_code == 422


def test_presence_persists_with_cursor_and_waiting(tmp_path):
    s = Settings(data_dir=tmp_path, agent_token="a", operator_token="o", operator="ada", cookie_secure=False)
    A, O = {"Authorization": "Bearer a"}, {"Authorization": "Bearer o"}
    with TestClient(create_app(s)) as c:
        c.post("/api/presence", headers=A, json={"handle": "atlas", "session_id": "s1", "cursor": 0})
        for i in range(3):
            c.post("/api/events", headers=O, json={"from": "ada", "to": "@atlas", "kind": "notice", "body": f"n{i}"})
        c.post("/api/events", headers=A, json={"from": "hermes", "to": "@all", "kind": "status", "body": "not pushed to atlas"})
    with TestClient(create_app(s)) as c:  # restart: the list and the cursor survive
        assert c.get("/api/presence/atlas", headers=A).json()["cursor"] == 0
        assert c.get("/api/presence/nobody", headers=A).status_code == 404
        app = c.app
        app.state.bus.presence["atlas"].last_seen = app.state.bus.presence["atlas"].last_seen.replace(year=2000)
        pres = c.get("/api/state", headers=O).json()["presence"]
        atlas = next(p for p in pres if p["handle"] == "atlas")
        assert atlas["live"] is False and atlas["waiting"] == 3
        # an old client (no cursor field) keeps the stored cursor
        c.post("/api/presence", headers=A, json={"handle": "atlas"})
        assert c.get("/api/presence/atlas", headers=A).json()["cursor"] == 0


def test_excerpts_short_addresses_hints_permalinks(client):
    client.post("/api/presence", headers=AGENT, json={"handle": "wayfinding-and-route-planning-service"})
    client.post("/api/presence", headers=AGENT, json={"handle": "trainer"})
    client.post("/api/presence", headers=AGENT, json={"handle": "cartographer"})
    e = client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@wayfinding", "kind": "info", "body": "x"}).json()
    assert e["to"] == "@wayfinding-and-route-planning-service"
    assert client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@p", "kind": "info", "body": "x"}).json()["to"] == "@p"  # ambiguous: as written
    note = client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@ada", "kind": "note", "body": "standard-1b is on main\nmore detail"}).json()
    ack = client.post("/api/events", headers=OP, json={"from": "ada", "kind": "ack", "ref": note["id"]}).json()
    assert ack["ref_excerpt"] == "standard-1b is on main" and ack["to"] == "@atlas"
    req = client.post("/api/events", headers=AGENT, json={"from": "atlas", "to": "@hermes", "kind": "request", "body": "?"}).json()
    r = client.post("/api/events", headers=AGENT, json={"from": "atlas", "kind": "answer", "ref": req["id"], "body": "self"})
    assert r.status_code == 403 and "kind='reply'" in r.json()["detail"]
    pl = client.get(f"/m/{note['id']}", follow_redirects=False)
    assert pl.status_code == 302 and pl.headers["location"] == f"/#q={note['id']}"


def test_rename_links_handles_and_redirects(client):
    client.post("/api/presence", headers=AGENT, json={"handle": "scout-82", "session_id": "sid-s", "cursor": 3})
    client.post("/api/presence", headers=AGENT, json={"handle": "scout-ops", "session_id": "sid-s"})
    pres = client.get("/api/state", headers=OP).json()["presence"]
    assert [p["handle"] for p in pres] == ["scout-ops"] and pres[0]["renamed_from"] == ["scout-82"]
    info = client.get("/api/presence/scout-82", headers=AGENT).json()
    assert info["renamed_to"] == "scout-ops"
    assert client.get("/api/presence/scout-ops", headers=AGENT).json()["cursor"] == 3  # cursor carried over
    ev = client.post("/api/events", headers=OP, json={"from": "ada", "to": "@scout-82", "kind": "notice", "body": "hi"}).json()
    assert ev["to"] == "@scout-ops"
    # a prefix only matches current names
    assert client.post("/api/events", headers=OP, json={"from": "ada", "to": "@scout", "kind": "notice", "body": "x"}).json()["to"] == "@scout-ops"


def test_rename_back_to_old_name(client):
    client.post("/api/presence", headers=AGENT, json={"handle": "a-one", "session_id": "s"})
    client.post("/api/presence", headers=AGENT, json={"handle": "b-two", "session_id": "s"})
    client.post("/api/presence", headers=AGENT, json={"handle": "a-one", "session_id": "s"})
    pres = client.get("/api/state", headers=OP).json()["presence"]
    assert [p["handle"] for p in pres] == ["a-one"]
    assert client.post("/api/events", headers=OP, json={"from": "ada", "to": "@b-two", "kind": "notice", "body": "x"}).json()["to"] == "@a-one"


def test_list_my_open_threads(client):
    a = client.post("/api/events", headers=AGENT, json={"from": "cartographer", "to": "@ada", "kind": "request", "body": "repo name?\nmore"}).json()
    b = client.post("/api/events", headers=AGENT, json={"from": "cartographer", "to": "@ada", "kind": "note", "body": "fyi"}).json()
    client.post("/api/events", headers=OP, json={"from": "ada", "kind": "ack", "ref": b["id"]})
    ts = client.get("/api/threads", params={"opened_by": "cartographer"}, headers=AGENT).json()["threads"]
    assert [t["id"] for t in ts] == [a["id"]] and ts[0]["first_line"] == "repo name?"
    assert len(client.get("/api/threads", params={"opened_by": "cartographer", "status": "any"}, headers=AGENT).json()["threads"]) == 2


def test_multi_address(client):
    for h in ("cartographer", "trainer", "route-planning-service", "archive-desk"):
        client.post("/api/presence", headers=AGENT, json={"handle": h, "cursor": 0})
    ev = client.post("/api/events", headers=OP, json={"from": "ada", "to": "@cart, @trainer,@route", "kind": "request", "body": "status?"}).json()
    assert ev["to"] == "@cartographer,@trainer,@route-planning-service"
    # each named recipient may answer; it stays open for the rest (a group request)
    for h in ("cartographer", "trainer"):
        assert client.post("/api/events", headers=AGENT, json={"from": h, "kind": "answer", "ref": ev["id"], "body": f"{h} ok"}).status_code == 200
    assert client.post("/api/events", headers=AGENT, json={"from": "archive-desk", "kind": "answer", "ref": ev["id"], "body": "x"}).status_code == 403
    t = client.get(f"/api/threads/{ev['id']}", headers=OP).json()
    assert t["status"] == "open" and [a["from"] for a in t["answers"]] == ["cartographer", "trainer"]
    assert client.post("/api/events", headers=AGENT, json={"from": "x1", "to": "@all,@trainer", "kind": "info", "body": "x"}).status_code == 422
    # a request to the operator plus a session lands in the operator's inbox
    both = client.post("/api/events", headers=AGENT, json={"from": "trainer", "to": "@ada,@cartographer", "kind": "request", "body": "both?"}).json()
    assert both["id"] in [x["id"] for x in client.get("/api/state", headers=OP).json()["inbox"]["open"]]
    # waiting counts multi-addressed mail for each recipient
    st = client.app.state.bus
    assert st.waiting_for("trainer", 0) >= 1 and st.waiting_for("archive-desk", 0) == 0
    tel = client.get("/api/telemetry", params={"hours": 1}, headers=OP).json()
    assert {"from": "ada", "to": "@trainer", "count": 1} in tel["edges"]


def test_handoffs_operator_only_and_bounded(client):
    for i in range(7):
        r = client.post("/api/handoffs", headers=AGENT, json={"handle": "trainer", "session_id": "s", "why": "before a manual compaction",
                                                              "written_at": f"2026-10-29T04:0{i}:00Z", "text": f"## Where we are\nnote {i}"})
        assert r.status_code == 200
    assert client.get("/api/handoffs/trainer", headers=AGENT).status_code == 403
    notes = client.get("/api/handoffs/trainer", headers=OP).json()["notes"]
    assert len(notes) == 5 and notes[0]["text"].endswith("note 6")
    client.post("/api/presence", headers=AGENT, json={"handle": "trainer"})
    p = client.get("/api/state", headers=OP).json()["presence"][0]
    assert p["handoff_at"].startswith("2026-10-29T04:06")
    assert "handoff_at" not in client.get("/api/state", headers=AGENT).json()["presence"][0]
    assert client.post("/api/handoffs", headers=OP, json={"handle": "trainer", "written_at": "2026-10-29T05:00:00Z", "text": "x"}).status_code == 403
