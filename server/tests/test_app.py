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
