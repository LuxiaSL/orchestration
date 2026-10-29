from __future__ import annotations

import json
from pathlib import Path

from orchestra_mcp.recall import Recall


def _row(**kw) -> str:
    return json.dumps(kw)


def test_search_and_read_reach_before_a_compaction(tmp_path: Path):
    f = tmp_path / "s.jsonl"
    f.write_text("\n".join([
        _row(type="user", timestamp="2026-10-29T01:00:00Z", message={"content": "keep the eval on the held-out v2 split, not the full one"}),
        _row(type="assistant", timestamp="2026-10-29T01:00:05Z", message={"content": [
            {"type": "text", "text": "Got it: held-out v2."},
            {"type": "tool_use", "name": "Bash", "input": {"command": "heimdall submit --name dpo-eval-7f3a"}}]}),
        _row(type="user", timestamp="2026-10-29T01:00:09Z", message={"content": [{"type": "tool_result", "content": "job 4412 queued\n" + "x" * 900}]}),
        _row(type="user", isSidechain=True, message={"content": "subagent chatter about held-out v2"}),
        _row(type="system", subtype="compact_boundary", timestamp="2026-10-29T02:00:00Z", compactMetadata={"trigger": "manual", "preTokens": 600000}),
        _row(type="user", isCompactSummary=True, timestamp="2026-10-29T02:00:01Z", message={"content": "This session is being continued... (summary)"}),
        _row(type="user", timestamp="2026-10-29T02:05:00Z", message={"content": "<system-reminder>noise</system-reminder>"}),
        _row(type="user", timestamp="2026-10-29T02:06:00Z", message={"content": "which split did we pick?"}),
    ]) + "\n")
    r = Recall()
    s = r.search(f, "held-out v2")
    assert [h["who"] for h in s["hits"]] == ["me", "you"]  # newest first; the subagent's row is not this session's
    assert s["hits"][1]["line"] == 1 and s["hits"][1]["compactions_before"] == 0 and s["compactions_in_transcript"] == 1
    assert r.search(f, "4412")["hits"][0]["who"] == "result"
    assert r.search(f, "dpo-eval-7f3a", who="tool")["hits"][0]["line"] == 2
    assert r.search(f, "noise")["hits"] == []  # machinery isn't the user talking
    out = r.read(f, 2, before=1, after=3)
    whos = [e["who"] for e in out["entries"]]
    assert whos[:4] == ["you", "me", "tool", "result"] and "compaction" in whos
    res = next(e for e in out["entries"] if e["who"] == "result")
    assert len(res["text"]) < 400 and "results=True" in res["text"]
    assert len(next(e for e in r.read(f, 3, 0, 0, results=True)["entries"])["text"]) > 900
    assert r.search(f, "   ")["hits"] == []


def test_code_written_with_edit_is_searchable(tmp_path: Path):
    f = tmp_path / "s.jsonl"
    f.write_text(_row(type="assistant", timestamp="t", message={"content": [
        {"type": "tool_use", "name": "Edit", "input": {"file_path": "/x/server.py", "old_string": "a", "new_string": "def run_legacy_stdio():\n    pass"}}]}) + "\n")
    r = Recall()
    assert r.search(f, "run_legacy_stdio")["hits"][0]["who"] == "tool"
    assert r.read(f, 1)["entries"][0]["text"].startswith("Edit: /x/server.py … [input")
    assert "run_legacy_stdio" in r.read(f, 1, results=True)["entries"][0]["text"]


def test_mark_tool_writes_marks_the_plugin_reads(tmp_path: Path, monkeypatch):
    import asyncio
    from orchestra_mcp import link as L, server as S
    monkeypatch.setattr(S, "CONTINUITY_DIR", tmp_path)
    monkeypatch.setattr(L, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setenv("ORCHESTRA_HANDLE", "scout")
    srv = S.build_server(L.Config(url="http://x", token="t", operator="ada"))
    monkeypatch.setattr(L.Link, "me", property(lambda self: type("I", (), {"session_id": "sid-9", "handle": "scout", "cwd": "/x"})()))
    async def go():
        return await srv.call_tool("mark", {"text": "the lake, not the park", "why": "reseeding"})
    asyncio.run(go())
    rows = [json.loads(l) for l in (tmp_path / "sid-9" / "marks.jsonl").read_text().splitlines()]
    assert rows[0]["text"] == "the lake, not the park" and rows[0]["said_by"] == "ada" and rows[0]["why"] == "reseeding"
