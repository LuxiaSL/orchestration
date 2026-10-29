from __future__ import annotations

import json
from pathlib import Path

from orchestra_mcp import usage as U


def _row(mid: str, inp: int, out: int, cr: int = 0, cw: int = 0, think: int = 0) -> str:
    return json.dumps({"type": "assistant", "timestamp": "2026-10-29T00:00:00Z", "message": {
        "id": mid, "model": "claude-opus-5-5",
        "usage": {"input_tokens": inp, "output_tokens": out, "cache_read_input_tokens": cr,
                  "cache_creation_input_tokens": cw, "output_tokens_details": {"thinking_tokens": think}}}}) + "\n"


def test_incremental_dedup_and_partial_lines(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(U, "PROJECTS_DIR", tmp_path)
    d = tmp_path / U.project_slug("/home/x/my_proj"); d.mkdir()
    f = d / "sess-1.jsonl"
    f.write_text(_row("m1", 10, 5, 100, 20, 2) + json.dumps({"type": "user"}) + "\n" + _row("m1", 10, 50, 100, 20, 30))
    t = U.UsageTracker()
    tot = t.poll("sess-1", "/home/x/my_proj")
    assert (tot.turns, tot.output, tot.thinking, tot.context) == (1, 50, 30, 130)  # m1 counted once, latest wins
    with f.open("a") as fh:
        fh.write(_row("m2", 1, 7, 200, 0))
        fh.write(_row("m3", 1, 9)[:20])  # half-written line must wait
    tot = t.poll("sess-1", "/home/x/my_proj")
    assert (tot.turns, tot.output, tot.context) == (2, 57, 201)
    with f.open("a") as fh:
        fh.write(_row("m3", 1, 9)[20:])
    assert t.poll("sess-1", "/home/x/my_proj").turns == 3


def test_missing_transcript_is_none(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(U, "PROJECTS_DIR", tmp_path)
    assert U.UsageTracker().poll("nope", "/x") is None
    assert U.UsageTracker().poll(None, "/x") is None


def test_real_transcript_if_present():
    # Under claude-code, our own session transcript should parse to something sane.
    from orchestra_mcp.identity import whoami
    me = whoami()
    tot = U.UsageTracker().poll(me.session_id, me.cwd)
    if tot is not None:
        assert tot.turns > 0 and tot.output > 0 and tot.context > 0
