"""Recall: search and read this session's own transcript, including everything before a compaction.

A compaction replaces the conversation with a summary, but Claude Code keeps the whole record in
~/.claude/projects/<cwd-slug>/<sessionId>.jsonl. Most of a context is tool output that can be fetched
again; what a summary drops is the thin layer in between: exact ids, the person's words, the reason
something was turned down. This reads it back on demand, in bounded pieces.

Each entry is addressed by its line in the transcript file, which never changes (the file is
append-only), so a line number found once can be read again later.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .usage import PROJECTS_DIR, project_slug

_MACHINERY = ("<command-", "<local-command", "<system-reminder", "<task-notification", "Caveat:", "<bash-")
SNIPPET = 240
READ_CHARS = 14_000


def locate(session_id: str | None, cwd: str) -> Path | None:
    if not session_id:
        return None
    p = PROJECTS_DIR / project_slug(cwd) / f"{session_id}.jsonl"
    if p.exists():
        return p
    hits = list(PROJECTS_DIR.glob(f"*/{session_id}.jsonl"))
    return hits[0] if hits else None


@dataclass
class Entry:
    line: int          # 1-based line in the transcript file
    epoch: int         # compactions before this entry (0 = before the first)
    ts: str
    who: str           # you | me | tool | result | summary | compaction
    text: str          # rendered in full


def _text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    return ""


def _tool_line(b: dict) -> str:
    """The call's gist on the first line, then its whole input (searchable: code written with Edit/Write
    holds identifiers too); recall_read shows only the gist unless asked."""
    inp = b.get("input") or {}
    name = b.get("name", "?")
    gist = inp.get("command") or inp.get("file_path") or inp.get("pattern") or inp.get("description") or inp.get("prompt") or ""
    if not gist:
        gist = json.dumps(inp, ensure_ascii=False)
    gist = " ".join(str(gist).split())
    full = json.dumps(inp, ensure_ascii=False)
    return f"{name}: {gist[:400]}" + (f"\n{full[:20_000]}" if len(full) > len(gist) + 20 else "")


def _result_text(b: dict) -> str:
    c = b.get("content")
    if isinstance(c, list):
        c = "\n".join(x.get("text", "") for x in c if isinstance(x, dict) and x.get("type") == "text")
    return str(c or "")


def entries(path: Path) -> list[Entry]:
    out: list[Entry] = []
    epoch = 0
    with path.open(errors="replace") as fh:
        for n, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                r = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if r.get("isSidechain"):
                continue
            ts = r.get("timestamp", "")
            if r.get("subtype") == "compact_boundary":
                epoch += 1
                md = r.get("compactMetadata") or {}
                out.append(Entry(n, epoch, ts, "compaction", f"── compaction #{epoch} ({md.get('trigger', '?')}, {md.get('preTokens', '?')} tokens before) ──"))
                continue
            kind, msg = r.get("type"), r.get("message") or {}
            c = msg.get("content")
            if kind == "user" and r.get("isCompactSummary"):
                out.append(Entry(n, epoch, ts, "summary", _text(c)))
            elif kind == "user":
                if isinstance(c, list) and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in c):
                    for b in c:
                        if isinstance(b, dict) and b.get("type") == "tool_result":
                            out.append(Entry(n, epoch, ts, "result", _result_text(b)))
                    continue
                t = _text(c).strip()
                if t and not r.get("isMeta") and not t.startswith(_MACHINERY):
                    out.append(Entry(n, epoch, ts, "you", t))
            elif kind == "assistant" and isinstance(c, list):
                t = _text(c).strip()
                if t:
                    out.append(Entry(n, epoch, ts, "me", t))
                for b in c:
                    if isinstance(b, dict) and b.get("type") == "tool_use":
                        out.append(Entry(n, epoch, ts, "tool", _tool_line(b)))
    return out


class Recall:
    """Parsed once per file size: the transcript only grows."""

    def __init__(self) -> None:
        self._key: tuple[str, int] | None = None
        self._entries: list[Entry] = []

    def load(self, path: Path) -> list[Entry]:
        key = (str(path), path.stat().st_size)
        if key != self._key:
            self._entries, self._key = entries(path), key
        return self._entries

    def search(self, path: Path, query: str, limit: int = 20, who: str | None = None) -> dict:
        terms = [t.lower() for t in query.split() if t.strip()]
        if not terms:
            return {"hits": [], "note": "empty query"}
        es = self.load(path)
        hits = []
        for e in reversed(es):  # newest first
            if who and e.who != who:
                continue
            low = e.text.lower()
            if all(t in low for t in terms):
                i = low.find(terms[0])
                lo = max(0, i - SNIPPET // 3)
                snip = " ".join(e.text[lo:lo + SNIPPET].split())
                hits.append({"line": e.line, "who": e.who, "ts": e.ts[:16], "compactions_before": e.epoch,
                             "snippet": ("…" if lo else "") + snip + ("…" if lo + SNIPPET < len(e.text) else "")})
                if len(hits) >= limit:
                    break
        return {"hits": hits, "compactions_in_transcript": es[-1].epoch if es else 0,
                "next": "recall_read(line=…) for the surrounding conversation"}

    def read(self, path: Path, line: int, before: int = 4, after: int = 12, results: bool = False) -> dict:
        es = self.load(path)
        idx = next((i for i, e in enumerate(es) if e.line >= line), None)
        if idx is None:
            return {"entries": [], "note": f"nothing at or after line {line}"}
        lo, hi = max(0, idx - before), min(len(es), idx + after + 1)
        out, used = [], 0
        for e in es[lo:hi]:
            text = e.text
            if e.who == "result" and not results:
                text = text[:300] + (f"… [{len(e.text)} chars; results=True for more]" if len(text) > 300 else "")
            elif e.who == "tool" and not results:
                head, _, rest = text.partition("\n")
                text = head + (" … [input; results=True for all]" if rest else "")
            elif e.who == "summary":
                text = text[:1500] + ("… [summary continues]" if len(text) > 1500 else "")
            if used + len(text) > READ_CHARS:
                text = text[: max(0, READ_CHARS - used)] + " … [cut: read again from a later line]"
            used += len(text)
            out.append({"line": e.line, "who": e.who, "ts": e.ts[:16], "text": text})
            if used >= READ_CHARS:
                break
        return {"entries": out, "first_line": es[lo].line, "last_line": out[-1]["line"] if out else None}
