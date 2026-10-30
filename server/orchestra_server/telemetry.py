"""Telemetry: token-usage samples from session heartbeats, plus summaries of the bus.

Usage samples are cumulative counters per session (from its transcript). A sample is
kept only when the counters move, appended to telemetry.jsonl and replayed at start
(bounded to RETAIN_DAYS). Rates are deltas between consecutive samples of the same
session id; a counter going backwards means the transcript was reset, so the new
value starts a fresh run rather than producing a negative rate.
"""

from __future__ import annotations

import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ValidationError

from .bus import Bus, now
from .models import CostRow, is_to, recipients

RETAIN_DAYS = 8
MAX_BUCKETS = 120


class Usage(BaseModel):
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    thinking: int = 0
    turns: int = 0
    context: int = 0
    model: str | None = None
    last_ts: str | None = None


class Sample(BaseModel):
    ts: datetime
    handle: str
    session_id: str
    usage: Usage


class Telemetry:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.samples: list[Sample] = []
        self._last: dict[str, Usage] = {}  # by session id
        # continuity's own model requests (never in a transcript), by session id: (handle, rows); kept beside the samples
        self.costs_path = path.with_name("continuity-costs.json")
        self.costs: dict[str, tuple[str, list[CostRow]]] = {}
        self._load()
        self._load_costs()

    def _load_costs(self) -> None:
        if not self.costs_path.exists():
            return
        try:
            raw = json.loads(self.costs_path.read_text())
            self.costs = {sid: (v["handle"], [CostRow.model_validate(r) for r in v["rows"]]) for sid, v in raw.items()}
        except (OSError, ValueError, KeyError, TypeError, ValidationError) as exc:
            print(f"orchestra: continuity costs unreadable ({exc}); starting empty", file=sys.stderr)

    def record_costs(self, handle: str, session_id: str | None, rows: list[CostRow]) -> None:
        """The client resends its session's recent rows each heartbeat; keep the newest set, write only on change."""
        if not session_id:
            return
        cutoff = now() - timedelta(days=RETAIN_DAYS)
        rows = sorted((r for r in rows if r.ts >= cutoff), key=lambda r: r.ts)
        prev = self.costs.get(session_id)
        if prev is not None and prev[0] == handle and prev[1] == rows:
            return
        self.costs[session_id] = (handle, rows)
        tmp = self.costs_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps({sid: {"handle": h, "rows": [r.model_dump(mode="json") for r in rs]}
                                       for sid, (h, rs) in self.costs.items()}))
            tmp.replace(self.costs_path)
        except OSError as exc:
            print(f"orchestra: continuity costs write failed: {exc}", file=sys.stderr)

    def _load(self) -> None:
        if not self.path.exists():
            return
        cutoff = now() - timedelta(days=RETAIN_DAYS)
        kept = 0
        with self.path.open() as f:
            for line in f:
                try:
                    s = Sample.model_validate_json(line)
                except ValidationError:
                    continue
                if s.ts >= cutoff:
                    self.samples.append(s)
                    self._last[s.session_id] = s.usage
                    kept += 1
        # compact: rewrite only what is retained, so the file doesn't grow forever
        if kept < sum(1 for _ in self.path.open()):
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text("".join(s.model_dump_json() + "\n" for s in self.samples))
            tmp.replace(self.path)

    def record(self, handle: str, session_id: str | None, usage: Usage | None) -> None:
        if usage is None or not session_id:
            return
        prev = self._last.get(session_id)
        if prev is not None and prev.output == usage.output and prev.turns == usage.turns and prev.context == usage.context:
            return  # nothing moved
        s = Sample(ts=now(), handle=handle, session_id=session_id, usage=usage)
        self.samples.append(s)
        self._last[session_id] = usage
        try:
            with self.path.open("a") as f:
                f.write(s.model_dump_json() + "\n")
        except OSError as exc:
            print(f"orchestra: telemetry write failed: {exc}", file=sys.stderr)


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def summarize(bus: Bus, tel: Telemetry, hours: float) -> dict:
    t1 = now()
    t0 = t1 - timedelta(hours=hours)
    span = (t1 - t0).total_seconds()
    # bucket size: round up to a friendly step so axes read cleanly
    steps = [60, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400]
    bucket_s = next((s for s in steps if span / s <= MAX_BUCKETS), steps[-1])
    n = math.ceil(span / bucket_s)
    start = t1 - timedelta(seconds=n * bucket_s)

    def bucket(ts: datetime) -> int:
        return min(n - 1, max(0, int((ts - start).total_seconds() // bucket_s)))

    evs = [e for e in bus.events if e.ts >= start]
    volume = [Counter() for _ in range(n)]
    edges: Counter[tuple[str, str]] = Counter()
    senders: Counter[str] = Counter()
    chars: dict[str, list[int]] = defaultdict(list)
    for e in evs:
        volume[bucket(e.ts)][e.kind] += 1
        senders[e.from_] += 1
        for r in recipients(e.to) or [e.to]:  # a message to several counts once per recipient
            edges[(e.from_, r)] += 1
        chars[e.kind].append(len(e.body))

    # time-to-resolution of threads addressed to the operator, resolved in the window
    latency: dict[str, list[float]] = defaultdict(list)
    outcomes: Counter[str] = Counter()
    for t in bus.threads.values():
        if not is_to(t.to, bus.operator) or t.resolved_at is None or t.resolved_at < start:
            continue
        outcomes[f"{t.kind}:{t.status}"] += 1
        if t.status in ("answered", "acked"):
            latency[t.kind].append((t.resolved_at - t.ts).total_seconds())

    # usage: per session id, deltas between consecutive samples, binned; then grouped by handle
    by_sid: dict[str, list[Sample]] = defaultdict(list)
    for s in tel.samples:
        by_sid[s.session_id].append(s)
    sessions: dict[str, dict] = {}
    for sid, ss in by_sid.items():
        ss.sort(key=lambda s: s.ts)
        handle = ss[-1].handle
        row = sessions.setdefault(handle, {
            "handle": handle, "output": [0] * n, "context": [None] * n,
            "window": {"output": 0, "input": 0, "cache_read": 0, "cache_write": 0, "thinking": 0, "turns": 0},
            "latest": None,
        })
        prev: Usage | None = None
        for s in ss:
            if prev is not None and s.ts >= start:
                d = {k: getattr(s.usage, k) - getattr(prev, k) for k in row["window"]}
                if d["turns"] < 0 or s.usage.output < prev.output // 2:
                    d = {k: getattr(s.usage, k) for k in row["window"]}  # counter reset: new run
                else:
                    # a small dip (a compaction rewrites a row's usage) is jitter, not a reset
                    d = {k: max(0, v) for k, v in d.items()}
                b = bucket(s.ts)
                row["output"][b] += d["output"]
                for k, v in d.items():
                    row["window"][k] += v
            if s.ts >= start and s.usage.context:  # 0 right after a compaction: not known until the next turn
                row["context"][bucket(s.ts)] = s.usage.context
            prev = s.usage
        last = ss[-1]
        if row["latest"] is None or last.ts.isoformat() > row["latest"]["ts"]:
            row["latest"] = {"ts": last.ts.isoformat(), "session_id": sid, **last.usage.model_dump(exclude_none=True)}
    # drop sessions with nothing in or before the window worth showing
    sess = sorted((r for r in sessions.values() if r["latest"] and r["latest"]["ts"] >= (start - timedelta(days=1)).isoformat()),
                  key=lambda r: -r["window"]["output"])

    # continuity's own requests in the window, per handle and kind, beside each session's own turns
    cont: dict[str, dict] = {}
    for sid, (handle, rows) in tel.costs.items():
        for r in rows:
            if r.ts < start:
                continue
            c = cont.setdefault(handle, {"handle": handle, "notes": 0, "fallbacks": 0, "compactions": 0,
                                         "input": 0, "output": 0, "cache_read": 0, "cache_write": 0})
            c["notes" if r.what == "note" else "fallbacks" if r.what == "note-transcript" else "compactions"] += 1
            u = r.usage
            c["input"] += u.input_tokens
            c["output"] += u.output_tokens
            c["cache_read"] += u.cache_read_input_tokens
            c["cache_write"] += u.cache_creation_input_tokens
    own = {r["handle"]: r["window"] for r in sess}
    for c in cont.values():
        w = own.get(c["handle"])
        c["session"] = {k: w[k] for k in ("input", "output", "cache_read", "cache_write")} if w else None
    continuity = sorted(cont.values(), key=lambda c: -(c["input"] + c["cache_read"] + c["cache_write"]))

    kinds = sorted({k for c in volume for k in c})
    return {
        "from": start.isoformat(), "to": t1.isoformat(), "bucket_s": bucket_s, "buckets": n,
        "volume": [{"t": (start + timedelta(seconds=i * bucket_s)).isoformat(), "total": sum(c.values()), "by_kind": dict(c)}
                   for i, c in enumerate(volume)],
        "kinds": kinds,
        "totals": {
            "messages": len(evs),
            "to_operator": sum(1 for e in evs if is_to(e.to, bus.operator)),
            "from_operator": sum(1 for e in evs if e.from_ == bus.operator),
            "asks": outcomes.get("ask:answered", 0) + outcomes.get("ask:acked", 0) + outcomes.get("ask:defaulted", 0),
            "asks_defaulted": outcomes.get("ask:defaulted", 0),
            "output_tokens": sum(r["window"]["output"] for r in sess),
        },
        "senders": dict(senders.most_common()),
        "edges": [{"from": f, "to": t, "count": c} for (f, t), c in edges.most_common()],
        "latency": {k: {"n": len(v), "median_s": statistics.median(v), "p90_s": _pct(v, 0.9)} for k, v in latency.items()},
        "outcomes": dict(outcomes),
        "avg_chars": {k: round(sum(v) / len(v)) for k, v in chars.items()},
        "sessions": sess,
        "continuity": continuity,
    }
