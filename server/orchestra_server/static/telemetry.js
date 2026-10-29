// orchestra telemetry view: bus volume, token use per session, who talks to whom, response times.
// Grayscale by design: one series per chart (small multiples for sessions), a one-hue
// light→dark ramp for the matrix. Every value is also in a table view; tooltips only enhance.
(() => {
  const $ = (s, r = document) => r.querySelector(s);
  let hours = 24, timer = null, data = null, visible = false;
  try { hours = Number(localStorage.getItem("orc-telem-hours")) || 24; } catch {}

  // ---- formatting ----
  const fmtN = (n) => {
    if (n == null) return "—";
    const a = Math.abs(n);
    if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + "B";
    if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + "M";
    if (a >= 1e3) return (n / 1e3).toFixed(a >= 1e4 ? 0 : 1) + "K";
    return String(Math.round(n));
  };
  const fmtDur = (s) => {
    if (s == null) return "—";
    if (s < 60) return `${Math.round(s)}s`;
    if (s < 3600) return `${Math.round(s / 60)}m`;
    if (s < 86400) return `${(s / 3600).toFixed(s < 36000 ? 1 : 0)}h`;
    return `${(s / 86400).toFixed(1)}d`;
  };
  const bucketLabel = (s) => s >= 86400 ? `${s / 86400} day` : s >= 3600 ? `${s / 3600} h` : `${s / 60} min`;
  const tlabel = (iso, span) => {
    const d = new Date(iso);
    return span > 36 * 3600
      ? d.toLocaleDateString([], { month: "short", day: "numeric" }) + " " + d.toLocaleTimeString([], { hour: "2-digit" })
      : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  };
  const niceMax = (v) => {
    if (v <= 0) return 1;
    const p = Math.pow(10, Math.floor(Math.log10(v))), m = v / p;
    return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
  };

  // ---- DOM helpers: untrusted strings go through textContent only ----
  function el(tag, attrs = {}, ...kids) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") n.className = v; else if (k === "text") n.textContent = v; else n.setAttribute(k, v);
    }
    for (const k of kids) if (k != null) n.append(k);
    return n;
  }
  const NS = "http://www.w3.org/2000/svg";
  function sv(tag, attrs = {}) {
    const n = document.createElementNS(NS, tag);
    for (const [k, v] of Object.entries(attrs)) { if (k === "text") n.textContent = v; else n.setAttribute(k, v); }
    return n;
  }
  const tip = () => $("#tip");
  function showTip(evt, rows) {
    const t = tip(); t.replaceChildren();
    for (const [v, l] of rows) t.append(el("div", { class: "row" }, el("span", { class: "v", text: v }), el("span", { class: "l", text: l })));
    t.style.display = "block";
    const r = t.getBoundingClientRect();
    let x = evt.clientX + 14, y = evt.clientY + 14;
    if (x + r.width > innerWidth - 8) x = evt.clientX - r.width - 14;
    if (y + r.height > innerHeight - 8) y = evt.clientY - r.height - 14;
    t.style.left = x + "px"; t.style.top = y + "px";
  }
  const hideTip = () => { tip().style.display = "none"; };

  function card(title, sub, wide = false) {
    const c = el("div", { class: "tcard" + (wide ? " wide" : "") }, el("h3", { text: title }), el("div", { class: "sub", text: sub }));
    return c;
  }
  function tableView(head, rows) {
    const t = el("table", { class: "tt" });
    t.append(el("thead", {}, el("tr", {}, ...head.map((h) => el("th", { text: h })))));
    const tb = el("tbody");
    for (const r of rows) tb.append(el("tr", {}, ...r.map((c) => el("td", { text: String(c) }))));
    t.append(tb);
    return el("details", { class: "tv" }, el("summary", { text: "table" }), el("div", { class: "scroll" }, t));
  }

  // ---- stat tiles ----
  function tiles(d) {
    const T = d.totals, req = d.latency.request, ask = d.latency.ask;
    const answered = [req, ask].filter(Boolean);
    const allN = answered.reduce((a, x) => a + x.n, 0);
    const med = answered.length ? answered.reduce((a, x) => a + x.median_s * x.n, 0) / allN : null;
    const st = window.orcState && window.orcState();
    const live = st && st.presence ? st.presence.filter((p) => p.live).length : null;
    const items = [
      ["messages", fmtN(T.messages), `${fmtN(T.messages / (d.buckets * d.bucket_s / 3600))}/h on average`],
      ["to you", fmtN(T.to_operator), `${fmtN(T.from_operator)} from you`],
      ["live sessions", live == null ? "—" : String(live), `${d.sessions.length} reporting tokens`],
      ["time to answer", fmtDur(med), allN ? `typical, over ${allN} answered` : "nothing answered yet"],
      ["asks defaulted", `${T.asks_defaulted}/${T.asks}`, T.asks ? `${Math.round(100 * T.asks_defaulted / T.asks)}% hit the deadline` : "no asks resolved"],
      ["output tokens", fmtN(T.output_tokens), "all sessions, this range"],
    ];
    return el("div", { class: "tiles" }, ...items.map(([l, v, s]) =>
      el("div", { class: "tile" }, el("div", { class: "tl", text: l }), el("div", { class: "tv", text: v }), el("div", { class: "ts2", text: s }))));
  }

  // ---- column chart: messages per bucket (one series) ----
  function volumeChart(d, width) {
    const H = 180, padL = 34, padB = 22, padT = 8, W = Math.max(260, width);
    const n = d.volume.length, max = niceMax(Math.max(1, ...d.volume.map((b) => b.total)));
    const slot = (W - padL) / n, bw = Math.max(1, Math.min(24, slot - 2));
    const y = (v) => padT + (H - padT - padB) * (1 - v / max);
    const svg = sv("svg", { viewBox: `0 0 ${W} ${H}`, height: H, role: "img", "aria-label": "messages per interval" });
    for (const f of [0, 0.5, 1]) {
      const yy = y(max * f);
      svg.append(sv("line", { class: f === 0 ? "ax" : "gl", x1: padL, x2: W, y1: yy, y2: yy }));
      svg.append(sv("text", { x: padL - 6, y: yy + 4, "text-anchor": "end", text: fmtN(max * f) }));
    }
    const span = d.buckets * d.bucket_s;
    const every = Math.max(1, Math.ceil(n / Math.floor((W - padL) / 70)));
    d.volume.forEach((b, i) => {
      const x = padL + i * slot + (slot - bw) / 2;
      if (b.total > 0) {
        const top = y(b.total), h = y(0) - top, r = Math.min(4, bw / 2, h);
        // rounded data-end, square at the baseline
        svg.append(sv("path", { class: "bar", d: `M${x},${y(0)} V${top + r} Q${x},${top} ${x + r},${top} H${x + bw - r} Q${x + bw},${top} ${x + bw},${top + r} V${y(0)} Z` }));
      }
      if (i % every === 0) svg.append(sv("text", { x: padL + i * slot + slot / 2, y: H - 6, "text-anchor": "middle", text: tlabel(b.t, span) }));
      const hit = sv("rect", { class: "hit", x: padL + i * slot, y: padT, width: slot, height: H - padT - padB, tabindex: 0 });
      const rows = () => [[String(b.total), `messages · ${tlabel(b.t, span)}`],
        ...Object.entries(b.by_kind).sort((a, c) => c[1] - a[1]).map(([k, v]) => [String(v), k])];
      hit.addEventListener("pointermove", (e) => showTip(e, rows()));
      hit.addEventListener("pointerleave", hideTip);
      hit.addEventListener("focus", (e) => { const r = hit.getBoundingClientRect(); showTip({ clientX: r.right, clientY: r.top }, rows()); });
      hit.addEventListener("blur", hideTip);
      svg.append(hit);
    });
    return svg;
  }

  // ---- small multiple: one session's output tokens per bucket ----
  function spark(row, d, max, width) {
    const H = 44, W = Math.max(120, width), n = row.output.length;
    const x = (i) => (n === 1 ? W / 2 : (i / (n - 1)) * (W - 8) + 4);
    const y = (v) => 4 + (H - 8) * (1 - v / max);
    const svg = sv("svg", { viewBox: `0 0 ${W} ${H}`, height: H, role: "img", "aria-label": `${row.handle} output tokens over time` });
    svg.append(sv("line", { class: "ax", x1: 0, x2: W, y1: y(0), y2: y(0) }));
    const pts = row.output.map((v, i) => [x(i), y(v)]);
    svg.append(sv("path", { class: "ar", d: `M${pts[0][0]},${y(0)} ` + pts.map((p) => `L${p[0]},${p[1]}`).join(" ") + ` L${pts[pts.length - 1][0]},${y(0)} Z` }));
    svg.append(sv("path", { class: "ln", d: pts.map((p, i) => `${i ? "L" : "M"}${p[0]},${p[1]}`).join(" ") }));
    const xh = sv("line", { class: "xh", y1: 0, y2: H, visibility: "hidden" });
    const dot = sv("circle", { class: "dot", r: 4, visibility: "hidden" });
    svg.append(xh, dot);
    const hit = sv("rect", { class: "hit", x: 0, y: 0, width: W, height: H });
    const span = d.buckets * d.bucket_s;
    hit.addEventListener("pointermove", (e) => {
      const r = svg.getBoundingClientRect();
      const i = Math.max(0, Math.min(n - 1, Math.round(((e.clientX - r.left) / r.width * W - 4) / (W - 8) * (n - 1))));
      xh.setAttribute("x1", pts[i][0]); xh.setAttribute("x2", pts[i][0]); xh.setAttribute("visibility", "visible");
      dot.setAttribute("cx", pts[i][0]); dot.setAttribute("cy", pts[i][1]); dot.setAttribute("visibility", "visible");
      const ctx = row.context[i];
      showTip(e, [[fmtN(row.output[i]), `output tokens · ${tlabel(d.volume[i].t, span)}`], ...(ctx != null ? [[fmtN(ctx), "context"]] : [])]);
    });
    hit.addEventListener("pointerleave", () => { hideTip(); xh.setAttribute("visibility", "hidden"); dot.setAttribute("visibility", "hidden"); });
    svg.append(hit);
    return svg;
  }
  // A live session's Remote Control link, so a heavy context can be compacted from the Claude app.
  function bridgeLink(handle) {
    const st = window.orcState && window.orcState();
    const p = st && st.presence && st.presence.find((x) => x.handle === handle && x.live && x.bridge);
    if (!p) return null;
    return el("a", { class: "rc", href: `https://claude.ai/code/${encodeURIComponent(p.bridge)}`, target: "_blank", rel: "noopener",
                     title: "open in the Claude app: /compact works there", text: "open ↗ to compact" });
  }
  function tokensCard(d, width) {
    const c = card("Tokens by session", `output tokens per ${bucketLabel(d.bucket_s)}, one shared scale · context is the latest prompt size`, true);
    if (!d.sessions.length) { c.append(el("div", { class: "empty", text: "no session has reported token usage yet (sessions report on their 60 s heartbeat)" })); return c; }
    const max = niceMax(Math.max(1, ...d.sessions.flatMap((s) => s.output)));
    c.append(el("div", { class: "sm head" }, el("span", { text: "session" }), el("span", { class: "spark", text: `scale 0–${fmtN(max)}` }),
      el("span", { class: "num", text: "output" }), el("span", { class: "num", text: "context now" })));
    const sparkW = Math.max(160, width - 220 - 90 - 110 - 60);
    for (const s of d.sessions) {
      const L = s.latest || {};
      c.append(el("div", { class: "sm" },
        el("span", { class: "h", title: s.handle }, s.handle, el("small", { text: `${L.model || "?"} · ${fmtN(s.window.turns)} turns` }), bridgeLink(s.handle)),
        el("span", { class: "spark" }, spark(s, d, max, width < 1000 ? width - 40 : sparkW)),
        el("span", { class: "num", text: fmtN(s.window.output) }),
        el("span", { class: "num", text: fmtN(L.context) })));
    }
    c.append(tableView(["session", "output", "thinking", "input", "cache read", "cache write", "turns", "context now", "model"],
      d.sessions.map((s) => [s.handle, s.window.output, s.window.thinking, s.window.input, s.window.cache_read, s.window.cache_write, s.window.turns, (s.latest || {}).context ?? "", (s.latest || {}).model || ""])));
    return c;
  }

  // ---- matrix: sender × recipient, one-hue ramp ----
  function matrixCard(d) {
    const c = card("Who talks to whom", "messages from each sender (rows) to each address (columns)");
    if (!d.edges.length) { c.append(el("div", { class: "empty", text: "quiet bus" })); return c; }
    const rowsK = [...new Set(d.edges.map((e) => e.from))].sort((a, b) => (d.senders[b] || 0) - (d.senders[a] || 0));
    const colTot = {};
    for (const e of d.edges) colTot[e.to] = (colTot[e.to] || 0) + e.count;
    const cols = Object.keys(colTot).sort((a, b) => colTot[b] - colTot[a]).slice(0, 18);
    const m = {}; let max = 1;
    for (const e of d.edges) { m[e.from + "\u0000" + e.to] = e.count; max = Math.max(max, e.count); }
    const step = (v) => (v <= 0 ? 0 : Math.min(5, 1 + Math.floor(4 * Math.log(v) / Math.log(Math.max(2, max)) + 1e-9)));
    const t = el("table", { class: "mx" });
    t.append(el("thead", {}, el("tr", {}, el("th"), ...cols.map((k) => el("th", { title: k, text: k })))));
    const tb = el("tbody");
    for (const r of rowsK) {
      const tr = el("tr", {}, el("th", { title: r, text: r }));
      for (const k of cols) {
        const v = m[r + "\u0000" + k] || 0;
        const td = el("td", { class: "c" + step(v), tabindex: v ? 0 : -1, text: v ? String(v) : "·" });
        if (v) {
          const rows = [[String(v), `${r} → ${k}`]];
          td.addEventListener("pointermove", (e) => showTip(e, rows));
          td.addEventListener("pointerleave", hideTip);
        }
        tr.append(td);
      }
      tb.append(tr);
    }
    t.append(tb);
    c.append(el("div", { class: "mxwrap" }, t));
    c.append(el("div", { class: "ramp" }, el("span", { text: "fewer" }), ...[1, 2, 3, 4, 5].map((i) => el("span", { class: "sw", style: `background: var(--s${i})` })), el("span", { text: `more (max ${max}, log scale)` })));
    c.append(tableView(["from", "to", "messages"], d.edges.map((e) => [e.from, e.to, e.count])));
    return c;
  }

  function latencyCard(d) {
    const c = card("Response times", "your time from a request/ask/note arriving to answering or closing it");
    const kinds = ["ask", "request", "note"].filter((k) => d.latency[k] || Object.keys(d.outcomes).some((o) => o.startsWith(k + ":")));
    if (!kinds.length) { c.append(el("div", { class: "empty", text: "nothing addressed to you was resolved in this range" })); return c; }
    const t = el("table", { class: "tt" });
    t.append(el("thead", {}, el("tr", {}, ...["kind", "typical", "slow (p90)", "answered", "acked", "defaulted"].map((h) => el("th", { text: h })))));
    const tb = el("tbody");
    for (const k of kinds) {
      const L = d.latency[k] || {};
      tb.append(el("tr", {}, ...[k, fmtDur(L.median_s), fmtDur(L.p90_s), d.outcomes[k + ":answered"] || 0, d.outcomes[k + ":acked"] || 0, d.outcomes[k + ":defaulted"] || 0].map((v) => el("td", { text: String(v) }))));
    }
    t.append(tb);
    c.append(el("div", { class: "mxwrap" }, t));
    const sizes = Object.entries(d.avg_chars).sort((a, b) => b[1] - a[1]);
    if (sizes.length) {
      c.append(el("div", { class: "sub", style: "margin-top:14px", text: "average message length, characters" }));
      c.append(el("table", { class: "tt" }, el("tbody", {}, ...sizes.map(([k, v]) => el("tr", {}, el("td", { text: k }), el("td", { text: String(v) }))))));
    }
    return c;
  }

  function render() {
    if (!data) return;
    const body = $("#telemBody");
    const width = body.clientWidth || 900;
    const vol = card(`Messages per ${bucketLabel(data.bucket_s)}`, `everything on the bus · ${new Date(data.from).toLocaleString()} → now`, true);
    vol.append(volumeChart(data, width - 34));
    vol.append(tableView(["interval start", "messages", ...data.kinds],
      data.volume.filter((b) => b.total).map((b) => [new Date(b.t).toLocaleString(), b.total, ...data.kinds.map((k) => b.by_kind[k] || 0)])));
    body.replaceChildren(tiles(data), el("div", { class: "tgrid" }, vol, tokensCard(data, width - 34), matrixCard(data), latencyCard(data)));
    body.classList.remove("loading");
    $("#tUpd").textContent = `updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} · refreshes every minute`;
  }

  async function load() {
    const body = $("#telemBody");
    if (data) body.classList.add("loading");  // keep the frame while refetching
    try {
      const r = await fetch(`/api/telemetry?hours=${hours}`, { credentials: "same-origin" });
      if (!r.ok) throw new Error(r.status === 401 ? "signed out" : `HTTP ${r.status}`);
      data = await r.json();
      render();
    } catch (e) {
      body.classList.remove("loading");
      if (!data) body.replaceChildren(el("div", { class: "empty", text: `couldn't load telemetry: ${e.message}` }));
    }
  }

  function paintRange() {
    document.querySelectorAll("#tRange button").forEach((b) => b.setAttribute("aria-pressed", Number(b.dataset.h) === hours));
  }
  document.querySelectorAll("#tRange button").forEach((b) => (b.onclick = () => {
    hours = Number(b.dataset.h); try { localStorage.setItem("orc-telem-hours", String(hours)); } catch {}
    paintRange(); load();
  }));
  let rz = null;
  addEventListener("resize", () => { if (!visible) return; clearTimeout(rz); rz = setTimeout(render, 150); });

  window.Telem = {
    show() { visible = true; paintRange(); load(); clearInterval(timer); timer = setInterval(() => { if (!document.hidden) load(); }, 60000); },
    hide() { visible = false; clearInterval(timer); hideTip(); },
  };
})();
