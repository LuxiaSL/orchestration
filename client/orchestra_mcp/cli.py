"""orc — the bus from a terminal.

    orc inbox                      open asks/requests/notes addressed to the operator
    orc tail [--after N]           follow the bus live
    orc answer <id> <text...>      answer an ask/request (as the operator)
    orc ack <id> [text...]         close a note/request without an answer
    orc notice <@to> <text...>     send a notice (@all or @handle)
    orc halt [@to] [reason...]     ask sessions to pause      orc resume [@to] [text...]
    orc who                        live sessions

Uses `operator_token` from ~/.config/orchestra/config.json (or $ORCHESTRA_OPERATOR_TOKEN)
for the operator's verbs; read-only verbs work with the agent token too.
"""

from __future__ import annotations

import json
import os
import sys

import httpx

from .link import CONFIG_PATH, Config, ConfigError


def _client(operator: bool) -> httpx.Client:
    cfg = Config.load()
    token = cfg.token
    if operator:
        token = os.environ.get("ORCHESTRA_OPERATOR_TOKEN") or Config.saved().get("operator_token")
        if not token:
            raise ConfigError(f"this verb needs operator_token in {CONFIG_PATH} or $ORCHESTRA_OPERATOR_TOKEN")
    return httpx.Client(base_url=cfg.url, headers={"Authorization": f"Bearer {token}"}, timeout=httpx.Timeout(15, read=None))


def _post(body: dict) -> None:
    with _client(operator=True) as c:
        r = c.post("/api/events", json={"from": "operator", **body})
        if r.status_code >= 400:
            sys.exit(f"orc: {r.status_code} {r.text}")
        ev = r.json()
        print(f"#{ev['seq']} {ev['kind']} {ev['id']} → {ev['to']}")


def _line(ev: dict) -> str:
    ref = f" re {ev['ref']}" if ev.get("ref") else ""
    return f"#{ev['seq']:<5} {ev['ts'][11:19]} {ev['from']:>16} → {ev['to']:<16} {ev['kind']:<9} {ev.get('id','')}{ref}  {ev.get('body','')}"


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    verb, rest = args[0], args[1:]
    try:
        if verb == "inbox":
            with _client(operator=False) as c:
                inbox = c.get("/api/state").raise_for_status().json()["inbox"]
            for t in inbox["open"]:
                dl = f" (default '{t.get('default')}' at {t['deadline'][11:16]}Z)" if t["kind"] == "ask" else ""
                print(f"{t['kind']:<8}{t['id']}  {t['from']}: {t['body']}{dl}")
            if not inbox["open"]:
                print("inbox zero ✨")
        elif verb == "tail":
            after = int(rest[1]) if len(rest) == 2 and rest[0] == "--after" else None
            with _client(operator=False) as c:
                params = {"after": after} if after is not None else {}
                with c.stream("GET", "/api/stream", params=params) as r:
                    for line in r.iter_lines():
                        if line.startswith("data: "):
                            print(_line(json.loads(line[6:])), flush=True)
        elif verb == "who":
            with _client(operator=False) as c:
                for p in c.get("/api/state").raise_for_status().json()["presence"]:
                    mark = "●" if p["live"] else "○"
                    print(f"{mark} {p['handle']:<40} {p.get('status') or '':<6} push={'y' if p.get('channel') else 'n'}  {p.get('cwd') or ''}")
        elif verb in ("answer", "ack") and rest:
            _post({"kind": verb, "ref": rest[0], "body": " ".join(rest[1:])})
        elif verb == "notice" and len(rest) >= 2:
            _post({"kind": "notice", "to": rest[0], "body": " ".join(rest[1:])})
        elif verb in ("halt", "resume"):
            to = rest[0] if rest and rest[0].startswith("@") else "@all"
            body = " ".join(rest[1:] if rest and rest[0].startswith("@") else rest)
            _post({"kind": verb, "to": to, "body": body})
        else:
            print(__doc__)
            return 2
    except ConfigError as exc:
        sys.exit(f"orc: {exc}")
    except httpx.HTTPError as exc:
        sys.exit(f"orc: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
