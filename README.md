# orchestra

orchestra is a thin coordination layer for one person running several [Claude Code](https://claude.com/claude-code) sessions in parallel.

- The sessions share one append-only **bus**.
- The person, called the *operator*, gets one **inbox page** with phone push.
- A message addressed to a session is **pushed into it as a new turn, even while it is idle**. No polling, no tmux, no supervisor: sessions stay in whatever terminals you already use.

It is deliberately small: one JSONL file, a small FastAPI server and a per-session MCP server. It is not an autonomous orchestrator. It keeps a human in the loop without making anyone wait on them.

## Three invariants

1. **No session ever hard-blocks on the human.** A blocking ask must carry a default and a deadline. When the deadline passes, the session is told to take the default and log it. This holds even when the bus is unreachable: the session side keeps its own copy of each open ask and defaults locally.
2. **All coordination goes through the bus**, so everyone, including the human, can read everything.
3. **The bus is append-only.** Nothing is edited or deleted; replies and resolutions are new events. The history is the audit trail.

## How a message moves

```
session ──tool call──▶ orchestra-mcp ──POST /api/events──▶ server ──▶ bus.jsonl
                                                            │
       ◀──claude/channel turn── orchestra-mcp ◀──SSE /api/stream──┤
                                                            └──▶ inbox page + Web Push
```

Every session runs its own `orchestra-mcp`. Each one holds an open SSE stream and pushes events addressed to its session as a `claude/channel` notification. Claude Code delivers that as a turn, even when the session was idle.

## Messages to the human

| Tool | For | Blocks? | Phone push |
|---|---|---|---|
| `note_human` | an FYI for the operator's eyes only | no | batched digest |
| `request_human` | a question that isn't urgent; the answer is pushed back whenever it comes | no | batched digest |
| `ask_human` | something irreversible, cross-boundary, or a taste call. Requires `default` and `deadline_minutes` | never idle: at the deadline the session takes the default | immediate, with a **Take default** button on the notification |

`request_human` and `ask_human` can also present choices, the way Claude Code's AskUserQuestion does:

- `options`: 2 to 8 options, each with a `label`, a `description` (the tradeoff) and an optional `recommended` flag;
- `multi_select`, to allow picking several;
- `context`, a note shown above the options.

On an ask, `default` must be one of the option labels. On the page, the operator picks one or more options and can add a comment, or answers in their own words instead. **Ask for more info** sends a reply back to the session and leaves the question open, and the back-and-forth shows inside the card. The session receives the pick as `choice`, with any comment in `body`.

Between sessions, use `post(to='@handle'|'@all', kind=…)`:

- `status` is routine progress and is never pushed to anyone.
- `info` is for news that is urgent for other sessions. It is pushed, including when sent to `@all`.
- `request` is a question for a peer. The peer closes it with `kind=answer, ref=<id>`.

Sessions also get `read` (catch up on the bus), `check` (the state of an ask or request) and `whoami`.

From the page (or the `orc` CLI), the operator can do the following. Only the operator's token can send these, so they always come from the operator.

- Answer, ack or "take default" on any thread.
- Send a notice to `@all` or to one session.
- Use **Halt all** and **Resume**. A halted session finishes its current step, posts a status and waits for resume.

## Identity

A session's handle is its Claude Code session name, slugified. orchestra reads it from `~/.claude/sessions/<pid>.json` by walking up the MCP server's parent processes. So `claude -n api-refactor` is `@api-refactor`, and `/rename` changes the handle. `ORCHESTRA_HANDLE` overrides it.

## Setup

### Server (any box with caddy + systemd)

```bash
cp deploy/local.env.example deploy/local.env   # HOST, DOMAIN, OPERATOR, VAPID_SUB
./deploy/deploy.sh
```

On the target, this does the following:

- Creates an `orchestra` system user.
- Puts the code in `/opt/orchestra` and state in `/var/lib/orchestra`.
- Installs a hardened `orchestra.service`.
- Adds one Caddy `import`, backing up the Caddyfile first.
- Validates Caddy, restarts the service and checks `/healthz`.

On first boot the server writes `tokens.json` (mode 0600) with two tokens:
- `agent`, for sessions. It can't post as the operator, send notices, or halt.
- `operator`, for you and the page.

To run it locally instead: `cd server && ORCHESTRA_OPERATOR=ada ORCHESTRA_COOKIE_SECURE=0 uv run orchestra-server` (port 7460).

### Sessions (each machine you run Claude Code on)

```bash
mkdir -p ~/.config/orchestra && chmod 700 ~/.config/orchestra
cat > ~/.config/orchestra/config.json <<'EOF'
{"url": "https://orchestra.example.com", "token": "<agent token>",
 "operator": "ada", "operator_token": "<operator token, only needed for the orc CLI>"}
EOF
chmod 600 ~/.config/orchestra/config.json

claude mcp add -s user orchestra -- uv run --directory /path/to/orchestra/client orchestra-mcp

# channels are a research preview; locally registered servers need the development flag:
alias claude-code='claude --dangerously-load-development-channels server:orchestra'
```

**Updating the client.** After changing the client code, run `/mcp reconnect orchestra` in each session. That restarts only orchestra's MCP process, so the session keeps its conversation, picks up the new code and re-registers the push lane. It also works from the Claude app over Remote Control. Each client reports a build hash with its heartbeat, and `deploy.sh` records the hash of the client code that it shipped. The page marks any live session still on older code with **reconnect**.

A session launched without the channel flag still has the tools, but nothing is pushed into it. The page marks such sessions **no push**.

### Phone

Open the page, sign in with the operator token (it's stored as a cookie), and tap **Enable push**, then **send test push**. On Android this works in Chrome directly. On iOS, add the page to your home screen first.

## The `orc` CLI

```
uv run --directory client orc inbox | tail | who
uv run --directory client orc answer <id> <text> | ack <id> | notice @to <text> | halt [@to] | resume [@to]
```

## Push tiers

| Event | Delivery |
|---|---|
| `ask` to the operator | immediate, high urgency |
| `request`, `note`, `defaulted` | one digest push about 3 minutes after the first one (`ORCHESTRA_DIGEST_S`) |
| everything else | page only |

Payloads use Web Push with VAPID and are encrypted end to end to the subscribed browser.

## A trap worth knowing: MCP protocol era vs. channels

Claude Code 2.1.31x opens MCP connections in the 2026-07-28 ("modern") protocol era when the server supports it. On a modern connection it silently drops the `claude/channel` capability, so no push ever arrives. The only trace is this line in `~/.cache/claude-cli-nodejs/<project>/mcp-logs-<server>/*.jsonl`:

```
Channel notifications skipped: server did not declare claude/channel capability
```

The Python SDK's `MCPServer.run("stdio")` (mcp ≥ 2.3) serves both eras. So `orchestra-mcp` drives `mcp.server.runner.serve_loop` directly, which serves only the legacy `initialize` handshake. The host falls back to it, and the log then shows `protocolEra: legacy` → `Channel notifications registered`. See `run_legacy_stdio` in `client/orchestra_mcp/server.py`. Any other Python channel server will hit the same trap.

## Layout

```
server/   FastAPI app: bus, threads, deadline sweeper, SSE, presence, Web Push, the page (static/)
client/   orchestra-mcp (per-session MCP + channel push) and the orc CLI
deploy/   systemd unit + caddy templates, deploy.sh
```

Tests: `cd server && uv run pytest`, `cd client && uv run pytest`.

## License

MIT
