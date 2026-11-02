#!/usr/bin/env bash
# Push the server to a VPS running caddy + systemd, and (re)start it. Idempotent.
# Settings come from deploy/local.env (see local.env.example) or the environment. Another bus on the same box (its own
# operator, domain and data) is the same script with its own env file: ./deploy/deploy.sh deploy/ada.env
set -euo pipefail
cd "$(dirname "$0")/.."
ENV_FILE=${1:-deploy/local.env}
[[ -f "$ENV_FILE" ]] && set -a && source "$ENV_FILE" && set +a
: "${HOST:?set HOST (ssh target), e.g. in deploy/local.env}"
: "${DOMAIN:?set DOMAIN, e.g. orchestra.example.com}"
: "${OPERATOR:?set OPERATOR, your handle on the bus}"
VAPID_SUB=${VAPID_SUB:-mailto:orchestra@$DOMAIN}
NAME=${NAME:-orchestra}   # the instance: its system user, unit, /opt/$NAME, /var/lib/$NAME and caddy include
PORT=${PORT:-7460}        # its localhost port behind caddy; each bus on a box needs its own
[[ "$NAME" =~ ^[a-z][a-z0-9-]{0,31}$ ]] || { echo "NAME must be lowercase letters, digits and -" >&2; exit 2; }
[[ "$PORT" =~ ^[0-9]{4,5}$ ]] || { echo "PORT must be a number" >&2; exit 2; }
# The client build this repo would load: sessions reporting a different one need `/mcp reconnect orchestra`.
CLIENT_BUILD=$(cd client && python3 -I -c 'import hashlib, pathlib
h = hashlib.sha256()
for f in sorted(pathlib.Path("orchestra_mcp").glob("*.py")): h.update(f.name.encode() + f.read_bytes())
print(h.hexdigest()[:8])')

render() { sed -e "s|@NAME@|$NAME|g" -e "s|@PORT@|$PORT|g" -e "s|@DOMAIN@|$DOMAIN|g" -e "s|@OPERATOR@|$OPERATOR|g" -e "s|@VAPID_SUB@|$VAPID_SUB|g" -e "s|@CLIENT_BUILD@|$CLIENT_BUILD|g" "$1"; }

# Before writing anything: another bus already on this port would answer this one's health check (and lose its traffic).
other=$(ssh "$HOST" "grep -l '^Environment=ORCHESTRA_PORT=$PORT\$' /etc/systemd/system/*.service 2>/dev/null | grep -v '/$NAME.service\$'" || true)
[[ -z "$other" ]] || { echo "port $PORT is already used by $other; pick another PORT" >&2; exit 3; }
ssh "$HOST" "mkdir -p /opt/$NAME/server"
rsync -az --delete --exclude .venv --exclude __pycache__ --exclude .pytest_cache --exclude tests \
  server/ "$HOST:/opt/$NAME/server/"
render deploy/orchestra.service.in | ssh "$HOST" "cat > /etc/systemd/system/$NAME.service"
render deploy/orchestra.caddy.in | ssh "$HOST" "cat > /etc/caddy/$NAME.caddy"
# Optional branding (fonts, brand.css): deploy/brand/ is gitignored and served as /brand.css + /brand/<file>.
if [[ -d deploy/brand ]]; then
  ssh "$HOST" "install -d -o $NAME -g $NAME -m 0755 /var/lib/$NAME/brand" 2>/dev/null || ssh "$HOST" "mkdir -p /var/lib/$NAME/brand"
  rsync -az --delete --chown=$NAME:$NAME deploy/brand/ "$HOST:/var/lib/$NAME/brand/"
fi
ssh "$HOST" NAME="$NAME" PORT="$PORT" bash -s <<'REMOTE'
set -euo pipefail
id "$NAME" >/dev/null 2>&1 || useradd --system --home "/var/lib/$NAME" --shell /usr/sbin/nologin "$NAME"
install -d -o "$NAME" -g "$NAME" -m 0700 "/var/lib/$NAME"
# System python: the unit's ProtectHome would hide a uv-managed python under /root.
cd "/opt/$NAME/server" && uv sync --no-dev --frozen --python /usr/bin/python3 -q
if ! grep -q "^import /etc/caddy/$NAME.caddy" /etc/caddy/Caddyfile; then
  cp /etc/caddy/Caddyfile "/etc/caddy/Caddyfile.bak-$NAME-$(date -u +%Y%m%dT%H%M%SZ)"
  printf '\nimport /etc/caddy/%s.caddy\n' "$NAME" >> /etc/caddy/Caddyfile
fi
caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1
systemctl daemon-reload
systemctl enable -q "$NAME"
systemctl restart "$NAME"
systemctl reload caddy
for i in $(seq 1 20); do curl -fsS "localhost:$PORT/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
systemctl is-active "$NAME"
curl -fsS "localhost:$PORT/healthz"; echo
echo "operator token: sudo cat /var/lib/$NAME/tokens.json  (key \"operator\"; agents use \"agent\")"
REMOTE
