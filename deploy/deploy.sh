#!/usr/bin/env bash
# Push the server to a VPS running caddy + systemd, and (re)start it. Idempotent.
# Settings come from deploy/local.env (see local.env.example) or the environment.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f deploy/local.env ]] && set -a && source deploy/local.env && set +a
: "${HOST:?set HOST (ssh target), e.g. in deploy/local.env}"
: "${DOMAIN:?set DOMAIN, e.g. orchestra.example.com}"
: "${OPERATOR:?set OPERATOR, your handle on the bus}"
VAPID_SUB=${VAPID_SUB:-mailto:orchestra@$DOMAIN}

render() { sed -e "s|@DOMAIN@|$DOMAIN|g" -e "s|@OPERATOR@|$OPERATOR|g" -e "s|@VAPID_SUB@|$VAPID_SUB|g" "$1"; }

ssh "$HOST" "mkdir -p /opt/orchestra/server"
rsync -az --delete --exclude .venv --exclude __pycache__ --exclude .pytest_cache --exclude tests \
  server/ "$HOST:/opt/orchestra/server/"
render deploy/orchestra.service.in | ssh "$HOST" "cat > /etc/systemd/system/orchestra.service"
render deploy/orchestra.caddy.in | ssh "$HOST" "cat > /etc/caddy/orchestra.caddy"
# Optional branding (fonts, brand.css): deploy/brand/ is gitignored and served as /brand.css + /brand/<file>.
if [[ -d deploy/brand ]]; then
  ssh "$HOST" "install -d -o orchestra -g orchestra -m 0755 /var/lib/orchestra/brand" 2>/dev/null || ssh "$HOST" "mkdir -p /var/lib/orchestra/brand"
  rsync -az --delete --chown=orchestra:orchestra deploy/brand/ "$HOST:/var/lib/orchestra/brand/"
fi
ssh "$HOST" bash -s <<'REMOTE'
set -euo pipefail
id orchestra >/dev/null 2>&1 || useradd --system --home /var/lib/orchestra --shell /usr/sbin/nologin orchestra
install -d -o orchestra -g orchestra -m 0700 /var/lib/orchestra
# System python: the unit's ProtectHome would hide a uv-managed python under /root.
cd /opt/orchestra/server && uv sync --no-dev --frozen --python /usr/bin/python3 -q
if ! grep -q '^import /etc/caddy/orchestra.caddy' /etc/caddy/Caddyfile; then
  cp /etc/caddy/Caddyfile "/etc/caddy/Caddyfile.bak-orchestra-$(date -u +%Y%m%dT%H%M%SZ)"
  printf '\nimport /etc/caddy/orchestra.caddy\n' >> /etc/caddy/Caddyfile
fi
caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1
systemctl daemon-reload
systemctl enable -q orchestra
systemctl restart orchestra
systemctl reload caddy
for i in $(seq 1 20); do curl -fsS localhost:7460/healthz >/dev/null 2>&1 && break; sleep 0.5; done
systemctl is-active orchestra
curl -fsS localhost:7460/healthz; echo
echo "operator token: sudo cat /var/lib/orchestra/tokens.json  (key \"operator\"; agents use \"agent\")"
REMOTE
