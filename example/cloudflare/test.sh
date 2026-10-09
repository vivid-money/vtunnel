#!/bin/bash
#
# Runs the Cloudflare example locally and checks it end to end:
#
#   wrangler dev (https, a throwaway certificate)
#     ◀═ vtunnel client, yamux-wss, signed upgrade
#          └─▶ a stand-in "internal service" on 127.0.0.1
#
# The client is a test build (-tags vtunnel_testing), the only kind that can
# be told to trust the throwaway certificate (-test-ca); against a deployed
# Worker the ordinary binary trusts Cloudflare's certificate as it is.
#
# Usage: ./example/cloudflare/test.sh
#
set -euo pipefail

cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)

GREEN='\033[0;32m'
RED='\033[0;31m'
DIM='\033[2m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC} $1"; }
fail() { echo -e "${RED}FAIL${NC} $1"; echo -e "${DIM}--- wrangler:${NC}"; tail -20 "$TMP/wrangler.log"; echo -e "${DIM}--- client:${NC}"; tail -20 "$TMP/client.log"; exit 1; }
log()  { echo -e "${DIM}$1${NC}"; }

TMP=$(mktemp -d)
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$TMP" .dev.vars
}
trap cleanup EXIT

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
PORT=$(free_port)
SERVICE_PORT=$(free_port)
URL="https://127.0.0.1:$PORT"

log "installing..."
npm install --no-audit --no-fund --silent >/dev/null 2>&1

log "certificate and upgrade key..."
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout "$TMP/key.pem" -out "$TMP/cert.pem" 2>/dev/null
KEY=$(openssl rand -base64 32)
echo "VTUNNEL_UPGRADE_KEY=\"$KEY\"" > .dev.vars

log "building the client (test build)..."
(cd "$ROOT" && go build -tags vtunnel_testing -o "$TMP/vtunnel" ./cmd/vtunnel)

log "starting the internal service on :$SERVICE_PORT..."
python3 - "$SERVICE_PORT" > "$TMP/service.log" 2>&1 <<'PY' &
import http.server, json, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        print(f"{self.path} {self.headers.get('Authorization')}", flush=True)
        body = json.dumps({"service": "api.internal", "path": self.path,
                           "authorization": self.headers.get("Authorization")}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
PIDS+=($!)

log "starting wrangler dev on $URL..."
npx wrangler dev --local-protocol https --https-key-path "$TMP/key.pem" --https-cert-path "$TMP/cert.pem" \
  --ip 127.0.0.1 --port "$PORT" --show-interactive-dev-session=false > "$TMP/wrangler.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 60); do curl -sk "$URL/" >/dev/null 2>&1 && break; sleep 1; done
grep -q "Ready on" "$TMP/wrangler.log" || fail "wrangler dev did not start"

log "starting the controller..."
"$TMP/vtunnel" client \
  -protocol yamux-wss \
  -server "wss://127.0.0.1:$PORT/tunnel" \
  -upgrade-key "$KEY" \
  -test-ca "$TMP/cert.pem" \
  -mitm-ca "$TMP/mitm-ca.pem" \
  -forward "api.internal=http://127.0.0.1:$SERVICE_PORT" \
  -H "Authorization: Bearer from-the-controller" \
  > "$TMP/client.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do grep -q "Forward OK" "$TMP/client.log" && break; sleep 0.5; done
grep -q "Forward OK" "$TMP/client.log" || fail "the controller did not connect"
pass "controller connected over yamux-wss"

check() {
  local route=$1 out
  out=$(curl -sk "$URL$route")
  echo "$out" | grep -q '"authorization": "Bearer from-the-controller"' || fail "$route: $out"
  pass "$route reached the internal service, credential injected by the controller"
}
check /demo/fetch
check /demo/app

# The service's own log, not just the answer: two requests arrived, each with
# the credential.
seen=$(grep -c "^/hello Bearer from-the-controller$" "$TMP/service.log" || true)
[ "$seen" = "2" ] || fail "the service logged $seen requests with the credential, want 2"
pass "the internal service received both requests, credential and all"

headers=$(curl -sk -D - -o /dev/null "$URL/demo/fetch?url=https://example.com/")
echo "$headers" | grep -q "^HTTP/[0-9.]* 502" || fail "a host no controller routes: $headers"
echo "$headers" | grep -qi "^x-vtunnel-error: no-route" || fail "a host no controller routes: no x-vtunnel-error"
pass "a host no controller routes gets 502 no-route from the tunnel object"

code=$(curl -sk -o /dev/null -w '%{http_code}' -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" "$URL/tunnel")
[ "$code" = "401" ] || fail "an unsigned upgrade got $code"
pass "an unsigned upgrade is refused (401)"

echo -e "\n${GREEN}All checks passed${NC}"
