#!/usr/bin/env bash
# Showcase step 06: the UI in a browser.
#   edge 12  Vite dev server <-> gateway: /ws, /history, /signals, /symbols proxied unchanged
#   edge 13  browser <-> Vite: the Vue app, then its HTTP calls and its WebSocket
# Runs the gateway as step 05 does, the compose ui service (npm ci + Vite) in
# front of it, and a headless Chromium (Playwright) in the UI container's
# network, so the page is http://localhost:5173 as on a laptop. Once the page
# is live, step 03's records are produced again, as in step 05. Needs Docker, a
# .env and network access for npm. Starts Redpanda and ClickHouse if they are
# not running and stops them again at the end.
set -euo pipefail
cd "$(dirname "$0")/../.."
out=showcase/06-ui-and-browser/output
s3=showcase/03-signal-to-bars-and-signals/output
bars=showcase-live-bars_ema
signals=showcase-live-signals
gateway=showcase-gateway
ui=showcase-ui
browser=showcase-browser
partitions=$(sed -n 's/^PARTITIONS=//p' .env)
partitions=${partitions:-32}
playwright_image=$(sed -n 's/^PLAYWRIGHT_IMAGE=//p' .env)
playwright_image=${playwright_image:-mcr.microsoft.com/playwright:v1.63.0-noble}
playwright_version=$(sed -E 's/.*:v([0-9.]+).*/\1/' <<<"$playwright_image")  # the npm package must match the image

[ -f "$s3/bars_ema.jsonl" ] || { echo "error: $s3 is missing; run the steps before this one first" >&2; exit 1; }
if [ -n "$(docker compose ps -q --status running ui)" ]; then
  echo "error: the ui service is running and shares node_modules with this step; stop it first (docker compose stop ui)" >&2
  exit 1
fi

running_rp=$(docker compose ps -q --status running redpanda)
running_ch=$(docker compose ps -q --status running clickhouse)
docker compose up -d --wait redpanda clickhouse
cleanup() {
  docker rm -f "$browser" "$ui" "$gateway" >/dev/null 2>&1 || true
  [ -n "$running_ch" ] || docker compose stop clickhouse
  [ -n "$running_rp" ] || docker compose stop redpanda
}
trap cleanup EXIT
rpk() { docker compose exec -T redpanda rpk "$@"; }
ch() {
  docker compose exec -T clickhouse sh -c \
    'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' -- "$@"
}

nbars=$(wc -l <"$s3/bars_ema.jsonl" | tr -d ' ')
nsignals=$(wc -l <"$s3/signals.jsonl" | tr -d ' ')
have=$(ch -q "SELECT (SELECT count() FROM showcase.bars_ema), (SELECT count() FROM showcase.signals)" 2>/dev/null | tr '\t' ' ' || true)
[ "$have" = "$nbars $nsignals" ] || { echo "error: database showcase is missing or differs from step 03; run step 04 first" >&2; exit 1; }

for t in "$bars" "$signals"; do
  rpk topic delete "$t" >/dev/null 2>&1 || true
  rpk topic create "$t" -p "$partitions" -r 1 -c retention.ms=-1 >/dev/null
done
rm -rf "$out"
mkdir -p "$out"
docker rm -f "$browser" "$ui" "$gateway" >/dev/null 2>&1 || true

# The gateway, as in step 05
docker compose build --quiet gateway
docker compose run -d --no-deps --name "$gateway" \
  --env BARS_TOPIC="$bars" --env SIGNALS_TOPIC="$signals" --env GATEWAY_GROUP="$gateway" \
  --env "CLICKHOUSE_URL=http://clickhouse:8123/?database=showcase" \
  gateway >/dev/null
for _ in $(seq 30); do
  docker exec "$gateway" wget -qO- http://127.0.0.1:8000/healthz >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$gateway" wget -qO- http://127.0.0.1:8000/healthz >/dev/null

# The compose ui service, unchanged but for GATEWAY_URL: npm ci, then Vite
docker compose run -d --no-deps --name "$ui" --env GATEWAY_URL="http://$gateway:8000" ui >/dev/null
echo "waiting for npm ci and Vite in $ui ..."
for _ in $(seq 300); do
  docker exec "$ui" curl -sf -o /dev/null http://127.0.0.1:5173/ 2>/dev/null && break
  sleep 1
done
docker exec "$ui" curl -sf -o /dev/null http://127.0.0.1:5173/ || { docker logs "$ui" >&2; echo "error: Vite never answered" >&2; exit 1; }

# Edge 12: the same requests through the Vite proxy and straight to the gateway
{
  for path in /symbols "/history?symbol=A1PHBB.ETR" "/signals?limit=5000"; do
    via=$(docker exec "$ui" curl -sf "http://127.0.0.1:5173$path" | cksum | cut -d" " -f1,2)
    direct=$(docker exec "$ui" curl -sf "http://$gateway:8000$path" | cksum | cut -d" " -f1,2)
    printf '%-28s via Vite %s  gateway %s  %s\n' "$path" "$via" "$direct" "$([ "$via" = "$direct" ] && echo identical || echo DIFFERENT)"
  done
} | tee "$out/proxy-check.txt"

# Edge 13: the browser, in the UI container's network namespace
docker run -d --name "$browser" --network "container:$ui" --user "$(id -u):$(id -g)" \
  --env HOME=/tmp --env OUT=/showcase/06-ui-and-browser/output \
  --env WANT_BARS="$nbars" --env WANT_SIGNALS="$nsignals" \
  --volume "$PWD/showcase:/showcase" "$playwright_image" sh -c "
    mkdir -p /tmp/pw && cd /tmp/pw &&
    npm install --silent --no-save --no-audit --no-fund playwright@$playwright_version &&
    cp /showcase/06-ui-and-browser/browser.mjs . && exec node browser.mjs" >/dev/null
echo "waiting for the browser (installs playwright@$playwright_version first) ..."
for _ in $(seq 300); do
  [ -f "$out/.ready" ] && break
  [ "$(docker inspect -f '{{.State.Running}}' "$browser")" = true ] || break
  sleep 1
done
[ -f "$out/.ready" ] || { docker logs "$browser" >&2; echo "error: the page never got live" >&2; exit 1; }
rm "$out/.ready"

# Step 03's records again, as in step 05
produce() {  # topic, records.jsonl
  sed -E 's/^\{"partition":([0-9]+),"offset":[0-9]+,"timestamp":[0-9]+,"key":"([^"]*)","value":(.*)\}$/\1 \2 \3/' "$2" |
    rpk topic produce "$1" -p 0 -f '%p %k %v\n' >/dev/null
}
produce "$bars" "$s3/bars_ema.jsonl"
produce "$signals" "$s3/signals.jsonl"

status=$(docker wait "$browser")
docker logs "$browser" >"$out/browser.log" 2>&1
cat "$out/browser.log"
[ "$status" = 0 ] || { echo "error: the browser failed" >&2; exit 1; }
docker logs "$ui" >"$out/ui.log" 2>&1
docker stop "$gateway" >/dev/null
docker logs "$gateway" >"$out/gateway.log" 2>&1

# What the browser received through Vite, against the gateway's own answers (step 05)
s5=showcase/05-gateway/output/http
if [ -d "$s5" ]; then
  for f in symbols history signals; do
    printf 'browser http/%s.json and step 05: %s\n' "$f" "$(cmp -s "$out/http/$f.json" "$s5/$f.json" && echo identical || echo DIFFERENT)"
  done | tee -a "$out/proxy-check.txt"
fi
echo "screenshots: $out/ui.png, $out/price-chart.png, $out/history-only.png"
