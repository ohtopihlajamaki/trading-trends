#!/usr/bin/env bash
# Showcase step 05: the gateway, with a client that does what the UI does.
#   edge 11  ClickHouse (database showcase, from step 04) -> /symbols, /history, /signals
#   edge 10  bars_ema and signals records -> gateway -> WebSocket frames
# The gateway reads its topics from the live end, so the script starts it and
# the client first, then produces step 03's records again, unchanged, into
# fresh demo topics. Needs Docker and a .env (copy .env.example). Starts
# Redpanda and ClickHouse if they are not running and stops them again at the
# end. The pipeline's own topics, groups and database are not touched.
set -euo pipefail
cd "$(dirname "$0")/../.."
out=showcase/05-gateway/output
s3=showcase/03-signal-to-bars-and-signals/output
bars=showcase-live-bars_ema
signals=showcase-live-signals
gateway=showcase-gateway
client=showcase-client
partitions=$(sed -n 's/^PARTITIONS=//p' .env)
partitions=${partitions:-32}
node_image=$(sed -n 's/^NODE_IMAGE=//p' .env)

[ -f "$s3/bars_ema.jsonl" ] || { echo "error: $s3 is missing; run the steps before this one first" >&2; exit 1; }

running_rp=$(docker compose ps -q --status running redpanda)
running_ch=$(docker compose ps -q --status running clickhouse)
docker compose up -d --wait redpanda clickhouse
cleanup() {
  docker rm -f "$client" "$gateway" >/dev/null 2>&1 || true
  [ -n "$running_ch" ] || docker compose stop clickhouse
  [ -n "$running_rp" ] || docker compose stop redpanda
}
trap cleanup EXIT
rpk() { docker compose exec -T redpanda rpk "$@"; }
ch() {
  docker compose exec -T clickhouse sh -c \
    'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' -- "$@"
}

# The history comes from step 04's database
have=$(ch -q "SELECT (SELECT count() FROM showcase.bars_ema), (SELECT count() FROM showcase.signals)" 2>/dev/null | tr '\t' ' ' || true)
nbars=$(wc -l <"$s3/bars_ema.jsonl" | tr -d ' ')
nsignals=$(wc -l <"$s3/signals.jsonl" | tr -d ' ')
want="$nbars $nsignals"
[ "$have" = "$want" ] || { echo "error: database showcase is missing or differs from step 03; run step 04 first" >&2; exit 1; }

# Fresh live topics, like redpanda/create-topics.sh makes them
for t in "$bars" "$signals"; do
  rpk topic delete "$t" >/dev/null 2>&1 || true
  rpk topic create "$t" -p "$partitions" -r 1 -c retention.ms=-1 >/dev/null
done

# The gateway as built from this checkout. Only the topics, the group and the
# database differ from the compose service; the gateway's queries keep the
# ?database= of CLICKHOUSE_URL
docker compose build --quiet gateway
rm -rf "$out"
mkdir -p "$out"
docker rm -f "$client" "$gateway" >/dev/null 2>&1 || true
docker compose run -d --no-deps --name "$gateway" \
  --env BARS_TOPIC="$bars" --env SIGNALS_TOPIC="$signals" --env GATEWAY_GROUP="$gateway" \
  --env "CLICKHOUSE_URL=http://clickhouse:8123/?database=showcase" \
  gateway >/dev/null
for _ in $(seq 30); do
  docker exec "$gateway" wget -qO- http://127.0.0.1:8000/healthz >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$gateway" wget -qO- http://127.0.0.1:8000/healthz >/dev/null  # fails the script if never healthy

# The client, on the compose network
network=$(docker inspect "$gateway" -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run -d --name "$client" --network "$network" --user "$(id -u):$(id -g)" \
  --volume "$PWD/showcase:/showcase" \
  --env OUT=/showcase/05-gateway/output --env GATEWAY="http://$gateway:8000" --env SYMBOL=A1PHBB.ETR \
  --env WANT_BARS="$nbars" --env WANT_SIGNALS="$nsignals" \
  "$node_image" node /showcase/05-gateway/client.mjs >/dev/null
for _ in $(seq 30); do
  [ -f "$out/.ready" ] && break
  sleep 1
done
[ -f "$out/.ready" ] || { docker logs "$client" >&2; echo "error: the client never got live" >&2; exit 1; }
rm "$out/.ready"

# Step 03's records again, with the same partition, key and value bytes
produce() {  # topic, records.jsonl
  sed -E 's/^\{"partition":([0-9]+),"offset":[0-9]+,"timestamp":[0-9]+,"key":"([^"]*)","value":(.*)\}$/\1 \2 \3/' "$2" |
    rpk topic produce "$1" -p 0 -f '%p %k %v\n' >/dev/null
}
produce "$bars" "$s3/bars_ema.jsonl"
produce "$signals" "$s3/signals.jsonl"

status=$(docker wait "$client")
docker logs "$client" >"$out/client.log" 2>&1
cat "$out/client.log"
[ "$status" = 0 ] || { echo "error: the client failed" >&2; exit 1; }

docker exec "$gateway" wget -qO- http://127.0.0.1:8000/metrics |
  grep -E '^gateway_(records_total|ws_clients|ws_frames_total|ws_dropped_total|http_seconds_count)' >"$out/metrics.txt"
docker stop "$gateway" >/dev/null
docker logs "$gateway" >"$out/gateway.log" 2>&1

# The gateway forwards record values as they are: each must be in a frame, byte for byte
missing=0
while read -r v; do
  grep -qF "$v" "$out/ws.jsonl" || { echo "not in any frame: $v" >&2; missing=$((missing + 1)); }
done < <(sed -E 's/^\{"partition":[0-9]+,"offset":[0-9]+,"timestamp":[0-9]+,"key":"[^"]*","value":(.*)\}$/\1/' \
  "$s3/bars_ema.jsonl" "$s3/signals.jsonl")
[ "$missing" = 0 ] || { echo "error: $missing records changed on the way" >&2; exit 1; }
echo "all $nbars bars and $nsignals signals of step 03 reached the client byte for byte;" \
  "$(grep -c '"dir":"received"' "$out/ws.jsonl") frames in $out/ws.jsonl"
