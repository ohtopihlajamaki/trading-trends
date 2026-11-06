#!/usr/bin/env bash
# CPU profile of one running signal instance: where its time per tick goes
# (JSON decoding, window state, produce, commit). Run it while the instance is
# busy, e.g. in a second terminal during bench/signal-drain.sh or a replay.
#
# Usage: bench/profile-signal.sh [SECONDS] [INSTANCE]    defaults: 30, 1
#   COMPOSE_PROJECT_NAME / COMPOSE_FILE as for the run being profiled
#
# Saves bench/results/profile-signal-<host>-<stamp>.pb.gz and prints the
# top functions; open it in detail with
#   docker run --rm -p 8081:8081 -v "$PWD/bench/results:/w" -w /w <GO_IMAGE> \
#     go tool pprof -http=0.0.0.0:8081 <file>
set -euo pipefail
cd "$(dirname "$0")/.."
secs=${1:-30}
idx=${2:-1}

mkdir -p bench/results
out=bench/results/profile-signal-$(hostname -s 2>/dev/null || hostname)-$(date +%Y%m%d-%H%M%S).pb.gz
echo "Profiling signal instance $idx for ${secs}s ..."
docker compose exec -T --index "$idx" signal \
  wget -qO- "http://127.0.0.1:9100/debug/pprof/profile?seconds=$secs" >"$out"
echo "Profile: $out"

go_image=$(awk -F= '$1 == "GO_IMAGE" { print $2 }' .env)
docker run --rm -v "$PWD/bench/results:/w:ro" -w /w "${go_image:?GO_IMAGE missing from .env}" \
  go tool pprof -top -nodecount=25 "$(basename "$out")"
