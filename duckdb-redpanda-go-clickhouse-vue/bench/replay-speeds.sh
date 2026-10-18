#!/usr/bin/env bash
# Producer speed sweep: finds how fast this machine can fast-forward.
#
# Replays the whole day at each speed into the ticks-demo topic (the
# pipeline's own topics are not touched) and reports how far the producer
# fell behind its schedule. Results go to the terminal and, with a
# description of the machine, to bench/results/ as CSV, so runs on different
# hardware and operating systems can be compared.
#
# Usage: bench/replay-speeds.sh [SPEED ...]
#   SPEED           default: 1000 2000 4000 6000 8000 12000 16000 24000
#   RUNS=2          runs per speed
#   DAYS=081121     days to replay (their Parquet buckets must exist)
#   REPLAY_WORKERS, REPLAY_THREADS   passed on to the replayer when set
#   COMPOSE_FILE    e.g. compose.yaml:compose.perf.yaml on the Linux box
#
# A run is "ok" when the producer never fell more than 1% of the replayed
# span behind schedule (about 10 minutes of market time for Monday),
# "behind" when it did, and "too fast" when it took over 2% longer than
# planned. Runs on macOS, Linux and Windows (WSL2) with Docker and bash
# (3.2 or later).
set -euo pipefail
cd "$(dirname "$0")/.."

(( $# )) || set -- 1000 2000 4000 6000 8000 12000 16000 24000
set -- $(printf '%s\n' "$@" | sort -n)
runs=${RUNS:-2}
days=${DAYS:-081121}
partitions=${PARTITIONS:-32}
topic=ticks-demo

# --- The machine -------------------------------------------------------------
host_os() {
  case "$(uname -s)" in
    Darwin) echo "macOS $(sw_vers -productVersion)" ;;
    Linux)
      if grep -qi microsoft /proc/version 2>/dev/null; then
        echo "Windows WSL2 (kernel $(uname -r))"
      else
        (. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-Linux} (kernel $(uname -r))") || uname -sr
      fi ;;
    *) uname -sr ;;
  esac
}
host_cpu() {
  case "$(uname -s)" in
    Darwin) sysctl -n machdep.cpu.brand_string ;;
    *) awk -F': ' '/^(model name|Model|Hardware)/ { print $2; exit }' /proc/cpuinfo ;;
  esac
}
clean() { tr -d ',"' <<<"$1"; }  # keep CSV fields simple

docker compose up -d --wait redpanda >/dev/null 2>&1
read -r docker_cpus docker_mem docker_ver <<<"$(docker info --format '{{.NCPU}} {{.MemTotal}} {{.ServerVersion}}')"
docker_os=$(docker info --format '{{.OperatingSystem}}')
arch=$(docker info --format '{{.Architecture}}')
host=$(hostname -s 2>/dev/null || hostname)
os=$(clean "$(host_os)")
cpu=$(clean "$(host_cpu)")
cores=$(getconf _NPROCESSORS_ONLN)
mem_gib=$(awk -v b="$docker_mem" 'BEGIN { printf "%.1f", b / 1073741824 }')
compose_files=${COMPOSE_FILE:-compose.yaml}

extra=()
[[ -n "${REPLAY_WORKERS:-}" ]] && extra+=(-e "REPLAY_WORKERS=$REPLAY_WORKERS")
[[ -n "${REPLAY_THREADS:-}" ]] && extra+=(-e "REPLAY_THREADS=$REPLAY_THREADS")

echo "Host:    $host, $os, ${cpu:-unknown CPU}, $cores cores"
echo "Docker:  $docker_ver, $docker_os, $arch, $docker_cpus CPUs, $mem_gib GiB"
echo "Replay:  days $days, workers ${REPLAY_WORKERS:-4}, threads ${REPLAY_THREADS:-all}, compose $compose_files"
echo

# One unmeasured run at full speed first: the first run after Redpanda starts,
# with the Parquet files not yet in the page cache, lags for reasons that have
# nothing to do with the speed
echo "Warming up (one unmeasured run at full speed) ..."
docker compose exec -T redpanda rpk topic delete "$topic" >/dev/null 2>&1 || true
docker compose exec -T redpanda rpk topic create "$topic" -p "$partitions" >/dev/null
docker compose --profile replay run --rm --no-deps -e TICKS_TOPIC="$topic" -e DAYS="$days" \
  -e REPLAY_SPEED=max ${extra[@]+"${extra[@]}"} replayer >/dev/null 2>&1
echo

mkdir -p bench/results
csv=bench/results/replay-speeds-$host-$(date +%Y%m%d-%H%M).csv
echo "date,host,host_os,cpu,host_cores,docker_cpus,docker_mem_gib,docker_os,docker_version,arch,compose,days,workers,threads,speed,run,planned_s,took_s,ticks_per_s,max_lag_ms,lag_market_min,errors,verdict" >"$csv"

printf '%7s  %3s  %9s  %9s  %11s  %10s  %11s  %6s  %s\n' \
  speed run planned took ticks/s "max lag" "lag market" errors verdict
best=""
failed=0
for speed in "$@"; do
  all_ok=1
  for run in $(seq 1 "$runs"); do
    docker compose exec -T redpanda rpk topic delete "$topic" >/dev/null 2>&1 || true
    docker compose exec -T redpanda rpk topic create "$topic" -p "$partitions" >/dev/null
    summary=$(docker compose --profile replay run --rm --no-deps -e TICKS_TOPIC="$topic" -e DAYS="$days" \
      -e REPLAY_SPEED="$speed" ${extra[@]+"${extra[@]}"} replayer 2>&1 | grep 'summary speed=' || true)
    if [[ -z "$summary" ]]; then
      echo "replayer failed at speed $speed; run it by hand to see why:" >&2
      echo "  docker compose --profile replay run --rm --no-deps -e TICKS_TOPIC=$topic -e DAYS=$days -e REPLAY_SPEED=$speed replayer" >&2
      exit 1
    fi
    # summary speed=… ticks=… errors=… took_s=… ticks_per_s=… planned_s=… max_lag_ms=…
    row=$(awk -v s="$summary" -v speed="$speed" 'BEGIN {
      n = split(s, kv, " ")
      for (i = 1; i <= n; i++) if (split(kv[i], p, "=") == 2) v[p[1]] = p[2]
      verdict = "ok"
      if (v["max_lag_ms"] / 1000 > 0.01 * v["planned_s"]) verdict = "behind"
      if (v["took_s"] > 1.02 * v["planned_s"]) verdict = "too_fast"
      printf "%s %s %s %s %.1f %s %s", v["planned_s"], v["took_s"], v["ticks_per_s"], v["max_lag_ms"],
        v["max_lag_ms"] * speed / 60000, v["errors"], verdict
    }')
    read -r planned took tps lag lag_min errors verdict <<<"$row"
    verdict=${verdict/_/ }
    [[ "$verdict" == ok && "$errors" == 0 ]] || all_ok=0
    printf '%6s×  %3s  %8ss  %8ss  %11s  %8sms  %8s min  %6s  %s\n' \
      "$speed" "$run" "$planned" "$took" "$tps" "$lag" "$lag_min" "$errors" "$verdict"
    echo "$(date +%Y-%m-%dT%H:%M:%S),$host,$os,$cpu,$cores,$docker_cpus,$mem_gib,$(clean "$docker_os"),$docker_ver,$arch,$compose_files,$days,${REPLAY_WORKERS:-4},${REPLAY_THREADS:-all},$speed,$run,$planned,$took,$tps,$lag,$lag_min,$errors,$verdict" >>"$csv"
  done
  # The answer is the fastest speed below the first one that failed
  if (( all_ok && ! failed )); then best=$speed; else failed=1; fi
done
docker compose exec -T redpanda rpk topic delete "$topic" >/dev/null 2>&1 || true

echo
if [[ -n "$best" ]]; then
  echo "Fastest speed on schedule in every run (and every slower speed): ${best}×"
else
  echo "No tested speed kept its schedule in every run; try lower speeds."
fi
echo "Results: $csv"
