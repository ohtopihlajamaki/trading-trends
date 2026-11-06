# Helpers shared by the bench scripts; source it after cd-ing to the
# pipeline directory. Works with bash 3.2 (macOS) and later.

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

# --- The pipeline ------------------------------------------------------------
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }  # date +%N is not portable
ch() {
  docker compose exec -T clickhouse sh -c \
    'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' -- "$@"
}
offsets() {  # sum of a topic's high watermarks
  docker compose exec -T redpanda rpk topic describe "$1" -p | awk 'NR > 1 { s += $NF } END { print s + 0 }'
}
committed() {  # group, topic: sum of the group's committed offsets
  docker compose exec -T redpanda rpk group describe "$1" 2>/dev/null \
    | awk -v t="$2" '$1 == t && $3 ~ /^[0-9]+$/ { s += $3 } END { print s + 0 }'
}

# --- Resource samples (bench/stats.sh) ---------------------------------------
stats_pid=""
stats_start() {  # name: sample docker stats in the background until stats_stop
  local out="bench/results/stats-$1-$(hostname -s 2>/dev/null || hostname)-$(date +%Y%m%d-%H%M).csv"
  mkdir -p bench/results
  bench/stats.sh "$out" &
  stats_pid=$!
  echo "Resource samples: $out"
}
stats_stop() {
  [[ -n "$stats_pid" ]] || return 0
  kill "$stats_pid" 2>/dev/null || true
  wait "$stats_pid" 2>/dev/null || true
  stats_pid=""
}
