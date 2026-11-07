#!/usr/bin/env bash
# Performance matrix: runs bench/realtime.sh once per combination of pipeline
# cores (CORES) and exchange size (COPIES = REPLAY_COPIES), REPEATS times each,
# on the Linux benchmark setup (compose.perf.yaml, every service pinned to its
# own cores), and collects every result row into one CSV per part of the test.
#
# Runs itself in a tmux session, so it survives a dropped SSH connection:
#   bench/matrix.sh                 # starts tmux session "bench-matrix"
#   tmux attach -t bench-matrix     # watch it; detach with Ctrl-b d
#
# Settings (environment):
#   CORES                 pipeline cores per run: Redpanda, signal, ClickHouse, gateway;
#                         default 4, 6, 8, ... up to the machine's CPUs (12 CPUs: "4 6 8 10 12")
#   COPIES="1 5 10 20"    REPLAY_COPIES per run
#   REPEATS=3             runs per combination
#   FIRST_CORE=4          the pipeline uses cores FIRST_CORE .. FIRST_CORE+N-1; a run
#                         too big for that starts lower (the last N cores) and shares
#                         cores with the load generator, which the log and CSV show
#   LOAD_CPUS=0-3         cores of the load generator (replayer and probe), the same
#                         in every run and outside the pipeline's cores
#   MODE=realtime         passed to bench/realtime.sh (realtime, sweep or all), as are
#                         REALTIME_POINTS, SWEEP_*, DAYS, LATENCY_SLO_MS and CATCHUP_TIMEOUT_S
#   MATRIX_RESUME=dir     continue an interrupted matrix in bench/results/matrix-*/
#   MATRIX_DRY_RUN=1      print the runs and their core sets, run nothing
#   MATRIX_NO_TMUX=1      run in the foreground
#   MATRIX_MAX_USED_GB    disk guard: used GB of Docker's filesystem that no run may
#                         pass (default: used at start + MATRIX_HEADROOM_GB=30). A run
#                         that passes it is stopped, its volumes removed, and the
#                         matrix ends. On a thin-provisioned VM set it from the host's
#                         free pool space, and mount the guest with discard so deleted
#                         volumes go back to the pool; the guard sees only the guest.
#                         Each run's disk use goes to disk.csv in the results.
#
# With sysstat and procps installed, each run also gets <run>.mpstat (per-core
# load every 5 s) and <run>.vmstat (including stolen CPU time) next to its log.
#
# Core numbers are Linux CPU numbers. With hyperthreading, two numbers can be
# the two threads of one physical core (lscpu -e shows which); choose
# FIRST_CORE and the counts so a run gets whole cores, and say which in the report.
#
# Split of N pipeline cores (edit split() to change it): Redpanda N/4, signal
# N/4 (one signal instance per core), gateway N/8, ClickHouse the rest; at
# least one core each, so N must be at least 4. REDPANDA_SMP follows Redpanda's
# cores and REDPANDA_MEMORY gets 1 GiB per Redpanda core.
set -euo pipefail
cd "$(dirname "$0")/.."

passed=(CORES COPIES REPEATS FIRST_CORE LOAD_CPUS MODE MATRIX_RESUME MATRIX_DRY_RUN COMPOSE_FILE
  MATRIX_MAX_USED_GB MATRIX_HEADROOM_GB CATCHUP_TIMEOUT_S
  REALTIME_POINTS REALTIME_SPEED SWEEP_WINDOWS SWEEP_SPEEDS SWEEP_SPAN_MIN DAYS LATENCY_SLO_MS)

# --- Into tmux -----------------------------------------------------------------
if [[ -z "${TMUX:-}" && -z "${MATRIX_NO_TMUX:-}" && -z "${MATRIX_DRY_RUN:-}" ]]; then
  command -v tmux >/dev/null || { echo "tmux is not installed; set MATRIX_NO_TMUX=1 to run in the foreground" >&2; exit 1; }
  session=bench-matrix
  tmux has-session -t "$session" 2>/dev/null && { echo "tmux session $session exists: tmux attach -t $session" >&2; exit 1; }
  # A running tmux server keeps its own environment, so the settings travel in the command
  cmd="cd $(printf '%q' "$PWD") && env MATRIX_NO_TMUX=1"
  for v in "${passed[@]}"; do
    [[ -n "${!v:-}" ]] && cmd+=" $v=$(printf '%q' "${!v}")"
  done
  cmd+=" bash bench/matrix.sh; echo; echo 'matrix finished; press Enter to close'; read -r _"
  tmux new-session -d -s "$session" "$cmd"
  echo "started in tmux session $session"
  echo "  watch:  tmux attach -t $session   (detach: Ctrl-b d)"
  exit 0
fi

# --- The matrix ------------------------------------------------------------------
copies_list=${COPIES:-1 5 10 20}
repeats=${REPEATS:-3}
first=${FIRST_CORE:-4}
load=${LOAD_CPUS:-0-3}
export MODE=${MODE:-realtime}
export COMPOSE_FILE=${COMPOSE_FILE:-compose.yaml:compose.perf.yaml}
[[ "$repeats" =~ ^[1-9][0-9]*$ ]] || { echo "REPEATS must be a positive integer" >&2; exit 1; }

range() { (( $2 == 1 )) && echo "$1" || echo "$1-$(( $1 + $2 - 1 ))"; }  # first core, count
split() {  # N -> sets rp sig gw ch (counts) and the CPUS_* core sets
  local n=$1 c=$first
  (( n >= 4 )) || { echo "CORES must be at least 4, got $n" >&2; exit 1; }
  (( ncpu == 0 || c + n <= ncpu )) || c=$(( ncpu - n ))  # too big: the last n cores
  shared=$(( c < first ))
  rp=$(( n / 4 )) sig=$(( n / 4 )) gw=$(( n / 8 > 0 ? n / 8 : 1 ))
  ch=$(( n - rp - sig - gw ))
  CPUS_REDPANDA=$(range $c $rp); c=$(( c + rp ))
  CPUS_SIGNAL=$(range $c $sig); c=$(( c + sig ))
  CPUS_GATEWAY=$(range $c $gw); c=$(( c + gw ))
  CPUS_CLICKHOUSE=$(range $c $ch)
}

# Every core must exist, and the load generator must stay off the pipeline's cores
ncpu=$(docker info --format '{{.NCPU}}' 2>/dev/null) || ncpu=""
[[ "$ncpu" =~ ^[1-9][0-9]*$ ]] || ncpu=$(nproc 2>/dev/null || echo 0)
if [[ -n "${CORES:-}" ]]; then
  cores_list=$CORES
else  # 4, 6, 8, ... up to this machine's CPUs
  (( ncpu >= 4 )) || { echo "The pipeline needs at least 4 CPUs; found $ncpu" >&2; exit 1; }
  cores_list=$(seq -s ' ' 4 2 "$ncpu")
fi
max_n=$(tr ' ' '\n' <<<"$cores_list" | sort -n | tail -1)
if (( ncpu > 0 && max_n > ncpu )); then
  echo "CORES up to $max_n needs $max_n CPUs; Docker has $ncpu" >&2
  exit 1
fi
load_max=$(tr ',-' '\n\n' <<<"$load" | sort -n | tail -1)
(( load_max < first )) || { echo "LOAD_CPUS ($load) overlaps the pipeline's cores (from $first)" >&2; exit 1; }

if [[ -n "${MATRIX_RESUME:-}" ]]; then
  dir=$MATRIX_RESUME
  [[ -d "$dir" ]] || { echo "MATRIX_RESUME: no directory $dir" >&2; exit 1; }
  rm -f "$dir/failed.txt" # failed runs are tried again
else
  dir=bench/results/matrix-$(hostname -s 2>/dev/null || hostname)-$(date +%Y%m%d-%H%M)
fi
total=$(( $(wc -w <<<"$cores_list") * $(wc -w <<<"$copies_list") * repeats ))

# --- Disk guard ------------------------------------------------------------------
disk_path=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null) || disk_path=""
[[ -d "$disk_path" ]] || disk_path=/
used_gb() { df -BG --output=used "$disk_path" 2>/dev/null | tail -1 | tr -dc 0-9; }
max_used=${MATRIX_MAX_USED_GB:-$(( $(used_gb) + ${MATRIX_HEADROOM_GB:-30} ))}
[[ "$max_used" =~ ^[0-9]+$ ]] || { echo "MATRIX_MAX_USED_GB must be a whole number of GB" >&2; exit 1; }
trim() { sudo -n fstrim "$disk_path" >/dev/null 2>&1 || true; }  # deleted volumes back to a thin pool, if allowed
watch_disk() {  # pid of the run, peak file: record the peak; stop the run past the cap
  local peak=0 u
  while kill -0 "$1" 2>/dev/null; do
    u=$(used_gb); u=${u:-0}
    if (( u > peak )); then peak=$u; echo "$peak" >"$2"; fi
    if (( u > max_used )); then
      echo "${u} GB used on $disk_path, over MATRIX_MAX_USED_GB=$max_used" >"$dir/disk-stop"
      pkill -TERM -P "$1" 2>/dev/null || true
      kill -TERM "$1" 2>/dev/null || true
      docker rm -f rt-probe >/dev/null 2>&1 || true
      COMPOSE_PROJECT_NAME=stock-signals-rt docker compose down -v --remove-orphans >/dev/null 2>&1 || true
      trim
      return
    fi
    sleep 5
  done
}

echo "Matrix: pipeline cores [$cores_list] x copies [$copies_list] x $repeats repeats = $total runs, MODE=$MODE"
echo "Load generator on cores $load; pipeline from core $first; results in $dir"
echo "Disk guard: $(used_gb) GB used on $disk_path now; a run stops the matrix past $max_used GB"
[[ -n "${MATRIX_DRY_RUN:-}" ]] || mkdir -p "$dir"
prefix="shares_load_cores,pipeline_cores,redpanda_cores,signal_cores,gateway_cores,clickhouse_cores,cpus_redpanda,cpus_signal,cpus_gateway,cpus_clickhouse,cpus_load,copies,repeat"

i=0
for n in $cores_list; do
  split "$n"
  for k in $copies_list; do
    for r in $(seq "$repeats"); do
      i=$(( i + 1 ))
      cell="cores$n-k$k-r$r"
      printf '[%d/%d] %-18s redpanda %-7s signal %-7s gateway %-7s clickhouse %-7s %s' "$i" "$total" "$cell" \
        "$CPUS_REDPANDA" "$CPUS_SIGNAL" "$CPUS_GATEWAY" "$CPUS_CLICKHOUSE" "$( (( shared )) && echo '(shares load cores) ')"
      if [[ -n "${MATRIX_DRY_RUN:-}" ]]; then echo; continue; fi
      if grep -qx "$cell" "$dir/done.txt" 2>/dev/null; then echo "done before, skipped"; continue; fi
      echo "... $(date +%H:%M)"

      log="$dir/$cell.log"
      before=$(used_gb)
      if (( before > max_used )); then
        echo "      STOPPED: ${before} GB already used on $disk_path, over MATRIX_MAX_USED_GB=$max_used"
        break 3
      fi
      # Per-core load and system counters during the run (sysstat, procps), to
      # see what else ran on the pipeline's cores and how much CPU was stolen
      mon=()
      if command -v mpstat >/dev/null; then
        S_TIME_FORMAT=ISO mpstat -P ALL 5 >"$dir/$cell.mpstat" 2>&1 & mon+=($!)
      fi
      if command -v vmstat >/dev/null; then
        vmstat -t 5 >"$dir/$cell.vmstat" 2>&1 & mon+=($!)
      fi
      CPUS_REDPANDA=$CPUS_REDPANDA CPUS_SIGNAL=$CPUS_SIGNAL CPUS_GATEWAY=$CPUS_GATEWAY \
        CPUS_CLICKHOUSE=$CPUS_CLICKHOUSE CPUS_REPLAYER=$load CPUS_PROBE=$load \
        REDPANDA_SMP=$rp REDPANDA_MEMORY=${rp}G SIGNAL_INSTANCES=$sig REPLAY_COPIES=$k \
        bench/realtime.sh >"$log" 2>&1 &
      run_pid=$!
      echo "$before" >"$dir/.peak"
      watch_disk "$run_pid" "$dir/.peak" &
      watch_pid=$!
      run_ok=0; wait "$run_pid" || run_ok=$?
      wait "$watch_pid" 2>/dev/null || true
      (( ${#mon[@]} == 0 )) || kill "${mon[@]}" 2>/dev/null || true
      trim
      peak=$(cat "$dir/.peak"); after=$(used_gb); rm -f "$dir/.peak"
      [[ -s "$dir/disk.csv" ]] || echo "cell,copies,used_before_gb,peak_used_gb,used_after_gb,cap_gb" >"$dir/disk.csv"
      echo "$cell,$k,$before,$peak,$after,$max_used" >>"$dir/disk.csv"
      if [[ -e "$dir/disk-stop" ]]; then
        echo "$cell" >>"$dir/failed.txt"
        echo "      STOPPED by the disk guard: $(cat "$dir/disk-stop"); the matrix ends here"
        rm -f "$dir/disk-stop"
        break 3
      fi
      if (( run_ok == 0 )); then
        # Each "Results: x.csv, x.md" line: one part of the test (realtime, sweep)
        while read -r csv; do
          part=$(basename "$csv" | cut -d- -f1)
          out="$dir/$part.csv"
          [[ -s "$out" ]] || echo "$prefix,$(head -1 "$csv")" >"$out"
          tail -n +2 "$csv" | sed "s|^|$shared,$n,$rp,$sig,$gw,$ch,$CPUS_REDPANDA,$CPUS_SIGNAL,$CPUS_GATEWAY,$CPUS_CLICKHOUSE,$load,$k,$r,|" >>"$out"
        done < <(sed -n 's/^Results: \([^,]*\.csv\),.*/\1/p' "$log")
        echo "$cell" >>"$dir/done.txt"
        echo "      ok (disk peak ${peak} GB): $(grep -E ' ms median| ticks/s achieved' "$log" | tr -s ' ' | tr '\n' ';' | cut -c1-140)"
      else
        echo "$cell" >>"$dir/failed.txt"
        echo "      FAILED, see $log: $(tail -1 "$log")"
        # realtime.sh leaves a failed run's volumes; remove them now, not at the next run
        docker rm -f rt-probe >/dev/null 2>&1 || true
        COMPOSE_PROJECT_NAME=stock-signals-rt docker compose down -v --remove-orphans >/dev/null 2>&1 || true
        trim
      fi
    done
  done
done
[[ -n "${MATRIX_DRY_RUN:-}" ]] && exit 0
echo
echo "Matrix done: $(cat "$dir/done.txt" 2>/dev/null | wc -l | tr -d ' ') of $total runs ok, results in $dir/*.csv"
[[ -s "$dir/failed.txt" ]] && echo "failed: $(tr '\n' ' ' <"$dir/failed.txt")(rerun with MATRIX_RESUME=$dir)"
exit 0
