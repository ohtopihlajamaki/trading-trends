#!/usr/bin/env bash
# Performance test: how the pipeline performs on the trading week's own load.
#
# 1. Real time. Picks 5-minute windows of the week by how many ticks they hold
#    (REALTIME_POINTS, default p10 p35 p50 p99 p100 = quiet, pre-open morning,
#    typical trading, the busiest 1% and the busiest window of all) and
#    replays each at 1x. Reports the latency from the window's end to a UI
#    client receiving its bars and BUY/SELL signals, split into the wait for
#    the record that closes the window, the signal service, and the gateway.
# 2. Load sweep. Replays the hour up to and around SWEEP_WINDOWS at rising
#    multiples of real time (SWEEP_SPEEDS) with SIGNAL_INSTANCES signal
#    services, and finds the highest speed whose p95 system latency stays
#    within LATENCY_SLO_MS.
#
# Every run starts from empty volumes, fast-forwards the day up to the measured
# span (so the EMAs are warm and signals can fire), waits until every service
# has caught up, then replays the span at the test speed while bench/probe
# records what the gateway sends. Results, with a description of the machine,
# go to bench/results/ as CSV and Markdown, so runs on different hardware and
# operating systems can be compared.
#
# Usage: bench/realtime.sh
#   MODE=all              all, realtime or sweep
#   DAYS                  days to choose windows from (default: every prepped day)
#   REALTIME_POINTS       default "p10 p35 p50 p99 p100"
#   REALTIME_SPEED=1      anything else is a quick smoke test, labelled "not real time"
#   SWEEP_WINDOWS         default "p50 p100"
#   SWEEP_SPEEDS          default "10 30 100 300 1000 3000"
#   SWEEP_SPAN_MIN=60     market minutes per sweep run, ending 10 min after the window
#   SIGNAL_INSTANCES="1"  e.g. "1 2 4"; the real-time part uses the first
#   LATENCY_SLO_MS=1000   p95 system latency a sweep step must stay within
#   COMPOSE_FILE          e.g. compose.yaml:compose.perf.yaml on the Linux box
#   REPLAY_COPIES         size of the exchange (default from .env, else 1): every
#                         tick replayed k times; expected ticks and bars scale with it
#   WS_MAX_PENDING_BARS   the gateway's per-client bar limit; default scales with
#                         REPLAY_COPIES (20,000, or 10,000 per copy from 3 copies)
#
# The test uses its own Compose project (stock-signals-rt), so the pipeline's
# usual volumes are not touched; stop the usual stack first, as both publish
# the same ports. Needs Docker, bash (3.2 or later) and perl. Defaults take
# about 45 min for the real-time part and 30 min per signal-instance count for
# the sweep; MODE=realtime REALTIME_SPEED=20 is a 5-minute smoke test.
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh

export COMPOSE_PROJECT_NAME=stock-signals-rt
export PROBE_USER="$(id -u):$(id -g)"
mode=${MODE:-all}
points=${REALTIME_POINTS:-p10 p35 p50 p99 p100}
rt_speed=${REALTIME_SPEED:-1}
sweep_windows=${SWEEP_WINDOWS:-p50 p100}
sweep_speeds=${SWEEP_SPEEDS:-10 30 100 300 1000 3000}
span_min=${SWEEP_SPAN_MIN:-60}
instances=${SIGNAL_INSTANCES:-1}
slo=${LATENCY_SLO_MS:-1000}
work=bench/.realtime
copies=${REPLAY_COPIES:-$(awk -F= '$1 == "REPLAY_COPIES" { print $2 }' .env 2>/dev/null)}
copies=${copies:-1}
[[ "$copies" =~ ^[1-9][0-9]*$ ]] || { echo "REPLAY_COPIES must be a positive integer" >&2; exit 1; }
# The probe subscribes to every symbol, so each window close sends it one bar
# per symbol (about 4,840 per copy) in one frame. The gateway's per-client
# limits must hold that burst with room to spare, yet a full frame must stay
# within the probe's 64 MiB read limit (about 250,000 bars)
export WS_MAX_PENDING_BARS=${WS_MAX_PENDING_BARS:-$(( copies > 2 ? 10000 * copies : 20000 ))}
export WS_MAX_PENDING_SIGNALS=${WS_MAX_PENDING_SIGNALS:-$(( 5000 * copies ))}
case $mode in all | realtime | sweep) ;; *) echo "MODE must be all, realtime or sweep" >&2; exit 1 ;; esac
(( span_min >= 15 && span_min % 5 == 0 )) || { echo "SWEEP_SPAN_MIN must be a multiple of 5, at least 15" >&2; exit 1; }

# Days: as given, or every day prep has written (bucket 00 has them all)
data_dir=$(awk -F= '$1 == "DATA_DIR" { print $2 }' .env 2>/dev/null)
data_dir=${data_dir:-../data}
if [[ -z "${DAYS:-}" ]]; then
  DAYS=$(ls "$data_dir/ticks/bucket=00/" 2>/dev/null | sed -n 's/^\([0-9]\{6\}\)\.parquet$/\1/p' \
    | awk '{ print substr($0, 5, 2) substr($0, 3, 2) substr($0, 1, 2), $0 }' | sort | awk '{ printf "%s ", $2 }')
fi
days=$(echo $DAYS)
[[ -n "$days" ]] || { echo "No prepped days in $data_dir/ticks; run prep first" >&2; exit 1; }

berlin() {  # epoch ms, strftime format -> Europe/Berlin time
  perl -MPOSIX -e '$ENV{TZ} = "Europe/Berlin"; tzset; print strftime($ARGV[1], localtime($ARGV[0] / 1000))' "$1" "$2"
}
kv() { tr ' ' '\n' <<<"$2" | awk -F= -v k="$1" '$1 == k { print $2 }'; }  # key, "k=v k=v ..."

# --- The machine -------------------------------------------------------------
read -r docker_cpus docker_mem docker_ver <<<"$(docker info --format '{{.NCPU}} {{.MemTotal}} {{.ServerVersion}}')"
docker_os=$(clean "$(docker info --format '{{.OperatingSystem}}')")
arch=$(docker info --format '{{.Architecture}}')
host=$(hostname -s 2>/dev/null || hostname)
os=$(clean "$(host_os)")
cpu=$(clean "$(host_cpu)")
cores=$(getconf _NPROCESSORS_ONLN)
mem_gib=$(awk -v b="$docker_mem" 'BEGIN { printf "%.1f", b / 1073741824 }')
compose_files=${COMPOSE_FILE:-compose.yaml}
commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
git diff --quiet HEAD -- . 2>/dev/null || commit="$commit-dirty"
machine_hdr="date,host,host_os,cpu,host_cores,docker_cpus,docker_mem_gib,docker_os,docker_version,arch,compose,git_commit,flush_ms,replay_copies,ws_max_pending_bars"
stamp=$(date +%Y%m%d-%H%M)

echo "Host:    $host, $os, ${cpu:-unknown CPU}, $cores cores"
echo "Docker:  $docker_ver, $docker_os, $arch, $docker_cpus CPUs, $mem_gib GiB"
echo "Test:    mode $mode, days $days, commit $commit, compose $compose_files"
echo

mkdir -p "$work" bench/results
probe=rt-probe
cleanup() { docker rm -f "$probe" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "Building images ..."
docker compose --profile replay --profile bench build replayer signal gateway probe >/dev/null 2>&1

# fresh K: empty volumes, then the pipeline with K signal instances
fresh() {
  cleanup
  docker compose down -v >/dev/null 2>&1 || true
  if ! docker compose up -d --wait --scale signal="$1" redpanda redpanda-init clickhouse signal gateway >/dev/null 2>&1; then
    echo "The pipeline did not start; is another stack using its ports? (docker compose ps)" >&2
    exit 1
  fi
}

gateway_bars() {
  docker compose exec -T gateway wget -qO- http://127.0.0.1:8000/metrics \
    | awk '/^gateway_records_total\{topic="bars_ema"\}/ { n = $2 } END { print n + 0 }'
}

# caught_up: wait until the signal service, ClickHouse and the gateway have read
# everything produced so far, so a measured span does not compete with backlog
caught_up() {
  local ticks bars deadline=$(( $(date +%s) + 900 ))
  ticks=$(offsets ticks)
  until (( $(committed signal ticks) >= ticks && $(committed ch-ticks ticks) >= ticks )); do
    (( $(date +%s) < deadline )) || { echo "timed out waiting for the ticks backlog" >&2; exit 1; }
    sleep 0.5
  done
  bars=$(offsets bars_ema)
  until (( $(committed ch-bars-ema bars_ema) >= bars && $(gateway_bars) >= bars )); do
    (( $(date +%s) < deadline )) || { echo "timed out waiting for the bars backlog" >&2; exit 1; }
    sleep 0.5
  done
}

replay() {  # day, from HH:MM:SS, until HH:MM:SS, speed -> log on stdout, host time on each line
  docker compose --profile replay run --rm --no-deps -e DAYS="$1" -e REPLAY_FROM="$2" \
    -e REPLAY_UNTIL="$3" -e REPLAY_SPEED="$4" -e REPLAY_COPIES="$copies" replayer 2>&1 | while IFS= read -r line; do echo "$(now) $line"; done
}

# measure TAG DAY K FROM_MS UNTIL_MS SPEED WIN_FROM_MS WIN_TO_MS: one run; sets
# res (probe analyze's key=value line), sent (ticks replayed), planned_s (the
# replay's schedule), drain_s (replay start until the signal service has
# committed every tick) and max_lag_ms (how far the replayer fell behind)
measure() {
  local tag=$1 day=$2 k=$3 from=$4 until=$5 speed=$6 wfrom=$7 wto=$8
  local from_hms until_hms log clock t_clock t_drained summary i=0
  from_hms=$(berlin "$from" %H:%M:%S)
  until_hms=$(berlin "$until" %H:%M:%S)
  fresh "$k"

  # Fast-forward the day up to the span; empty when the span starts the day
  replay "$day" "" "$from_hms" max >"$work/ff-$tag.log" \
    || { echo "fast-forward failed; see $work/ff-$tag.log" >&2; exit 1; }
  caught_up

  rm -f "$work/raw-$tag.csv" "$work/raw-$tag.csv.ready"
  docker compose --profile bench run -d --no-deps --name "$probe" -e SYMBOLS_FILE="/work/symbols-$day-x$copies.json" \
    -e OUT="/work/raw-$tag.csv" probe listen >/dev/null
  until [[ -e "$work/raw-$tag.csv.ready" ]]; do
    (( ++i < 60 )) || { echo "probe did not start: $(docker logs "$probe" 2>&1 | tail -3)" >&2; exit 1; }
    sleep 0.5
  done
  sleep 1 # let the gateway apply the subscription

  log="$work/replay-$tag.log"
  replay "$day" "$from_hms" "$until_hms" "$speed" >"$log" || { echo "replay failed; see $log" >&2; exit 1; }
  local ticks deadline=$(( $(date +%s) + 900 ))
  ticks=$(offsets ticks)
  until (( $(committed signal ticks) >= ticks )); do
    (( $(date +%s) < deadline )) || { echo "timed out waiting for the signal service" >&2; exit 1; }
    sleep 0.2
  done
  t_drained=$(now)
  sleep 2 # the gateway flushes every FLUSH_MS (250 ms by default)
  docker stop "$probe" >/dev/null
  docker rm "$probe" >/dev/null

  clock=$(grep ' clock ' "$log" | tail -1)
  summary=$(grep ' summary ' "$log" | tail -1)
  [[ -n "$clock" && -n "$summary" ]] || { echo "no clock or summary line; see $log" >&2; exit 1; }
  t_clock=${clock%% *}
  sent=$(kv ticks "$summary")
  planned_s=$(kv planned_s "$summary")
  max_lag_ms=$(kv max_lag_ms "$summary")
  drain_s=$(awk -v a="$t_clock" -v b="$t_drained" 'BEGIN { printf "%.2f", b - a }')
  res=$(docker compose --profile bench run --rm --no-deps probe analyze in="/work/raw-$tag.csv" \
    start_ms="$(kv start_ms "$clock")" origin_ms="$(kv origin_ms "$clock")" speed="$speed" \
    from_ms="$wfrom" to_ms="$wto")
}

# --- Choose the windows ------------------------------------------------------
echo "Choosing windows from days $days ..."
fresh 1
flush_ms=$(docker compose exec -T gateway printenv FLUSH_MS 2>/dev/null || echo 250)
glob="data/ticks/bucket=*/{$(echo $days | tr ' ' ',')}.parquet"
ch --multiquery -q "
  CREATE TABLE rt_windows ENGINE = Memory AS
  SELECT toUnixTimestamp(toStartOfFiveMinutes(event_ts)) * 1000 AS w, count() AS n, uniqExact(symbol) AS syms,
         formatDateTime(toTimeZone(toStartOfFiveMinutes(event_ts), 'Europe/Berlin'), '%d%m%y') AS day
  FROM file('$glob') GROUP BY w, day"
labels=$(printf '%s\n' $points $sweep_windows | awk '!seen[$0]++')
: >"$work/windows.tsv"
for label in $labels; do
  q=${label#p}
  [[ "$q" =~ ^[0-9]+(\.[0-9]+)?$ ]] && awk -v q="$q" 'BEGIN { exit !(q >= 0 && q <= 100) }' \
    || { echo "window points look like p50 or p99.9, got $label" >&2; exit 1; }
  # The window whose tick count is closest to the quantile (earliest on ties),
  # its busiest second, and the sweep span's ticks and bars
  ch -q "
    WITH (SELECT quantileExact($(awk -v q="$q" 'BEGIN { print q / 100 }'))(n) FROM rt_windows) AS target
    SELECT '$label', day, w, n, syms FROM rt_windows ORDER BY abs(toInt64(n) - toInt64(target)), w LIMIT 1
    FORMAT TSV" >"$work/pick.tsv"
  read -r _ day w n syms <"$work/pick.tsv"
  span_from=$(( w - (span_min - 10) * 60000 ))
  span_to=$(( w + 10 * 60000 ))
  read -r peak span_ticks span_bars <<<"$(ch -q "
    SELECT
      (SELECT max(c) FROM (SELECT count() c FROM file('data/ticks/bucket=*/$day.parquet')
         WHERE toUnixTimestamp64Milli(event_ts) >= $w AND toUnixTimestamp64Milli(event_ts) < $w + 300000
         GROUP BY toStartOfSecond(event_ts))),
      (SELECT count() FROM file('data/ticks/bucket=*/$day.parquet')
         WHERE toUnixTimestamp64Milli(event_ts) >= $span_from AND toUnixTimestamp64Milli(event_ts) < $span_to),
      (SELECT sum(syms) FROM rt_windows WHERE w >= $span_from AND w < $span_to)
    FORMAT TSV")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$day" "$w" "$n" "$syms" "$peak" "$span_ticks" "$span_bars" \
    >>"$work/windows.tsv"
  printf '  %-5s %s  %7s ticks, busiest second %s\n' "$label" "$(berlin "$w" '%a %d.%m. %H:%M')" "$n" "$peak"
done
for day in $(cut -f2 "$work/windows.tsv" | sort -u); do
  # Every symbol of the day, and its copies c1:SYMBOL .. c<k-1>:SYMBOL
  [[ -s "$work/symbols-$day-x$copies.json" ]] || ch -q "
    SELECT toJSONString(arrayFlatten(arrayMap(i -> arrayMap(s -> if(i = 0, s, concat('c', toString(i), ':', s)), syms),
                                              range($copies))))
    FROM (SELECT groupUniqArray(symbol) AS syms FROM file('data/ticks/bucket=*/$day.parquet'))
    FORMAT TSVRaw" >"$work/symbols-$day-x$copies.json"
done
echo

window() {  # label -> sets day w n syms peak span_ticks span_bars
  local row
  row=$(awk -F'\t' -v l="$1" '$1 == l' "$work/windows.tsv")
  IFS=$'\t' read -r _ day w n syms peak span_ticks span_bars <<<"$row"
  # The replay sends every tick $copies times, under as many symbols
  n=$((n * copies)) syms=$((syms * copies)) peak=$((peak * copies))
  span_ticks=$((span_ticks * copies)) span_bars=$((span_bars * copies))
}
machine() { echo "$(date +%Y-%m-%dT%H:%M:%S),$host,$os,$cpu,$cores,$docker_cpus,$mem_gib,$docker_os,$docker_ver,$arch,$compose_files,$commit,$flush_ms,$copies,$WS_MAX_PENDING_BARS"; }

# --- 1. Real time ------------------------------------------------------------
if [[ $mode != sweep ]]; then
  k=${instances%% *}
  rt_csv=bench/results/realtime-$host-$stamp.csv
  echo "$machine_hdr,realtime_speed,signal_instances,point,day,window_berlin,ticks,avg_ticks_per_s,peak_ticks_per_s,bars_expected,bars,signals,dropped,total_p50_ms,total_p95_ms,total_max_ms,signal_p50_ms,signal_max_ms,system_p50_ms,system_p95_ms,system_max_ms,wait_p50_ms,service_p50_ms,gateway_p50_ms,replay_max_lag_ms" >"$rt_csv"
  speed_note=""
  [[ "$rt_speed" == 1 ]] || speed_note=" (${rt_speed}x, not real time)"
  echo "Real time$speed_note: $(wc -w <<<"$points" | tr -d ' ') windows, about $(awk -v s="$rt_speed" -v n="$(wc -w <<<"$points")" 'BEGIN { printf "%.0f", n * (1.5 + 7 / s) }') min"
  for label in $points; do
    window "$label"
    printf '  %-5s %s ... ' "$label" "$(berlin "$w" '%a %H:%M')"
    measure "rt-$label" "$day" "$k" $(( w - 60000 )) $(( w + 360000 )) "$rt_speed" "$w" $(( w + 1 ))
    echo "$(kv total_p50_ms "$res") ms median, $(kv signals "$res") signals"
    echo "$(machine),$rt_speed,$k,$label,$day,$(berlin "$w" '%a %d.%m. %H:%M'),$n,$(awk -v n="$n" 'BEGIN { printf "%.0f", n / 300 }'),$peak,$syms,$(kv bars "$res"),$(kv signals "$res"),$(kv dropped "$res"),$(kv total_p50_ms "$res"),$(kv total_p95_ms "$res"),$(kv total_max_ms "$res"),$(kv sig_total_p50_ms "$res"),$(kv sig_total_max_ms "$res"),$(kv system_p50_ms "$res"),$(kv system_p95_ms "$res"),$(kv system_max_ms "$res"),$(kv wait_p50_ms "$res"),$(kv service_p50_ms "$res"),$(kv gateway_p50_ms "$res"),$max_lag_ms" >>"$rt_csv"
  done

  # The table: one column per window, one row per measure
  md=${rt_csv%.csv}.md
  awk -F, -v host="$host" -v os="$os" -v cpu="$cpu" -v cores="$cores" -v dcpus="$docker_cpus" -v dmem="$mem_gib" \
      -v commit="$commit" -v compose="$compose_files" -v note="$speed_note" -v flush="$flush_ms" '
    NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
    { n++; for (f in col) v[n, f] = $col[f] }
    function row(title, a, b, c, sep,   s, i) {
      s = "| " title
      for (i = 1; i <= n; i++) {
        s = s " | " v[i, a]
        if (b != "") s = s sep v[i, b]
        if (c != "") s = s sep v[i, c]
      }
      print s " |"
    }
    END {
      print "# Real-time performance" note
      print ""
      printf "%s, %s, %s, %s cores; Docker with %s CPUs and %s GiB. Commit %s, %s, gateway flush %s ms, %s signal instance(s).\n\n", host, os, cpu, cores, dcpus, dmem, commit, compose, flush, v[1, "signal_instances"]
      s = "| |"; t = "|---|"
      for (i = 1; i <= n; i++) { s = s " " v[i, "point"] (v[i, "point"] == "p100" ? " (busiest)" : "") " |"; t = t "---|" }
      print s; print t
      row("Window (Berlin time)", "window_berlin")
      row("Ticks in the 5 min", "ticks")
      row("Throughput, ticks/s (avg / busiest second)", "avg_ticks_per_s", "peak_ticks_per_s", "", " / ")
      row("Window closes received / expected (bars)", "bars", "bars_expected", "", " / ")
      row("BUY/SELL signals", "signals")
      row("Latency window end → shown, ms (median / slowest)", "total_p50_ms", "total_max_ms", "", " / ")
      row("Latency window end → BUY/SELL shown, ms (median / slowest)", "signal_p50_ms", "signal_max_ms", "", " / ")
      row("System latency, closing tick sent → shown, ms (median / slowest)", "system_p50_ms", "system_max_ms", "", " / ")
      row("Median ms: wait for closing tick / signal service / gateway + flush", "wait_p50_ms", "service_p50_ms", "gateway_p50_ms", " / ")
      print ""
      print "Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep."
    }' "$rt_csv" >"$md"
  echo "Results: $rt_csv, $md"
  echo
fi

# --- 2. Load sweep -----------------------------------------------------------
if [[ $mode != realtime ]]; then
  sw_csv=bench/results/sweep-$host-$stamp.csv
  echo "$machine_hdr,point,day,window_berlin,span_from,span_to,signal_instances,speed,ticks_sent,offered_ticks_per_s,achieved_ticks_per_s,planned_s,drain_s,bars_expected,bars,signals,dropped,system_p50_ms,system_p95_ms,system_p99_ms,system_max_ms,total_p50_ms,total_p95_ms,total_max_ms,wait_p50_ms,service_p50_ms,gateway_p50_ms,replay_max_lag_ms,verdict" >"$sw_csv"
  echo "Load sweep: windows $sweep_windows, speeds $sweep_speeds, signal instances $instances, SLO p95 system latency $slo ms"
  for label in $sweep_windows; do
    window "$label"
    span_from=$(( w - (span_min - 10) * 60000 ))
    span_to=$(( w + 10 * 60000 ))
    for k in $instances; do
      fails=0
      for speed in $(printf '%s\n' $sweep_speeds | sort -n); do
        printf '  %-5s K=%s %6sx ... ' "$label" "$k" "$speed"
        # One extra minute so the last window's closing ticks are sent too
        measure "sw-$label-k$k-$speed" "$day" "$k" "$span_from" $(( span_to + 60000 )) "$speed" "$span_from" "$span_to"
        offered=$(awk -v n="$sent" -v p="$planned_s" 'BEGIN { printf "%.0f", n / p }')
        achieved=$(awk -v n="$sent" -v d="$drain_s" 'BEGIN { printf "%.0f", n / d }')
        p95=$(kv system_p95_ms "$res")
        verdict=$(awk -v p="$p95" -v slo="$slo" -v d="$drain_s" -v pl="$planned_s" -v got="$(kv bars "$res")" \
          -v want="$span_bars" -v dr="$(kv dropped "$res")" 'BEGIN {
            if (got < want || dr > 0) print "incomplete"
            else if (p == "NA" || p > slo || d > 1.1 * pl + 1) print "too_slow"
            else print "ok" }')
        echo "p95 $p95 ms, $achieved ticks/s achieved of $offered offered: $verdict"
        echo "$(machine),$label,$day,$(berlin "$w" '%a %d.%m. %H:%M'),$(berlin "$span_from" %H:%M),$(berlin "$span_to" %H:%M),$k,$speed,$sent,$offered,$achieved,$planned_s,$drain_s,$span_bars,$(kv bars "$res"),$(kv signals "$res"),$(kv dropped "$res"),$(kv system_p50_ms "$res"),$p95,$(kv system_p99_ms "$res"),$(kv system_max_ms "$res"),$(kv total_p50_ms "$res"),$(kv total_p95_ms "$res"),$(kv total_max_ms "$res"),$(kv wait_p50_ms "$res"),$(kv service_p50_ms "$res"),$(kv gateway_p50_ms "$res"),$max_lag_ms,$verdict" >>"$sw_csv"
        if [[ $verdict == ok ]]; then fails=0; else fails=$(( fails + 1 )); fi
        (( fails < 2 )) || break # two failed steps in a row: faster ones will not pass
      done
    done
  done

  md=${sw_csv%.csv}.md
  awk -F, -v host="$host" -v os="$os" -v cpu="$cpu" -v cores="$cores" -v dcpus="$docker_cpus" -v dmem="$mem_gib" \
      -v commit="$commit" -v compose="$compose_files" -v slo="$slo" -v flush="$flush_ms" -v span="$span_min" '
    NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
    { n++; for (f in col) v[n, f] = $col[f] }
    END {
      print "# Load sweep"
      print ""
      printf "%s, %s, %s, %s cores; Docker with %s CPUs and %s GiB. Commit %s, %s, gateway flush %s ms. Each run replays %s market minutes around the window at the given multiple of real time.\n\n", host, os, cpu, cores, dcpus, dmem, commit, compose, flush, span
      print "| Window | Span | Signal instances | Speed | Offered ticks/s | Achieved ticks/s | System latency p50 / p95 / p99 ms | Window end → shown p50 / p95 ms | Verdict |"
      print "|---|---|---|---|---|---|---|---|---|"
      for (i = 1; i <= n; i++) {
        printf "| %s %s | %s–%s | %s | %s× | %s | %s | %s / %s / %s | %s / %s | %s |\n", v[i, "point"], v[i, "window_berlin"],
          v[i, "span_from"], v[i, "span_to"], v[i, "signal_instances"], v[i, "speed"], v[i, "offered_ticks_per_s"],
          v[i, "achieved_ticks_per_s"], v[i, "system_p50_ms"], v[i, "system_p95_ms"], v[i, "system_p99_ms"],
          v[i, "total_p50_ms"], v[i, "total_p95_ms"], v[i, "verdict"]
        key = v[i, "point"] " " v[i, "window_berlin"] ", " v[i, "signal_instances"] " signal instance(s)"
        if (!(key in best)) { order[++keys] = key; best[key] = "" }
        if (v[i, "verdict"] == "ok" && v[i, "speed"] + 0 > bestspeed[key]) {
          bestspeed[key] = v[i, "speed"] + 0; best[key] = v[i, "speed"] "× (" v[i, "offered_ticks_per_s"] " ticks/s)"
        }
      }
      print ""
      printf "**Highest speed within the SLO** (p95 system latency ≤ %s ms, drained on schedule, nothing lost):\n\n", slo
      for (j = 1; j <= keys; j++) printf "- %s: %s\n", order[j], (best[order[j]] == "" ? "none of the tested speeds" : best[order[j]])
    }' "$sw_csv" >"$md"
  echo "Results: $sw_csv, $md"
fi

docker compose down -v >/dev/null 2>&1 || true
