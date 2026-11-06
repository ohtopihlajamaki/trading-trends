#!/usr/bin/env bash
# Samples CPU, memory, network and disk of every running container into a CSV,
# about once a second (each `docker stats --no-stream` call takes ~1 s), until
# stopped with Ctrl-C or SIGTERM. The bench scripts start it in the background
# through stats_start in lib.sh; run it by hand next to anything else:
#   bench/stats.sh bench/results/stats-mytest.csv
#
# Columns: t_ms (wall clock when the sample started, epoch ms), container,
# cpu_perc (100% = one CPU), mem_usage, mem_perc, net_io, block_io, pids.
# net_io and block_io are totals since the container started, as docker shows
# them ("1.2MB / 3.4MB" = in / out, read / write).
set -uo pipefail
out=${1:?usage: $0 OUT.csv}

trap 'exit 0' INT TERM
echo "t_ms,container,cpu_perc,mem_usage,mem_perc,net_io,block_io,pids" >"$out"
while :; do
  t=$(perl -MTime::HiRes=time -e 'printf "%.0f", time * 1000')
  rows=$(docker stats --no-stream \
    --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' 2>/dev/null)
  if [[ -n "$rows" ]]; then
    sed "s/^/$t,/" <<<"$rows" >>"$out"
  else
    sleep 1 # no containers, or the daemon is not reachable
  fi
done
