#!/usr/bin/env bash
# Creates the pipeline topics, or brings existing ones to the configured
# partition count and retention. Run once by the redpanda-init service.
set -euo pipefail

partitions=${PARTITIONS:-32}
retention_bytes=${RETENTION_BYTES:-20000000000}

for topic in ticks bars_ema signals; do
  if ! rpk topic describe "$topic" >/dev/null 2>&1; then
    # retention.ms=-1: the signal service rebuilds its state from offset 0
    # after a rebalance, so ticks must only ever expire by size
    rpk topic create "$topic" -p "$partitions" -r 1 \
      -c retention.bytes="$retention_bytes" -c retention.ms=-1
    continue
  fi
  have=$(rpk topic describe "$topic" -s | awk '$1 == "PARTITIONS" { print $2 }')
  if (( have < partitions )); then
    rpk topic add-partitions "$topic" -n $(( partitions - have ))
  elif (( have > partitions )); then
    echo "error: $topic has $have partitions, more than PARTITIONS=$partitions" >&2
    exit 1
  fi
  rpk topic alter-config "$topic" --set retention.bytes="$retention_bytes" --set retention.ms=-1
done

rpk topic list
