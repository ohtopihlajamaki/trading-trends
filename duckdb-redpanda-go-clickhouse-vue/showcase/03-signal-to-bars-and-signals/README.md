# 03 · `ticks` topic → signal service → `bars_ema` and `signals`

This step covers edges 6 and 7 of the graph: `redpanda → signal → redpanda`.
- **What the service does.** It reads `ticks` as one consumer group, keeps each symbol's open window and EMAs in memory, and turns each closed 5-minute window into a bar with EMA(38) and EMA(100). When the EMAs cross, it also writes a BUY or SELL.
- **Where its output goes.** Output for ticks partition p goes to partition p of `bars_ema` and `signals`. Offsets are committed only after that output is acknowledged.
- **In this step.** The service, built from this checkout, reads step 02's demo topic `showcase-ticks` as group `showcase-signal`. It writes to `showcase-bars_ema` and `showcase-signals`.

## Run

```sh
./run.sh        # Docker and a .env are enough; ClickHouse is not started
```

Run [step 02](../02-replayer-to-topic/) first. The script:

1. **Starts Redpanda** if it isn't running, and stops it again at the end.
2. **Checks the input.** `showcase-ticks` must still match step 02's `output/partitions.txt`. If it doesn't, the script asks you to run step 02 again.
3. **Prepares fresh topics and group.** It creates `showcase-bars_ema` and `showcase-signals` afresh, like `redpanda/create-topics.sh` does. It deletes the group `showcase-signal`, so the service finds no commits and starts at offset 0.
4. **Runs the service.** It builds the service and starts it in the background. The service never exits, so the script waits until the group has committed all 39 input records, then reads `/metrics`. Finally it stops the service with SIGTERM, and the service leaves the group: that's the `revoked` line in the log.
5. **Saves the result.** It reads both output topics and the group's offsets into `output/`, and prints:

```text
2026/11/05 16:33:13.633905 consuming showcase-ticks as group showcase-signal, EMA_INIT first_close=true
2026/11/05 16:33:13.643266 assigned [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31], now own 32 partitions
2026/11/05 16:33:13.842367 revoked [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31], now own 0 partitions
39 records of showcase-ticks committed by group showcase-signal; 4 records in showcase-bars_ema, 1 in showcase-signals -> showcase/03-signal-to-bars-and-signals/output/
```

**Differences from the compose service `signal`.** Only these settings change:
- `TICKS_TOPIC`, `BARS_TOPIC`, `SIGNALS_TOPIC` and `SIGNAL_GROUP`.
- `EMA_INIT=first_close`, the `.env.example` default, whatever `.env` says.

The pipeline's own topics and consumer groups are untouched. To remove the demo ones:

```sh
docker compose exec redpanda rpk topic delete showcase-bars_ema showcase-signals
docker compose exec redpanda rpk group delete showcase-signal
```

## In: topic `showcase-ticks`

This is what step 02 produced and saved in `../02-replayer-to-topic/output/records.jsonl`, 39 records in all:
- Partition 27 holds A1PHBB.ETR's 7 ticks at offsets 0–6, then a watermark at offset 7.
- Each of the other 31 partitions holds only a watermark.

## Inside the signal service

**Assignment.** One instance gets all 32 partitions. Each partition has its own state:
- the symbols it has seen
- each symbol's open window and EMAs
- the partition watermark, which is the highest event time seen so far

**Each record** ([`signal/state.go`](../../signal/state.go), `add`):
1. The record's window is `ts_ms` rounded down to 5 minutes.
2. A tick whose window ended at or before the watermark is late, and is dropped. There are none here.
3. A tick for a symbol whose open window is earlier closes that window first.
4. When the watermark crosses a 5-minute boundary, every open window that has ended is closed. A watermark record only moves the watermark.

**Closing a window.**
- The close is the trade with the greatest `(ts_ms, seq)`.
- With `first_close`, the first bar sets both EMAs to its close. Every later bar updates them with the smoothing factors 2/39 and 2/101.
- The sign of EMA38 − EMA100 is then compared with the last non-zero sign: −1 → +1 gives BUY, +1 → −1 gives SELL. The first non-zero sign only sets the baseline.

Partition 27, record by record:

| Offset | Record | Event time (Berlin) | What happens |
| ---: | --- | --- | --- |
| 0–2 | 3 trades at 21 | 09:04:20.825 | Open the 09:00 window: open = high = low = close = 21, 3 trades |
| 3 | trade at 21.2 | 09:05:02.509 | **Closes 09:00 → bar 0.** Close 21; both EMAs start at 21, so the sign is 0. Opens 09:05 |
| 4 | trade at 21.2 | 10:38:48.573 | **Closes 09:05 → bar 1.** EMA38 21.0103 > EMA100 21.0040: the sign becomes +1, which is only the baseline. Opens 10:35. The windows 09:10–10:30 have no trades, so they make no bars and don't move the EMAs |
| 5 | trade at 21.0 | 10:38:48.573 | Same window and millisecond, but `seq` 7 > 6, so this trade becomes the close |
| 6 | trade at 20.6 | 11:43:17.871 | **Closes 10:35 → bar 2.** Close 21.0; EMA38 21.0097 > EMA100 21.0039, still +1. Opens 11:40 |
| 7 | watermark | 00:00 the next day | The watermark passes 11:45. **Closes 11:40 → bar 3.** Close 20.6; EMA38 20.98872 < EMA100 20.99588, so the sign turns from +1 to −1: **SELL** |

The other partitions see only a watermark. Nothing is open there, so they produce no output, but their offsets are committed all the same.

**Produce, then commit.** All five outputs carry the same `emitted_ms`: they were encoded within the same millisecond.
- The service sends a batch's outputs first, waits for the acknowledgements, and only then commits the batch's tick offsets.
- So a crash in between replays the batch rather than losing output, and ClickHouse's ReplacingMergeTree drops the duplicates.

The service's counters (`output/metrics.txt`):

```text
signal_assigned_partitions 32
signal_bars_total 4
signal_decode_errors_total 0
signal_late_ticks_total 0
signal_rebuild_records_total 0
signal_signals_total{side="SELL"} 1
signal_ticks_total 39
```

- **`signal_ticks_total`** counts watermarks too: 39 = 7 + 32.
- **`signal_rebuild_records_total`** is 0 because the group had no commits. When a partition with a committed offset C moves to an instance, that instance first re-reads offsets 0..C−1 without emitting anything.

## Out: the topics, read back into `output/`

| File | What it holds |
| --- | --- |
| `bars_ema.jsonl` | Every bar: partition, offset, Kafka timestamp, key and value |
| `signals.jsonl` | Every crossover, in the same format |
| `group-offsets.txt` | `rpk group describe -c`: the committed offset equals the log end in every partition (8 in partition 27, 1 elsewhere), so the lag is 0 |
| `metrics.txt` | The counters above |
| `signal.log` | The service's log |

`showcase-bars_ema`, all of it:

```text
{"partition":27,"offset":0,"timestamp":1793896393647,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","window_start_ms":1636358400000,"open":21,"high":21,"low":21,"close":21,"trades":3,"ema38":21,"ema100":21,"trigger_sent_ms":1793893319531,"emitted_ms":1793896393647}}
{"partition":27,"offset":1,"timestamp":1793896393647,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","window_start_ms":1636358700000,"open":21.2,"high":21.2,"low":21.2,"close":21.2,"trades":1,"ema38":21.01025641025641,"ema100":21.003960396039606,"trigger_sent_ms":1793893319531,"emitted_ms":1793896393647}}
{"partition":27,"offset":2,"timestamp":1793896393647,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","window_start_ms":1636364100000,"open":21.2,"high":21.2,"low":21,"close":21,"trades":2,"ema38":21.00973044049967,"ema100":21.00388197235565,"trigger_sent_ms":1793893319531,"emitted_ms":1793896393647}}
{"partition":27,"offset":3,"timestamp":1793896393647,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","window_start_ms":1636368000000,"open":20.6,"high":20.6,"low":20.6,"close":20.6,"trades":1,"ema38":20.988718623038146,"ema100":20.995884309536724,"trigger_sent_ms":1793893319539,"emitted_ms":1793896393647}}
```

`showcase-signals`, all of it:

```text
{"partition":27,"offset":0,"timestamp":1793896393647,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","window_start_ms":1636368000000,"side":"SELL","close":20.6,"ema38":20.988718623038146,"ema100":20.995884309536724,"trigger_sent_ms":1793893319539,"emitted_ms":1793896393647}}
```

Where each field of the SELL's bar (offset 3) comes from:

| Field | Value | From |
| --- | --- | --- |
| key, partition | `A1PHBB.ETR`, 27 | the tick's key and partition |
| `window_start_ms` | `1636368000000` (11:40 Berlin) | `ts_ms` rounded down to 5 minutes |
| `open`, `high`, `low`, `close` | 20.6 | the first, highest, lowest and last trade in the window, by `(ts_ms, seq)` |
| `trades` | 1 | the ticks in the window |
| `ema38`, `ema100` | `20.988718623038146`, `20.995884309536724` | updated with the close, and written as the shortest decimal that reads back exactly |
| `trigger_sent_ms` | `1793893319539` | the `sent_ms` of the record that closed the window: the watermark at offset 7 in step 02's `records.jsonl` |
| `emitted_ms` | `1793896393647` | the service's wall clock when it encoded the bar |

**The signal record** repeats the window, close and EMAs, and adds `side`.

**`trigger_sent_ms`** separates the two kinds of closing. Bars 0–2 carry `…531`, the `sent_ms` of the ticks at offsets 3, 4 and 6, all sent within that millisecond. Bar 3 and the SELL carry `…539`, which only the watermark has.

**Latency.** In the live pipeline, `emitted_ms − trigger_sent_ms` is the latency through Redpanda and the signal service. Here it is about 51 minutes (3,074 s), only because this step ran that long after step 02. In the live pipeline the replayer, the signal service and the gateway run at the same time.

**Changes on every run:** `timestamp`, `emitted_ms` and the times in the log. `trigger_sent_ms` changes only when step 02 runs again. Everything else repeats exactly.

**What comes next:** [step 04](../04-topics-to-clickhouse/) loads these topics into ClickHouse.

## Snippet against the full run

The real `bars_ema` and `signals` topics in the local Redpanda still hold the full Monday run of 18 October 2026.
- **An older version of the service.** It wrote no `trigger_sent_ms`, and bars without `emitted_ms`.
- **Size.** Partition 27 holds 12,879 bars and 213 signals, for 165 symbols.

A1PHBB.ETR's records there:

| Record | Offset here | Offset in the full run |
| --- | ---: | ---: |
| bar 09:00 | 0 | 1,041 |
| bar 09:05 | 1 | 1,146 |
| bar 10:35 | 2 | 3,115 |
| bar 11:40 | 3 | 4,511 |
| SELL 11:40 | 0 | 94 |

`open`, `high`, `low`, `close`, `trades`, `ema38` and `ema100` are identical, digit for digit. The differences:

- **What closes the 11:40 window.** In the full run it's the first tick in partition 27 at or after 11:45: A0SM7V.ETR at 11:45:00.000, `ticks` offset 151,623. That one record closes all 106 open 11:40 windows of the partition, bars at offsets 4,413–4,518; A1PHBB.ETR's is at 4,511. In the snippet the partition holds nothing else, so the day-end watermark closes it.
- **Offsets.** Other symbols' bars and signals sit in between.
- **The day goes on.** A1PHBB.ETR's next bar is at 15:10 (offset 8,984). The snippet ends at 11:40.
