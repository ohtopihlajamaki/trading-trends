# 02 · Parquet buckets → replayer → `ticks` topic

This step covers edges 4 and 5 of the graph: `data/ticks → replayer → redpanda`. The replayer reads a day's 32 bucket files, merges them by the event time of each file's next row, and produces every row as a JSON record, bucket k to partition k. Here the replayer, built from this checkout, reads the buckets that step 01 wrote and produces them to a demo topic, `showcase-ticks`, on the project's Redpanda.

## Run

```sh
./run.sh        # Docker and a .env are enough; ClickHouse is not started
```

Run [step 01](../01-raw-to-prep/) first if its `output/` is missing. The script:

1. Starts Redpanda if it isn't running, and stops it again at the end.
2. Creates `showcase-ticks` afresh, the way `redpanda/create-topics.sh` creates the pipeline's topics: 32 partitions, one replica, `retention.ms=-1`.
3. Builds the replayer from the checkout and runs it.
4. Reads the whole topic back into `output/`, and prints:

```text
2026/11/05 15:41:59.531548 replaying days 081121 from /showcase/ticks, starting at the first tick: 32 buckets, 4 workers, 8 threads, speed max
2026/11/05 15:41:59.531602 clock start_ms=1793893319531 origin_ms=1636358660825 speed=0
2026/11/05 15:41:59.548655 done: 7 ticks and 32 watermarks in 17ms (410 ticks/s), 0 produce errors
2026/11/05 15:41:59.548683 summary speed=max ticks=7 watermarks=32 errors=0 took_s=0.017 ticks_per_s=410 planned_s=0.000 max_lag_ms=0.0
39 records in showcase-ticks -> showcase/02-replayer-to-topic/output/records.jsonl, offsets in showcase/02-replayer-to-topic/output/partitions.txt
```

**Differences from `docker compose --profile replay run replayer`.** Only these settings change, whatever `.env` says:
- `TICKS_DIR`: step 01's output.
- `TICKS_TOPIC=showcase-ticks`.
- `DAYS=081121`.
- `REPLAY_SPEED=max`.
- `REPLAY_FROM` and `REPLAY_UNTIL` are empty.

`REPLAY_WORKERS` comes from `.env` (4 in `.env.example`).

**The real `ticks` topic is untouched**, and so are the signal service and ClickHouse, which read it. The demo topic stays in Redpanda's volume until the next run. To remove it:

```sh
docker compose exec redpanda rpk topic delete showcase-ticks
```

## In: `../01-raw-to-prep/output/ticks/`

There are 32 files, `bucket=00/081121.parquet` to `bucket=31/081121.parquet`:
- `bucket=27` holds the 7 trades of A1PHBB.ETR.
- The other 31 are empty.

The replayer opens every bucket of every day in `DAYS`, and stops with `missing bucket file (run prep for 081121)` if one is missing.

## Inside the replayer

**Workers.** Each of the `REPLAY_WORKERS` goroutines owns the buckets with `bucket % 4 == worker`. It keeps its bucket readers in a heap ordered by the event time of each reader's current row, with ties going to the lower bucket, and always sends the earliest. Within a bucket, rows keep prep's receipt order, so the merge is only roughly in event time: a late row goes out when its bucket reaches it. A bucket's last item for the day is a watermark at the next midnight. An empty bucket starts with that watermark.

The Kafka timestamps in `records.jsonl` show the order that results:

| Worker | Buckets | What it sends, in order |
| --- | --- | --- |
| 0, 1, 2 | the other 24 | 24 watermarks, at once |
| 3 | 3, 7, 11, …, 27, 31 | 1. The 7 ticks of bucket 27 (09:04:20.825–11:43:17.871), which are earlier than midnight.<br/>2. The 8 watermarks, in bucket order: 3, 7, 11, 15, 19, 23, 27, 31 |

Across workers there is no order; within a partition the order is fixed.

**Watermarks.**
- One per bucket and day, after the bucket's last row.
- The symbol and key are empty, `last` and `seq` are 0.
- `ts_ms = 1636412400000`, which is 2021-11-09 00:00 in Berlin: the midnight after the day.

**Pacing.** With `REPLAY_SPEED=max` nothing waits. At speed N, a record with event time `ts` is due at `start_ms + (ts − origin_ms) / N`, using the anchor in the log line `clock start_ms=… origin_ms=…`. Here `origin_ms` is the first tick, 09:04:20.825.

**Producer.** franz-go (`kgo`) settings:
- A manual partitioner: partition = bucket.
- LZ4 batch compression; every record here shows `lz4`.
- A 5 ms linger.
- The Kafka timestamp is the producer's create time.

## Out: the topic, read back into `output/`

| File | What it holds |
| --- | --- |
| `records.jsonl` | Every record, partition by partition, in offset order: partition, offset, Kafka timestamp (ms), key and value |
| `partitions.txt` | `rpk topic describe -p`: the high watermark (next offset) is 8 in partition 27 and 1 everywhere else |
| `replayer.log` | The replayer's log, as printed above |

Partition 27, all of it:

```text
{"partition":27,"offset":0,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":21,"ts_ms":1636358660825,"seq":2,"sent_ms":1793893319531}}
{"partition":27,"offset":1,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":21,"ts_ms":1636358660825,"seq":3,"sent_ms":1793893319531}}
{"partition":27,"offset":2,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":21,"ts_ms":1636358660825,"seq":4,"sent_ms":1793893319531}}
{"partition":27,"offset":3,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":21.2,"ts_ms":1636358702509,"seq":5,"sent_ms":1793893319531}}
{"partition":27,"offset":4,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":21.2,"ts_ms":1636364328573,"seq":6,"sent_ms":1793893319531}}
{"partition":27,"offset":5,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":21,"ts_ms":1636364328573,"seq":7,"sent_ms":1793893319531}}
{"partition":27,"offset":6,"timestamp":1793893319531,"key":"A1PHBB.ETR","value":{"symbol":"A1PHBB.ETR","sectype":"E","last":20.6,"ts_ms":1636368197871,"seq":8,"sent_ms":1793893319531}}
{"partition":27,"offset":7,"timestamp":1793893319539,"key":"","value":{"symbol":"","sectype":"","last":0,"ts_ms":1636412400000,"seq":0,"sent_ms":1793893319539}}
```

Every other partition holds only its watermark:

```text
{"partition":0,"offset":0,"timestamp":1793893319531,"key":"","value":{"symbol":"","sectype":"","last":0,"ts_ms":1636412400000,"seq":0,"sent_ms":1793893319531}}
```

What changes across the edge:

| In the Parquet file | In the record | Change |
| --- | --- | --- |
| directory `bucket=27` | partition 27 | the bucket becomes the partition |
| row order (`event_ts`, `seq`) | offset 0–6 | each row's place in the file; the watermark takes offset 7 |
| `symbol` | key, and `value.symbol` | the key is the symbol's bytes, empty for a watermark |
| `sectype` | `value.sectype` | none |
| `last` (DOUBLE) | `value.last` | the shortest decimal that reads back exactly: 21.0 → `21` |
| `event_ts` (INT64 µs, UTC) | `value.ts_ms` | divided by 1000: `1636358660825000` → `1636358660825` |
| `seq` | `value.seq` | none |
| — | `value.sent_ms` | added: the replayer's wall clock when it encoded the record |
| — | Kafka timestamp | added by the producer, the same wall clock |

- **Changes on every run:** only `timestamp` and `sent_ms`. Here they read `1793893319531`, which is 2026-11-05 15:41:59.531 UTC; everything else repeats exactly.
- **Size:** a tick's value is 101–103 bytes of JSON, and a watermark's is 89.

**What comes next ([step 03](../03-signal-to-bars-and-signals/)).** The signal service closes a window when a later tick of the same symbol arrives, or when the partition's watermark passes the window's end:
- Offsets 3, 4 and 6 close the 09:00, 09:05 and 10:35 windows.
- Nothing in partition 27 is later than 11:45 except the watermark at offset 7. That record closes the 11:40 window, and with it comes the SELL.

## Snippet against the full run

The real `ticks` topic in the local Redpanda holds one full Monday replay, made on 18 October 2026.
- **Partition 27** holds the 451,539 rows of `data/ticks/bucket=27/081121.parquet`, for 165 symbols, and then one watermark at offset 451,539.
- **Offsets** start at 0 because the topic was empty when the replay started. Each record's offset is its row's position in the bucket file.

A1PHBB.ETR's records there:

| Trade | Offset here | Offset in the full run | `seq` here | `seq` in the full run |
| --- | ---: | ---: | ---: | ---: |
| 09:04:20.825, 21 | 0 | 21,560 | 2 | 636,067 |
| 09:04:20.825, 21 | 1 | 21,561 | 3 | 636,068 |
| 09:04:20.825, 21 | 2 | 21,562 | 4 | 636,069 |
| 09:05:02.509, 21.2 | 3 | 22,055 | 5 | 651,394 |
| 10:38:48.573, 21.2 | 4 | 99,113 | 6 | 2,675,338 |
| 10:38:48.573, 21 | 5 | 99,114 | 7 | 2,675,339 |
| 11:43:17.871, 20.6 | 6 | 150,372 | 8 | 4,005,181 |
| watermark, 00:00 | 7 | 451,539 | 0 | 0 |

**What is the same and what differs:**
- **The same:** key, partition, `symbol`, `sectype`, `last` and `ts_ms`.
- **Different offsets:** in the full run the other 164 symbols' trades sit between these records.
- **Different `seq`:** step 01 numbers the snippet's rows from 1.
- **Different wall-clock fields**, as on every run.

One more difference: in the full run, every partition has trades, so no worker sends its watermarks first.
