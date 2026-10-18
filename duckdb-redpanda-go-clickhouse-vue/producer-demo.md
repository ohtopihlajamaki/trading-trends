# Producer demo

These commands show the producer (`replayer/`) on its own. It reads Monday's Parquet buckets and sends every
tick to Redpanda, and `rpk` prints the records exactly as any consumer receives them. Each demo stops after
one minute.

The demos write to a separate topic, `ticks-demo`, so the pipeline's `ticks` topic and everything downstream
stay untouched. Only Redpanda needs to run.

**Before you start**

- Run the commands from this directory (`duckdb-redpanda-go-clickhouse-vue/`), in bash or zsh. On Windows,
  use WSL2.
- `.env` must exist (`cp .env.example .env`).
- Monday must be prepped: `../data/ticks/bucket=*/081121.parquet` must exist. If not, run
  `docker compose --profile prep run --rm prep`.

## Start the broker

```sh
docker compose up -d --wait redpanda
```

## 1. Monday morning, live

This replays Monday from its first tick at real time (`REPLAY_SPEED=1`) and prints each tick the moment the
producer sends it.

```sh
docker rm -f producer-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic delete ticks-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic create ticks-demo -p 32 >/dev/null
docker compose --profile replay run -d --no-deps --name producer-demo --entrypoint "timeout 60 replayer" \
  -e TICKS_TOPIC=ticks-demo -e DAYS=081121 -e REPLAY_SPEED=1 replayer >/dev/null 2>&1
docker compose exec redpanda sh -c "trap 'exit 0' INT; timeout --foreground 60 rpk topic consume ticks-demo -o start -f 'partition %p  %v\n' || true"
```

Monday's first tick is at 07:00:00 Berlin time. The first minute holds only 48 ticks, all Paris (`.FR`) index
updates, arriving at the pace they happened in 2021: a burst at 07:00:00, then roughly one line a second. The
morning picks up within about 20 minutes, as the Paris and Amsterdam (`.NL`) indices start updating. At 09:00
Xetra (`.ETR`) opens; the 09:00 hour has 1.3 million ticks, about 370 a second. To get there within the
minute, set `REPLAY_SPEED=60` (one hour of trading per minute) or `600`.

The first lines look like this:

```
partition 2  {"symbol":"ISBTC.FR","sectype":"I","last":0.738,"ts_ms":1636351200023,"seq":5179,"sent_ms":1792321990110}
partition 4  {"symbol":"IUSAC.FR","sectype":"I","last":419.36,"ts_ms":1636351200023,"seq":5176,"sent_ms":1792321990110}
partition 9  {"symbol":"IGAGU.FR","sectype":"I","last":61.3605,"ts_ms":1636351200023,"seq":5174,"sent_ms":1792321990110}
```

## 2. As fast as the machine can

This is the same command with `REPLAY_SPEED=max`. It also prints the producer's own summary at the end.

```sh
docker rm -f producer-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic delete ticks-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic create ticks-demo -p 32 >/dev/null
docker compose --profile replay run -d --no-deps --name producer-demo --entrypoint "timeout 60 replayer" \
  -e TICKS_TOPIC=ticks-demo -e DAYS=081121 -e REPLAY_SPEED=max replayer >/dev/null 2>&1
docker compose exec redpanda sh -c "trap 'exit 0' INT; timeout --foreground 60 rpk topic consume ticks-demo -o start -f 'partition %p  %v\n' || true"
docker logs producer-demo 2>&1 | grep 'done:'
```

The producer sends the whole day, 11.8 million ticks, within the first few seconds. Printing takes longer:
lines appear as fast as your terminal can draw them. A fast terminal gets through all 11,794,552 records
within the minute; a slower one is still somewhere in the trading day when the minute ends. The last line is
the producer's own report, for example:

```
2026/10/18 11:14:25.240686 done: 11794520 ticks and 32 watermarks in 6.811s (1731595 ticks/s), 0 produce errors
```

To see how fast a consumer reads without the terminal in the way, count the records instead of printing them:

```sh
docker compose exec -T redpanda sh -c "timeout 60 rpk topic consume ticks-demo -o start -f '%v\n' | wc -l"
```

Once it has read everything it waits for more, so it always takes the full minute. It should print
11794552 (the ticks plus the 32 watermarks).

## 3. Start right before the action

`REPLAY_FROM` starts the replay at a time of day (Berlin time, on the first day in `DAYS`). The producer
skips every earlier tick, and its clock starts at exactly that moment. Set `FROM` to one of the times in the
table below; at real speed you get about ten quiet seconds, and then the burst.

```sh
FROM=08:59:50
docker rm -f producer-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic delete ticks-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic create ticks-demo -p 32 >/dev/null
docker compose --profile replay run -d --no-deps --name producer-demo --entrypoint "timeout 60 replayer" \
  -e TICKS_TOPIC=ticks-demo -e DAYS=081121 -e REPLAY_SPEED=1 -e REPLAY_FROM=$FROM replayer >/dev/null 2>&1
docker compose exec redpanda sh -c "trap 'exit 0' INT; timeout --foreground 60 rpk topic consume ticks-demo -o start -f 'partition %p  %v\n' || true"
```

Monday's busiest moments (8 Nov 2021, Berlin time):

| `FROM` | What happens next | Activity |
| --- | --- | --- |
| `07:14:50` | Paris (`.FR`) and Amsterdam (`.NL`) indices start updating at 07:15:01 | Nothing before; then a batch of about 1,100 ticks every 15 s (4,908 a minute) |
| `08:59:50` | The market opens at 09:00:00, Xetra (`.ETR`) included | About 2 ticks a second, then 3,106 in the first second and 800–1,800 a second for 20 s. 47,881 in the minute, the day's second busiest |
| `15:29:50` | US markets open at 15:30 Berlin time | 2,922 ticks at 15:30:00; the next ten minutes run 3–17% busier than the ones before |
| `17:34:50` | The closing auctions print at 17:35:00 | About 150 a second, then 600–3,900 a second for 30 s. 53,254 in the minute, the day's busiest |

Between 09:00 and 17:30, trading is steady at about 20,000 ticks a minute: around 180 a second, plus index
snapshots of about 2,400 ticks every 15 s (at :00, :15, :30 and :45). Before 07:15 and after 17:40 it is
quiet (about 10 ticks a second in the evening). To cover more ground within the minute, combine `FROM` with
a speed, e.g. `-e REPLAY_SPEED=10` shows ten minutes from the open.

## 4. Choose how many threads the producer uses

`REPLAY_THREADS` caps how many CPU threads the producer's Go runtime may run at once (its `GOMAXPROCS`). By
default it uses every CPU the container has. This runs the whole day at full speed without printing, so the
producer's own speed is what you measure, and shows its start and summary lines:

```sh
THREADS=2
docker rm -f producer-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic delete ticks-demo >/dev/null 2>&1
docker compose exec redpanda rpk topic create ticks-demo -p 32 >/dev/null
docker compose --profile replay run --rm --no-deps -e TICKS_TOPIC=ticks-demo -e DAYS=081121 \
  -e REPLAY_SPEED=max -e REPLAY_THREADS=$THREADS replayer 2>&1 | grep -E 'replaying|done:'
```

Try `THREADS=1`, then `2`, `4` and `8`, and compare the `done:` lines. On the 8-CPU laptop this was written
on, with other work running at the same time, one thread took 25–36 s, two took 11–18 s, four took 8–15 s
and eight took 7–14 s. The first extra threads help most. Past four, which is the default number of workers,
the gain is small: the workers and the broker are the limit then.

`REPLAY_THREADS` is different from `REPLAY_WORKERS` (default 4):

- **`REPLAY_WORKERS`** is how many streams read and send buckets in parallel; worker *w* owns the buckets
  with `bucket % N == w`.
- **`REPLAY_THREADS`** is how many of those, plus the Kafka client's compression and network work, can run
  on a CPU at the same moment.

Both can be added as `-e REPLAY_THREADS=…` or `-e REPLAY_WORKERS=…` to any command above, or set in `.env`.

## 5. Speed sweep: how fast can this machine fast-forward?

`bench/replay-speeds.sh` replays the whole day into `ticks-demo` at a series of speeds, and measures how far
the producer falls behind its schedule at each one. Run the same sweep on each machine and operating system
you want to compare: it records a description of the machine with every result.

**Setting up a new machine**

1. Clone the repository. On Windows, clone it inside the WSL2 filesystem, not on `C:`, and run everything
   from a WSL2 shell.
2. Put the CSV files in `../data/raw/`, then run `cp .env.example .env`.
3. Prep Monday: `docker compose --profile prep run --rm prep` (about a minute).
4. With Docker Desktop (macOS, Windows), its CPU and memory settings cap what the containers get. Note them,
   or set them alike on every machine; the script records what Docker reports.

**Running it**

```sh
bench/replay-speeds.sh
```

This starts Redpanda if needed and makes one unmeasured warm-up run. It then runs each speed twice (1,000×,
2,000×, 4,000×, 6,000×, 8,000×, 12,000×, 16,000× and 24,000×), which takes about 5 minutes. You can choose
the speeds and other settings:

```sh
bench/replay-speeds.sh 3000 5000 7000              # your own speeds
RUNS=3 bench/replay-speeds.sh                      # more runs per speed
REPLAY_THREADS=2 bench/replay-speeds.sh            # fewer producer threads (see section 4)
COMPOSE_FILE=compose.yaml:compose.perf.yaml bench/replay-speeds.sh   # the Linux performance setup
```

With `compose.perf.yaml`, Redpanda runs without dev mode and the producer is pinned to the cores in
`CPUS_REPLAYER`, so compare those results only with each other. The `CPUS_*` values in `.env` are an example
for 16 threads: set them to cores the machine has, or the containers will not start.

**Reading the result**

This is the result from an Apple M1 Pro laptop (macOS, Docker Desktop with 8 CPUs and 7.7 GiB):

```
Host:    khmbp, macOS 27.0.1, Apple M1 Pro, 8 cores
Docker:  29.8.2, Docker Desktop, aarch64, 8 CPUs, 7.7 GiB
Replay:  days 081121, workers 4, threads all, compose compose.yaml

  speed  run    planned       took      ticks/s     max lag   lag market  errors  verdict
  1000×    1    61.200s    61.206s       192700      46.7ms       0.8 min       0  ok
  1000×    2    61.200s    61.209s       192691     240.9ms       4.0 min       0  ok
  2000×    1    30.600s    30.608s       385341     165.3ms       5.5 min       0  ok
  2000×    2    30.600s    30.603s       385400      39.4ms       1.3 min       0  ok
  4000×    1    15.300s    15.305s       770652      57.3ms       3.8 min       0  ok
  4000×    2    15.300s    15.307s       770534     107.6ms       7.2 min       0  ok
  6000×    1    10.200s    10.208s      1155404     504.1ms      50.4 min       0  behind
  6000×    2    10.200s    10.211s      1155060      22.4ms       2.2 min       0  ok
  8000×    1     7.650s     7.659s      1540032    1422.4ms     189.7 min       0  behind
  8000×    2     7.650s     7.659s      1539913    1150.8ms     153.4 min       0  behind
 12000×    1     5.100s     5.598s      2107024    2307.0ms     461.4 min       0  too fast
 12000×    2     5.100s     5.134s      2297302    1842.3ms     368.5 min       0  behind
 16000×    1     3.825s     5.207s      2265073    2715.4ms     724.1 min       0  too fast
 16000×    2     3.825s     5.161s      2285241    2673.7ms     713.0 min       0  too fast
 24000×    1     2.550s     5.038s      2340942    3362.6ms    1345.0 min       0  too fast
 24000×    2     2.550s     5.078s      2322465    3403.4ms    1361.4 min       0  too fast

Fastest speed on schedule in every run (and every slower speed): 4000×
Results: bench/results/replay-speeds-khmbp-20261018-1851.csv
```

| Column | Meaning |
| --- | --- |
| `planned` | How long the replay should take at that speed: Monday from 07:00 to the final watermark at midnight, divided by the speed |
| `took` | How long it actually took |
| `ticks/s` | Average sending rate over the run |
| `max lag` | The furthest the producer fell behind its schedule at any moment |
| `lag market` | The same lag in market time (lag × speed): how far behind the replayed clock was |
| `errors` | Records the broker did not accept. Speed does not cause these: a producer that is too fast falls behind instead |
| `verdict` | `ok`: never more than 1% of the replayed span behind (about 10 market minutes for Monday). `behind`: more than that, usually during trading hours, catching up in the quiet evening. `too fast`: took more than 2% longer than planned, i.e. the producer ran flat out |

The last line names the fastest speed that was `ok` in every run, along with every slower speed. On the
laptop above, that is 4,000×. Around 6,000× is borderline, and from 12,000× up the producer simply sends as
fast as it can (about 2.3 million ticks a second). That throughput is the other number worth comparing
between machines.

This measures the producer and Redpanda only. With the whole pipeline running on the same machine, the
signal service and ClickHouse share the CPUs, so the usable speed is lower: on the laptop above, everything
kept up to about 1,500×.

**Comparing machines**

Every sweep writes `bench/results/replay-speeds-<host>-<date>.csv`, with one row per run. Each row carries
the machine's details: host, OS, CPU model and cores, Docker's CPUs, memory, OS and version, architecture,
and the compose files. Commit these files, and collect the files from every machine into one table:

```sh
awk 'FNR > 1 || NR == 1' bench/results/replay-speeds-*.csv > all-speeds.csv
```

For a fair comparison, close other heavy programs. Plug laptops in, since on battery they slow down. Use the
same `DAYS`, `RUNS` and compose files everywhere.

## Reading the output

Each record is one JSON object. Its Kafka key is the symbol, and its partition is the symbol's bucket.

| Field | Meaning |
| --- | --- |
| `symbol` | Instrument ID, e.g. `MC.FR`; empty means a watermark (see below) |
| `sectype` | `E` for equity, `I` for index |
| `last` | Trade price |
| `ts_ms` | Event time in epoch milliseconds (Trading date + Trading time, Europe/Berlin) |
| `seq` | Row number in the day's CSV; breaks ties between equal times |
| `sent_ms` | When the producer sent the record (wall clock), for latency measurements |

A watermark has an empty symbol and `ts_ms` set to the next midnight. The producer writes one at the end of
each bucket-day, as the last record of that day in the partition. It tells consumers that no earlier tick
will follow. In the fast demo they appear at the end of the day.

Ctrl-C stops the printing early. The producer stops by itself after the minute either way.

## Clean up

```sh
docker rm -f producer-demo
docker compose exec redpanda rpk topic delete ticks-demo
docker compose stop redpanda
```
