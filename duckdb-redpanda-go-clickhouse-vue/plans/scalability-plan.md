# Scalability plan

Work plan for meeting the scalability requirements in `../../scalability_requirements.md` (the review of the code against the requirements: what is addressed, the gaps ranked by grading risk, and the missing experiments). This file holds the decisions, the order of work, and enough detail per step to pick any step up cold. Tick items off (`- [x]`) in the commit that finishes them.

## How to work on this

- **Correctness gate after every code change:** `clickhouse/check.sh` passes at K = 1 and K = 5 signal instances. Run it with the same `EMA_INIT` and crossover rule as the service. No step is done until this holds.
- **Measure before and after.** Each change in steps 2–6 gets one before/after number from the same command on the same machine. The before/after pairs are the "measured trade-offs" the Creative explorations criterion asks for.
- **Report-grade numbers come only from the benchmark VM** with `COMPOSE_FILE=compose.yaml:compose.perf.yaml`. Laptop runs are for development.
- **Commit results.** Everything a bench script writes to `bench/results/` (Markdown, CSV, `stats-*.csv`, `profile-*.pb.gz`) gets committed with a one-line note of what was run.
- Paths are relative to `duckdb-redpanda-go-clickhouse-vue/`.

## Decisions

| Topic | Decision | Source |
|---|---|---|
| Deployment | One host. Scale-out is shown by varying signal instances and pinned cores. No multi-VM demo. | Team, 2026-11-06 |
| Broker | One Redpanda broker, replication factor 1, stated as out of scope in the report. Revisit only if a sweep shows the broker as the bottleneck and `REDPANDA_SMP=2` does not fix it. | Team, 2026-11-06 |
| Benchmark machine | Linux VM, 10 vCPUs, 12 GB RAM, ≥ 40 GB SSD, on a host with an AMD Ryzen 5 5600-series CPU (6 cores / 12 threads, all cores the same type). Record the exact model from `lscpu` for the report. | Team, 2026-11-06 |
| Core layouts | Layout B (end-to-end, sweeps) is the default in `.env.example`; layout A (drain and scaling tests) is in the comment below it. Probe, Prometheus and Grafana share `CPUS_GATEWAY`. | `.env.example`, `compose.perf.yaml` |
| EMA start | **Keep `EMA_INIT=first_close`**: the first window's close seeds both EMAs. Realistically the previous EMA would come from the end of the previous week; the professor confirmed either start is fine for the course as long as the report documents it. `zero` stays available for comparisons. | Professor, via team, 2026-11-06 |
| Crossover rule | **Open: see step 0.** | — |

SMT note for every scaling result: 10 vCPUs sit on about 5 physical cores, so curves are expected to flatten before K = 5 for hardware reasons. State "6 cores / 12 threads, 10 vCPUs to the VM" next to each plot.

## Status

- [ ] Step 0: crossover rule (decision, then implementation)
- [x] Step 1a: instrumentation (code)
- [ ] Step 1b: baseline runs on the VM
- [ ] Step 2: more than one core per signal instance
- [ ] Step 3: cheaper per-tick processing (profile first)
- [ ] Step 4: long tail and partition skew
- [ ] Step 5: periodic watermarks and allowed lateness
- [ ] Step 6: subscriber path
- [ ] Step 7: next bottlenecks, only if measured

---

## Step 0: Crossover rule

**The spec (equations 1–4).** BUY at window *i* iff EMA38ᵢ > EMA100ᵢ **and** EMA38ᵢ₋₁ ≤ EMA100ᵢ₋₁; SELL iff EMA38ᵢ < EMA100ᵢ **and** EMA38ᵢ₋₁ ≥ EMA100ᵢ₋₁. Equal EMAs at *i−1* satisfy both ≤ and ≥, so the first deviation from equality fires a signal in the direction of the deviation.

**Ours today (the "flip" rule, `signal/state.go:166-186`, `clickhouse/oracle.sql:27-35`).** It fires only when the sign of EMA38 − EMA100 changes from the last *non-zero* sign. The first non-zero sign only sets the baseline, and a tie never fires.

**Where they differ in practice.** With `first_close` seeding, both EMAs are equal after a symbol's first window. In the next window with a different close, EMA38 moves further than EMA100 (α38 > α100), so the spec rule fires once per symbol, in the direction of that price move. That is roughly one extra signal for most of the ~4,970 symbols over the week, against 5,735 signals on Monday today. It is an artefact of the seeding, not a trend crossover. Mid-stream exact ties (+,0,+ and −,0,−) also fire under the spec but not under flip; once the EMAs have diverged, those are rare.

**Recommendation (team to confirm):** implement the spec rule exactly, with an explicit, documented warm-up: **the comparison against the seeded equality does not fire**. A symbol's first comparison with a non-equal previous window then sets nothing extra, and every later window follows the spec's ≤/≥ rule literally. Keep `flip` as a measured variant. In the report, count the three variants on the week: spec without warm-up, spec with warm-up, flip.

**Tasks**
- [ ] Decide: spec + warm-up (recommended), spec without warm-up, or keep flip and document it.
- [ ] `CROSSOVER_RULE=spec|flip` and `SIGNAL_WARMUP=seed|none` in the signal service.
  - For `spec`, store the previous window's EMA38 and EMA100 in `series` (`signal/state.go:53-65`) and compare directly, with no sign memory. Under `EMA_INIT=zero` the previous EMAs before the first window are (0, 0).
  - `seed` suppresses only the comparison whose previous window is the seeding window.
- [ ] `oracle.sql`: the same rule, using `lagInFrame` of both EMAs over `flat`, with the same warm-up. `check.sh` passes both settings through to the queries.
- [ ] Unit tests in `signal/state_test.go`: sign sequences 0→+, −,0,+, +,0,+, −,0,−; the seeded first deviation with and without warm-up; the first window under `zero`.
- [ ] Defaults in `.env.example`, `compose.yaml` (passes the env), and the service's `loadConfig`.
- [ ] Update the "Signal rules" section and the todo in `PLAN-2026-11-04.md`.
- [ ] Count signals per variant on the week; note the 229 rounding-noise flips on Monday (`PLAN-2026-11-04.md:447`) and how each variant treats them.

**Done when** `check.sh` passes for each variant, and the counts are in `bench/results/`.

---

## Step 1: Instrumentation and baseline

### 1a. Instrumentation (done 2026-11-06)

- `signal_ticks_total` is now labelled by `partition` (`signal/main.go`). Per-instance totals are `sum by (instance)`, per-partition load is `sum by (partition)`.
- The signal service serves `/debug/pprof` on its metrics port (9100, not published). `bench/profile-signal.sh [SECONDS] [INSTANCE]` takes a CPU profile from a running instance, saves it to `bench/results/`, and prints the top functions.
- `bench/stats.sh OUT.csv` samples `docker stats` (CPU, memory, network, disk per container) about once a second. `full-speed.sh`, `signal-drain.sh` and `realtime.sh` start it automatically and write `bench/results/stats-<script>-<host>-<stamp>.csv`.
- `compose.perf.yaml` pins `probe`, `prometheus` and `grafana` to `CPUS_GATEWAY`.
- `.env.example` has the 10-vCPU layouts.
- Grafana: new panels "Ticks per partition (skew)", with a max/mean line, and "CPU per Go service" (`process_cpu_seconds_total`).

### 1b. Baseline runs on the VM (current code, before step 2)

Set up the VM: clone the repo, copy `.env.example` to `.env`, set the password, check `CPUS_*` against `nproc` (10), and prep the week (`DAYS="081121 091121 101121 111121 121121"`).

- [ ] Layout B: `DAYS="…all five…" bench/full-speed.sh` on fresh volumes, then `clickhouse/check.sh`. Gives week throughput, CPU and the correctness gate (Req 1, 8, 10).
- [ ] Layout B, same run with `REPLAY_SPEED=300` instead of max: latency stays stable across the week (Req 1, 9). Run with `--profile observe` and export the lag and latency panels as CSV.
- [ ] Layout A: `for k in 1 2 3 5; do bench/signal-drain.sh $k; clickhouse/check.sh; done` on the pre-filled week. Gives the scaling curve, correctness across K, and CPU per instance (Req 5, 8, 10).
- [ ] During the K = 1 drain: `bench/profile-signal.sh 30` (input for step 3).
- [ ] Layout B: `MODE=sweep SWEEP_WINDOWS=p100 SWEEP_SPEEDS="300 1000 1500 2000 3000" SIGNAL_INSTANCES="1 2 3" bench/realtime.sh` (Req 3, 9).
- [ ] Record the per-partition max/mean from Grafana during the p100 sweep (Req 4).

**Done when** these result files are committed and summarised in a short `bench/results/baseline-<host>.md`: throughput, latency p50/p99, CPU per component, max/mean per partition, check results.

---

## Step 2: More than one core per signal instance (Req 5, 9)

**Problem.** One goroutine handles every partition an instance owns, under one mutex, and each poll blocks on `ProduceSync` and the commit before the next poll (`signal/main.go:266-335`). An instance therefore uses about one core, and adding containers is the only way to scale. A rebuild after a rebalance also holds the mutex, so it stalls the instance's other partitions (`signal/main.go:278`, `290-293`).

**Approach (first version: small diff, same commit semantics)**
1. After `PollRecords`, snapshot the `owned` pointers for the fetched partitions under `a.mu`, then release the lock.
2. Process each partition in its own goroutine (an `errgroup` with limit `runtime.GOMAXPROCS(0)`). Each goroutine:
   - rebuilds if needed,
   - decodes and applies its records,
   - produces its own outputs with `ProduceSync`.

   Partitions share no state, so this is safe. Metrics are already safe for concurrent use.
3. After all goroutines finish without error: `CommitUncommittedOffsets`, then `AllowRebalance`, as today. `BlockRebalanceOnPoll` stays. Check in the franz-go docs whether `OnPartitionsLost` can fire while processing runs; if so, keep the snapshot approach and ignore a lost partition's results.
4. Rebuild: fail with an error if the first record read is not offset 0 (retention removed history). Today the state would silently be partial (`signal/main.go:228`).

Optional second version: pipelining, i.e. poll the next batch while the previous one is being produced. Commit offsets per partition only after that partition's outputs are acknowledged (`MarkCommitRecords` + `CommitMarkedOffsets`). Do this only if the first version leaves the instance waiting on produce (the batch histogram and the profile show it).

GOMAXPROCS follows the container's cpuset on Linux, so an instance on 5 pinned vCPUs gets 5.

**Verify**
- [ ] Unit tests still pass. `check.sh` passes at K = 1 and K = 5, and after `docker compose restart signal` mid-drain (forces a rebuild).
- [ ] Layout A: drain with 1 instance × 5 vCPUs against 5 instances × 1 vCPU each (pin by running instances with separate cpusets, or compare `--scale signal=5` on 5 vCPUs). Report both against the step 1b baseline.
- [ ] Re-run the step 1b sweep with K = 1.

---

## Step 3: Cheaper per-tick processing (Req 3, 5)

**Do this only after the step 1b profile.** Expected hotspot: `json.Unmarshal` into `tick` (`signal/main.go:301`, and the rebuild at `252`). It uses reflection for six fixed fields.

- [ ] If decoding dominates: a hand-written decoder for the tick format the replayer writes (`replayer/main.go:286-301`), which falls back to `encoding/json` on anything unexpected. A unit test runs both decoders on generated ticks (odd floats, escapes, empty symbol for watermarks) and requires identical results.
- [ ] If output encoding shows up: `appendJSONString` calls `json.Marshal` per bar (`signal/main.go:178-181`); copy the replayer's allocation-free version.
- [ ] Re-profile and re-run the K = 1 drain. Report ticks/s per core before and after.

A binary wire format (Avro, Protobuf, fixed layout) is the bigger alternative. Mention it in the report as a trade-off; implement it only if the hand-written decoder is not enough.

---

## Step 4: Long tail and partition skew (Req 4)

**Facts** (measured 2026-11-06 over the prepped week, see the requirements review):
- With `hash(symbol) % 32` (`prep/prep_day.sql:14`), the busiest partition gets 1.52× the mean over the week, and 1.62× in the busiest 5 minutes.
- The top symbol is 0.27% of ticks, so the skew comes from hash collisions, not from one dominant symbol.
- At K = 32 this caps the ideal speedup near 21×. At K ≤ 5 it partly averages out, so the measurement matters more than the fix on this hardware.

**Tasks**
- [ ] Measure: per-partition load and per-instance load during the p100 sweep (new panel), plus latency for hot vs. rare symbols. Split the probe's per-bar latencies by the symbol's weekly tick-count decile in `bench/probe/analyze.go`.
- [ ] Load-balanced bucket map in prep. Compute tick counts per symbol from a **reference day** (Monday, or the previous day), not from the day being prepped, so the mapping uses no future knowledge. Assign symbols greedily, largest first, to the lightest bucket. Symbols missing from the reference fall back to the hash.
  - Write the map to `data/ticks/bucket_map.parquet` and join it in `prep_day.sql`.
  - Log max/mean before and after.
  - The replayer needs no change (bucket = partition).
- [ ] Re-run `check.sh` (results must be unchanged: only the placement moves) and the p100 sweep; compare per-partition max/mean and throughput.
- [ ] Partition-count trade-off: re-prep with `PARTITIONS=16` and `64` into separate `DATA_DIR`s on fresh volumes. One sweep step each. Show skew, throughput, and close latency in quiet hours (step 5 interacts).
  - `redpanda/create-topics.sh` adds partitions to an existing topic. Always start these runs on fresh volumes so old data stays under its old mapping.

---

## Step 5: Periodic watermarks and allowed lateness (Req 8, 9)

**Problem.**
- A window closes only on the next record in its partition, plus a day-end watermark (`replayer/main.go:236-238`). Quiet partitions close late, and more partitions make each one quieter.
- The service allows zero lateness (`signal/state.go:95-98`) and relies on prep sorting each bucket (`prep/prep_day.sql:24`). The rubric asks for handling of late and out-of-order events.

**Tasks**
- [ ] Measure disorder in the raw CSVs, read-only. In file order, find the share of trade rows earlier than the running max event time per symbol and per bucket, and the lateness p50/p99/max per exchange. This decides how much of the rest matters, and it belongs in the dataset analysis.
- [ ] Periodic watermarks in the replayer, every `WATERMARK_MS` of event time (e.g. 1000). One replayer worker pops records in global event order across its buckets. When the heap head first reaches or passes a boundary T, every bucket of that worker has its next tick at or after T, so a watermark T is valid for each of them. Emit one per bucket at that moment. Count them in `replayer_records_total{kind="watermark"}`. `check.sh` gate 3 already counts watermarks.
- [ ] `ALLOWED_LATENESS` in the signal service:
  - The watermark becomes max event time − lateness.
  - A window closes only when that watermark passes its end, or when the symbol's next tick is beyond end + lateness.
  - Ticks for closed windows are counted as late and dropped.
  - Replayer watermarks are already lateness-safe for sorted input.
- [ ] `PREP_ORDER=receipt`: buckets in file order (`ORDER BY seq`) instead of event order. The oracle stays on event-time order, so `check.sh` shows what disorder costs. With receipt order, replayer watermarks are not valid: disable them (`WATERMARK_MS=0`) and rely on the service's lateness bound.
- [ ] Sweep `ALLOWED_LATENESS` = 0, 1 s, 5 s, 30 s in receipt order. Report late ticks, bars and signals that differ from the oracle, and window-end → UI latency. Pick the default from that.
- [ ] Close latency in quiet hours with and without periodic watermarks (realtime p10/p35 windows).

---

## Step 6: Subscriber path (Req 6, 7)

**Problem.**
- Every signal goes to every client (`gateway/hub.go:150-156`), so per-client cost grows with all signals, not with the subscription.
- `/symbols` scans `bars_ema FINAL` on the single ClickHouse on every request (`gateway/main.go:378-387`).
- Only one test client has ever been used.

**Tasks**
- [ ] Gateway: signals only for subscribed symbols, plus an explicit `{"subscribe_signals":"all"}` (or similar) so the UI's signal grid keeps working. Document the protocol change in `PLAN-2026-11-04.md` ("Topics and tables").
- [ ] Cache the `/symbols` result in the gateway and refresh it every few seconds. Requests then never reach ClickHouse directly.
- [ ] Load clients: give `bench/probe` a mode with N clients per process, each with its own random watchlist of W symbols. Connect clients to gateway instances directly by container IP or round-robin over DNS results, so no load balancer is needed.
- [ ] Subscriber isolation (Req 7): one sweep step (p100 at 300×) with the gateway stopped, with 1 client, and with N clients. Compare `signal_tick_latency_seconds` and `signal_batch_seconds`.
- [ ] Fan-out capacity (Req 6): 1, 10, 100, 1000 clients against 1 gateway, then against `--scale gateway=2`. Record `gateway_ws_dropped_total`, frames, and gateway CPU (stats CSV).
- [ ] `FLUSH_MS` is already configurable (`gateway/main.go:107`). Measure 50 against 250 ms for latency and gateway CPU.

---

## Step 7: Next bottlenecks, only if measured

- ClickHouse falls behind on `ticks`: raise `CH_KAFKA_CONSUMERS` (`clickhouse/init/20-kafka.sh:52`; re-run the script to apply).
- The broker saturates (Redpanda CPU in the stats CSV near 100% of its cpuset): `REDPANDA_SMP=2`.
- The replayer cannot offer enough load for a sweep: raise `REPLAY_WORKERS`, or give it more cores.
- Multi-VM stays out of scope. For "future work" in the report: make `--advertise-kafka-addr` an `.env` variable (`compose.yaml:32`), run `signal` with `--no-deps` against a remote broker, and replace compose-DNS discovery in `observe/prometheus.yml`.

---

## Report checklist (scalability parts)

- [ ] Hardware line with every result: host CPU model, cores/threads, vCPUs, RAM, layout A or B.
- [ ] Scaling curves (ticks/s vs K and vs cores) with CPU utilisation, and the SMT note.
- [ ] Peak capacity (p100 sweep) and the busiest real second for comparison.
- [ ] Per-exchange throughput and latency (ETR 53.9%, FR 36.0%, NL 10.1% of ticks).
- [ ] Skew: max/mean per partition before and after step 4.
- [ ] Subscriber isolation and fan-out results.
- [ ] The stream carries trade rows only (59.4M of ~289M rows); say why the filtering happens in prep.
- [ ] Documented choices: EMA start (`first_close`, agreed with the professor), crossover rule and warm-up, skipped empty windows, lateness policy, single broker with replication factor 1.
