# Scalability requirements: Detecting Trading Trends in Financial Tick Data

Sources: course project description, evaluation criteria (Scalability, Performance, Evaluation and performance rows), example report.
Requirements are qualitative on purpose. Concrete numbers come from our own evaluation.

Rubric anchor (Scalability, top grade): "Scales effectively across all exchanges and event volumes, including peak rates and the long-tail symbol distribution, with capacity for further increases (more symbols, more subscribers, more resources)."

Reviewed against commit `23c1432`. The work plan that follows from this review, with decisions and status, is `duckdb-redpanda-go-clickhouse-vue/plans/scalability-plan.md`; step 1 there has since added per-partition tick metrics, pprof, and `docker stats` sampling in the bench scripts (Req 4 and 10 instrumentation, no results yet). Paths in the Details section are relative to `duckdb-redpanda-go-clickhouse-vue/` unless they start with `data/` or `notebooks/`.

**Status legend.** *Addressed*: the code implements it **and** the repo holds a result that shows it. *Partially*: the code implements part of it, or implements it with no report-grade result yet. *Not addressed*: the code does not implement it. *Unclear*: the answer depends on runtime config or deployment.

**Evidence grade.** Every result in `bench/results/` comes from a MacBook Air M2 under Docker Desktop. That setup uses `compose.yaml` alone, with Redpanda in dev-container mode (no fsync) and no core pinning. The repo itself calls these numbers not report-grade (`compose.perf.yaml:1`, `bench/results/full-speed-…-1055.md:6`). Claims that live only in prose under `plans/`, with no result file or script output, are marked *(prose only)*.

| # | Requirement | How the current design addresses it | Evidence / metric | Status |
|---|---|---|---|---|
| 1 | Process the full week (289M events, 5,504 symbols) without falling behind | Replayer streams all `DAYS` into `ticks`, and topics never expire by time. Prep drops non-trade rows before streaming, so the stream carries 59.4M ticks and 4,970 symbols, not 289M and 5,504. The whole week has never been run in one go. | Sustained events/s; consumer lag over time | Partially |
| 2 | Handle all three exchanges (FR, NL, ETR) together | Exchange is not a dimension anywhere in the stream: symbols from all exchanges are hashed into the same 32 partitions. Nothing measures per exchange and there is no per-exchange replay switch. | Throughput and latency per exchange vs combined | Partially |
| 3 | Sustain peak event rates, not just the average | Paced replay (`REPLAY_SPEED`) plus a sweep over the busiest window (p100). With 1 signal instance it holds 1000× (~360k ticks/s, p95 233 ms) and falls behind at 3000×. | Max sustainable rate (accelerated replay); latency at peak | Partially |
| 4 | Cope with the skewed (long-tail) symbol distribution | Static `hash(symbol) % 32` in prep, so no single symbol dominates (top one is 0.27%). Hash collisions still make the hottest partition 1.52× the mean (1.62× at peak), and nothing rebalances or measures it per partition at runtime. | Per-partition load and CPU spread; latency for hot vs rare symbols | Partially |
| 5 | Scale out by adding resources (workers, partitions, VMs) | Signal service is a consumer group: `--scale signal=K`, K ≤ 32, one processing goroutine per instance. Gateway scales by instance. Redpanda, ClickHouse and the replayer each run as one instance. Nothing is configured for a second VM; by decision, scale-out is shown on one host (see Decisions). | Throughput and latency vs number of workers/cores on one host | Partially |
| 6 | Headroom for more symbols and more subscribers | Per-symbol state is a map per partition, so memory grows linearly with symbols. Changing the partition count means re-running prep. Every gateway reads every bar and signal, and every signal goes to every client. History comes from a single ClickHouse. None of this is tested. | Behaviour as symbol count and subscriber count increase | Partially |
| 7 | Subscribers do not affect the core computation (all symbols always computed; output only for the subscribed subset) | The signal service has no knowledge of subscribers and computes every symbol. Output goes through Redpanda topics that the gateway reads in its own consumer group without committing, and per-client buffers drop rather than block. Bars are filtered per subscription, but signals go to every client. | Processing latency with 0, 1, many subscribers | Partially |
| 8 | Results stay correct at any parallelism (per-symbol grouping and order, 5-min tumbling windows, per-symbol EMA state, window w_i evaluated when w_{i+1} starts) | State is per partition, and partition is a fixed function of symbol. Windows use event time and are clock-aligned. A rebalance rebuilds state from offset 0. Outputs are acknowledged before offsets are committed. Disorder is removed by sorting in prep: the service allows zero lateness. | EMA and buy/sell advisories identical across worker counts vs a single-worker reference | Partially |
| 9 | Latency stays stable as load and parallelism grow | Latency is measured at every stage (histograms, probe). Latency against load exists for 1 instance only, over 1-hour spans. | Latency distribution (p50/p99) vs load; no backlog growth | Partially |
| 10 | Resource use is observable per component | Prometheus scrapes all five components, and Grafana shows throughput, lag, latency and Go heap. There is no per-container CPU, network or disk collector, and no run has recorded utilisation. | CPU, memory, network, disk per component | Partially |

Notes
- Requirement 8 is the most likely to be lost when scaling out; test it explicitly.
- Total order of events across symbols is not assumed (project description, section 2.1).
- Numbers marked *(measured in review)* were computed during this review with DuckDB 1.5.6, read-only, over `data/ticks/bucket=*/*.parquet`. They are not stored in the repo; the "Missing experiments" section says how to make them reproducible.

---

## Details

### 1. Full week without falling behind

**Design**
- The replayer reads one Parquet file per bucket and day for every day in `DAYS` (`replayer/main.go:185-204`). A worker merges its buckets by event time (`replayer/main.go:252-265`, `322-387`). After each bucket-day it writes a watermark record (`replayer/main.go:236-238`).
- Topics are created with `retention.ms=-1` and `retention.bytes=20 GB`, so nothing expires by time during the week (`redpanda/create-topics.sh:11-14`, `.env.example:39`).
- **The stream does not carry 289M events.** Prep keeps only rows with both a `Last` and a `Trading time`, and drops `Last <= 0` (`prep/prep_day.sql:17`, `22-24`). The week's Parquet holds **59,391,670 ticks and 4,970 symbols** *(measured in review)*, about 11.8–12.1M per day. Monday's prep log reports 11,794,520 trades out of 53,994,554 rows, 21.8% (`plans/PLAN-2026-11-04.md:66`). The other ~230M rows (quotes and housekeeping) are discarded offline by batch prep, never by the streaming system.
- Lag is exported per partition as `signal_partition_lag_records` (`signal/main.go:48-50`, `313`) and plotted in Grafana (`observe/grafana/dashboards/pipeline.json`, panel "Consumer lag").

**Limits**
- Throughput is capped by the signal stage: one goroutine per instance, and each batch blocks on produce and commit (`signal/main.go:266-335`).
- After a rebalance, rebuild time grows with the week, because it re-reads offsets [0, committed) (`signal/main.go:218-264`).

**Existing evidence**
- `bench/results/full-speed-Ohto-MacBook-Air-20261030-1055.md`: **Monday only**, 11.79M records. The signal service drained in 26.2 s (~450k/s) and the backlog peaked at 7.0M records, then drained (lines 21-29, 56-62). `check.sh` passed.
- No whole-week run exists. `plans/PLAN-2026-11-04.md:464` and `474` list both "pass `check.sh` on the whole week" and "replay the whole week" as open todos.
- No lag-over-time series is stored for any run beyond the 5-row backlog table above.

### 2. All three exchanges together

**Design**
- Partitioning ignores the exchange: `hash(column00) % partitions` (`prep/prep_day.sql:14`), where `column00` is the full ID such as `A1PHBB.ETR`. All exchanges therefore share partitions, signal instances and topics. In that sense they are processed together by construction.
- The exchange appears only in the `instruments` table, taken from the ID suffix (`prep/prep.py:161-162`). The gateway joins it in `/symbols` (`gateway/main.go:378-387`).
- Share of ticks *(measured in review)*: ETR 53.9% (2,126 symbols), FR 36.0% (2,285), NL 10.1% (559).

**Limits**
- No metric carries an exchange label: the replayer counts by `kind` (`replayer/main.go:50-52`) and the signal service has unlabelled counters (`signal/main.go:33-41`).
- The replayer has no exchange filter: it reads every row of every bucket (`replayer/main.go:207-250`). Replaying one exchange alone therefore needs a prep change or a separate set of buckets.
- The exchanges trade on different hours. Whether their peaks overlap is not analysed. `notebooks/dataset_overview.ipynb` has no per-exchange section.

**Existing evidence**
- None per exchange. The realtime windows (`bench/results/realtime-…-1649.md:7`) mix all exchanges.

### 3. Peak event rates

**Design**
- `REPLAY_SPEED` paces replay at a multiple of real time (`replayer/main.go:330-343`). `bench/realtime.sh` picks windows by tick-count quantile, including p100, the busiest one (`bench/realtime.sh:192-224`). It then sweeps speeds and judges each against a p95 latency SLO (`bench/realtime.sh:293-321`).
- The replayer's own ceiling is about 0.8–1.0M ticks/s on the laptop (`bench/results/replay-speeds-khmbp-20261018-1851.csv`: on schedule up to 4000×, ~770k ticks/s; behind from 6000–8000×).

**Limits**
- The single-goroutine signal instance (`signal/main.go:266`) saturates at ~530–560k ticks/s achieved (`bench/results/sweep-…-1649.md:12`, `18`).
- Peak per partition matters more than peak in total: in the busiest window the hottest partition gets 1.62× the mean *(measured in review)*. The busiest single partition-second holds 1,014 ticks *(measured in review)*.
- Every result is from the laptop in dev-container mode.

**Existing evidence**
- `bench/results/sweep-Ohto-MacBook-Air-20261030-1649.md`, p100 window Wed 10.11. 17:35, **1 signal instance**: 1000× = 359,435 ticks/s offered, p95 system latency 233 ms, verdict ok. 3000× = too slow (p95 1,222 ms).
- `bench/results/realtime-…-1649.md:9`: the busiest real second is 4,247 ticks/s, roughly 85× below the 1-instance ceiling.
- Each point was run once. There are no repeats and no spread.

### 4. Skewed (long-tail) symbol distribution

**Partition key**
- The partition key is **DuckDB `hash(symbol) % PARTITIONS`, computed in prep** (`prep/prep_day.sql:14`). The replayer writes bucket *k* to partition *k* using a manual partitioner (`replayer/main.go:349-354`, `427`). The record key is the symbol (`replayer/main.go:352`), but the broker never uses it for placement.
- Output keeps the same partition: bars and signals for ticks partition *p* go to partition *p* (`signal/main.go:138`, `159`).

**What happens when a few symbols dominate**
- Distribution *(measured in review, whole week)*:
  - The top symbol is 0.27% of ticks, and the top 5 are each ~0.27%.
  - The top 1% of symbols carry 13.2% of ticks and the top 10% carry 54.7%.
  - 27.9% of symbols have fewer than 100 ticks in the week.
  - So no single symbol dominates. The skew comes from hash collisions among the many mid-size symbols.
- Partition load: buckets hold 1.34M–2.82M ticks over the week, so max/mean is **1.52** (1.50–1.55 per day). In the busiest 5-minute window it is **1.62** *(measured in review)*. Prep prints the same ratio per day (`prep/prep.py:144-148`), and Monday's value of 1.50 is recorded in `plans/PLAN-2026-11-04.md:69`.
- Consequence for scale-out: one partition is processed sequentially by one instance (`signal/main.go:281-315`). At K = 32 the hottest partition sets the pace, which caps the ideal speedup at about 32 / 1.52 ≈ **21×** (the busiest window's 1.62 gives ≈ 20×). At smaller K the consumer group balances partitions **by count, not by load**: there is no custom balancer in `signal/main.go:354-370`, so franz-go's default applies. A hot-symbol mix can therefore land on one instance.
- A single very hot symbol could never be split, because its EMA is inherently sequential. That is acceptable for correctness, but it is a hard per-symbol ceiling.
- Rare symbols are not starved. A window closes when the **partition** watermark passes its end, not only on the symbol's own next tick (`signal/state.go:132-146`). A rare symbol in a busy partition therefore closes on time.

**Limits**
- The mapping is static: no load-aware assignment, no hot-key handling.
- Changing `PARTITIONS` requires re-running prep. `create-topics.sh` will add partitions to an existing topic (`redpanda/create-topics.sh:18-19`), which would leave earlier data under the old mapping.
- Per-partition processing load is not exported. There is lag per partition (`signal/main.go:313`), but `signal_ticks_total` has no partition label (`signal/main.go:33-35`), and CPU per instance is not collected (see 10).
- Quiet partitions close late: windows close only on the next record in the partition, plus a day-end watermark (`replayer/main.go:236-238`). There are no periodic watermarks. **More partitions make each one sparser**, which lengthens close latency in quiet hours: the realtime p35 median wait is 112 ms (`bench/results/realtime-…-1649.md:15`).

**Existing evidence**
- Only bucket-level tick counts: the prep log, and the numbers measured in this review.
- No hot-vs-rare latency comparison. The probe records latency per bar, so the data could support one (`bench/probe/analyze.go`), but no result splits it.

### 5. Scale out (workers, partitions, VMs)

**Component by component**

| Component | Instances | How it scales | Citation |
|---|---|---|---|
| prep | 1, batch | `PREP_THREADS` | `prep/prep.py:30`, `194` |
| replayer | 1 process | `REPLAY_WORKERS` goroutines over buckets (`k % workers`); reads Parquet from a local bind mount | `replayer/main.go:404-420`, `compose.yaml:140-141` |
| redpanda | **1 node**, replication factor 1 | `REDPANDA_SMP` (default 1); topics `-r 1` | `compose.yaml:23-33`, `redpanda/create-topics.sh:13` |
| signal | K, **K ≤ 32** (partitions) | consumer group `signal`; `--scale signal=K` | `signal/main.go:354-360`, `bench/signal-drain.sh:12`, `bench/realtime.sh:104` |
| clickhouse | **1 node** | `CH_KAFKA_CONSUMERS` for ticks (default 4); bars_ema and signals use **1 consumer each** | `clickhouse/init/20-kafka.sh:52`, `69`, `82` |
| gateway | M | own consumer group per instance (`gateway-$HOSTNAME`); fixed host port 8000 in base compose, removed in `compose.perf.yaml` | `gateway/main.go:97`, `compose.yaml:169-170`, `compose.perf.yaml:37` |
| ui (Vite proxy) | 1 | proxies to `http://gateway:8000` | `compose.yaml:187` |
| prometheus / grafana | 1 | — | `compose.yaml:210-235` |

**Signal-service limits**
- Inside an instance, one goroutine handles every assigned partition, under one mutex (`signal/main.go:278-316`). Each batch then blocks on `ProduceSync` and the commit (`signal/main.go:323-331`). More cores per instance therefore do little, and adding instances is the only way to scale.
- Above 32 instances the extra ones sit idle.

**Multi-VM path (not addressed in the repo)**
- Nothing in the repo targets more than one host: no multi-host compose file, no Swarm or Kubernetes manifests. The only mentions are plans (`plans/stock-signals-implementation-plan.md:440`, `491`; `plans/performance-next-steps.md:116`).
- These are the concrete blockers:
  1. **Redpanda's advertised addresses.** It advertises `redpanda:9092` (compose DNS) and `localhost:19092` (`compose.yaml:32`). A client on a second VM bootstraps, then receives an address it cannot reach. `--advertise-kafka-addr` needs the VM's routable address.
  2. **Service dependencies.** `signal` has `depends_on: redpanda-init` (`compose.yaml:153-155`), so `docker compose up signal` on VM2 would also start a broker there. It needs `--no-deps` or a separate compose file, with `KAFKA_BROKERS` pointing at VM1.
  3. **Monitoring.** Prometheus finds instances through compose DNS (`observe/prometheus.yml:15-30`), so it would not see instances on VM2.
  4. **Data locality.** The replayer and ClickHouse read Parquet from a local bind mount (`compose.yaml:93`, `141`). A replayer on another VM needs its own copy of the data.
  5. **Single points.** The broker (RF 1) and ClickHouse are single instances. Spreading only the signal service over VMs still funnels every record through one broker on VM1.
- The smallest working multi-VM setup is: broker, ClickHouse and replayer on VM1, and K signal instances on VM2 (and VM3). That needs change 1 and a way around change 2. Whether it is worth doing depends on the target environment (see Open questions).

**Existing evidence**
- `bench/results/sweep-…-1649.md`: **K = 1 only**. `SIGNAL_INSTANCES` defaults to `"1"` (`bench/realtime.sh:51`).
- `plans/PLAN-2026-11-04.md:446` *(prose only)*: `signal-drain.sh` drained at 0.71M / 1.15M / 1.47M records/s for K = 1 / 2 / 4, including container start, on the laptop. No result file holds these numbers.
- No run on more than one VM. No run varies `REDPANDA_SMP`, `CH_KAFKA_CONSUMERS` or `PARTITIONS`.

### 6. Headroom for more symbols and subscribers

**More symbols**
- Per-symbol state is a `map[string]*series` plus an ordered slice per partition (`signal/state.go:69-76`). Memory grows linearly, and symbols are never evicted.
- On each 5-minute boundary the service loops over every symbol in the partition (`signal/state.go:141-145`), so that cost grows with symbols per partition.
- Partition count is fixed at 32, set both at prep time and at topic creation, so the parallelism ceiling does not grow with symbols (see 4 and 5).
- No synthetic-symbol test exists. `plans/performance-next-steps.md:158` proposes renaming and duplicating the week.

**More subscribers**
- Every gateway instance consumes **all** of `bars_ema` and `signals` (`gateway/main.go:129-133`). That is 373k bars per day on Monday (`bench/results/full-speed-…-1055.md:36`).
- Fan-out:
  - Bars go only to the clients subscribed to that symbol (`gateway/hub.go:142-148`).
  - Every signal goes to every client (`gateway/hub.go:150-156`), so cost is O(clients × signals).
  - Each client gets one frame per `FLUSH_MS` (250 ms) (`gateway/hub.go:170-195`).
- History and symbol lists come from the single ClickHouse. `/symbols` runs a full `bars_ema FINAL GROUP BY symbol` on each call (`gateway/main.go:378-387`), and the UI requests it on load. Many subscribers loading at once therefore land on ClickHouse, not on the stream.
- With `--scale gateway=M`, the Vite proxy points to one DNS name (`compose.yaml:187`), and no load balancer is configured. **Unclear** how clients spread across instances. To find out, run M gateways and check `gateway_ws_clients` per instance.

**Existing evidence**
- None: no multi-client run and no symbol-count scaling run.

### 7. Subscribers do not affect the core computation

**Are all symbols computed regardless of subscriptions?** Yes.
- The signal service reads only `ticks` (`signal/main.go:357`) and has no subscriber input. Every tick goes through `partition.add` (`signal/main.go:299-309`).
- Every closed window is emitted as a bar (`signal/state.go:163-164`), whether or not anyone watches it.

**Is output decoupled from processing?** Yes, through the broker.
- The signal service waits only for Redpanda's acknowledgement before committing (`signal/main.go:321-331`). It never waits for the gateway or ClickHouse.
- The gateway reads `bars_ema` and `signals` in its own group, from the end, and never commits (`gateway/main.go:11-14`, `129-142`).
- A slow browser loses its oldest messages, which are counted, and does not stall the hub (`gateway/hub.go:14-19`, `35-54`).

**Limits**
- Coupling through shared CPU. In the base compose nothing is pinned, so gateway load competes with the signal service for the same cores. `compose.perf.yaml:29-36` gives them separate cpusets, but only on Linux, and no run has used it.
- Signals are broadcast to every client, not only to subscribers (`gateway/hub.go:150-156`). That is a superset of the spec, and it means per-client cost grows with all signals, not with the subscription. This is acknowledged in `plans/performance-next-steps.md:177-179`.
- Duplicates reach clients. After a crash between produce and commit, the outputs are produced again (`signal/main.go:321-322`). ClickHouse removes them (`ReplacingMergeTree`, `clickhouse/init/10-schema.sql:34`, `41`), but the gateway forwards them to clients as they arrive.

**Existing evidence**
- None comparing 0, 1 and many subscribers.
- Each realtime and sweep run used exactly **one** probe subscribed to every symbol (`bench/probe/main.go:3-4`, `98`; `bench/realtime.sh:153-154`).
- `full-speed.sh` does not record whether any client was connected.

### 8. Correctness at any parallelism

**Event time vs processing time**
- Event time is `Trading date` + `Trading time` in Europe/Berlin. A missing date is filled with the file's day (`prep/prep_day.sql:11-12`).
- Windows are clock-aligned 5-minute buckets of event time (`signal/state.go:8`, `82-84`). Processing time is used only for latency fields (`sent_ms`, `emitted_ms`).

**Late and out-of-order events**
- Disorder is removed before streaming, not handled:
  - Prep sorts each bucket by `(event_ts, seq)` (`prep/prep_day.sql:24`).
  - Each bucket is read by a single replayer goroutine, in file order.
  - The producer does not set ordering explicitly (`replayer/main.go:425-431`), so per-partition order relies on franz-go's default idempotent producer.
- The service's watermark is the highest event time seen in the partition, with **zero allowed lateness**. A tick whose window has already closed is dropped and counted in `signal_late_ticks_total` (`signal/state.go:95-98`, `105-109`; `signal/main.go:42-44`).
- Raw receipt-order disorder has not been measured or supported. This is acknowledged in `plans/performance-next-steps.md:77-99` and listed as a todo in `plans/PLAN-2026-11-04.md:467-471`.

**Empty windows**
- Windows are skipped. A new window starts only when a tick arrives (`signal/state.go:112-115`). A window with no trades emits no bar and does not advance the EMA, so the EMA counts traded windows, not wall-clock windows.
- The oracle does the same (`clickhouse/oracle.sql:4-12`), so `check.sh` cannot detect a difference from another interpretation. The spec is silent here; `plans/performance-next-steps.md:177` says to state it in the report.

**"w_i evaluated when w_{i+1} starts"**
- A window closes on the symbol's next tick in a later window (`signal/state.go:105-111`), or when the partition watermark passes its end (`signal/state.go:132-146`). The day-end watermark closes the last windows (`replayer/main.go:236-238`).

**EMA state across restarts and rebalances**
- State lives in memory only (`signal/main.go:183-195`). When a partition is assigned with committed offset C, the instance replays [0, C) without emitting, then continues (`signal/main.go:218-264`, `290-295`).
- Offsets are committed only after outputs are acknowledged (`signal/main.go:321-331`). The result is at-least-once output with deterministic state.
- Limit: rebuild starts at the log start (`AtStart()`, `signal/main.go:228`). If retention had removed early segments, state would be rebuilt from partial history **with no error**. That is unlikely at 20 GB per partition against ~59M ticks for the week, but nothing checks for it.
- Limit: a rebuild runs inside the poll loop while holding the instance mutex (`signal/main.go:278`, `290-293`), so the instance's other partitions stall during it. `RebalanceTimeout` is 5 minutes (`signal/main.go:365`).

**Would results differ between 1 and many workers?**
- By construction, no:
  - Partition is a pure function of the symbol (`prep/prep_day.sql:14`).
  - Each partition's records are applied in offset order by exactly one owner.
  - Bar contents depend only on the symbol's own ticks.
  - With sorted input, a window's ticks all arrive before any record that could close it, so no tick is dropped as late at any partition count.
  - Only the **timing** of closes depends on how symbols share partitions, not the values.
- Not shown by an automated test. `check.sh` compares the service against `oracle.sql` bit for bit (`clickhouse/check.sh:36-71`), and agreement with the oracle at each K would imply equality across K. But:
  - The only recorded pass is K = 1 on Monday (`bench/results/full-speed-…-1055.md:70-73`).
  - "Gate holds when scaled 1→4 mid-drain" is *(prose only)* (`plans/PLAN-2026-11-04.md:436`).
  - No run varies `PARTITIONS`.
  - The unit tests cover one partition only (`signal/state_test.go`).
- Separate from scaling, there are two documented deviations from the spec: EMA seeding (`first_close` by default, `signal/main.go:98-104`) and the flip crossover rule (`signal/state.go:166-186`). See `plans/performance-next-steps.md:21-75`. Both the service and the oracle use them, so `check.sh` passes, but "correct" is relative to the oracle, not to the spec.

### 9. Latency stable as load and parallelism grow

**Design**
- Latency histograms at each stage:
  - replayer produce latency (`replayer/main.go:56-59`)
  - send → signal processing (`signal/main.go:51-54`)
  - batch time (`signal/main.go:55-58`)
  - emit → gateway and end-to-end (`gateway/main.go:58-65`)
- The probe splits latency into the wait for the closing tick, the signal service, and the gateway plus flush (`bench/realtime.sh:284`).

**Limits**
- The gateway's 250 ms flush (`gateway/main.go:107-111`) dominates real-time latency, ~40–200 ms of it (`bench/results/realtime-…-1649.md:15`). The flush sits outside the core, but it is part of every number reported.
- Under overload, latency grows without bound as a queue: mean 7.4–10.5 s at full speed (`bench/results/full-speed-…-1055.md:47-50`, `79-83`). There is no backpressure on the replayer, which is correct for a replay, but it shows the point at which latency is no longer stable.

**Existing evidence**
- Latency against load: the sweep at 10×–3000× for two windows, K = 1 (`bench/results/sweep-…-1649.md`).
- p50 / p95 / p99 per step, each over a 1-hour span run once.
- **Not** against parallelism (K > 1), and **not** over long runs (a whole day or week at a fixed speed).

### 10. Resource use observable per component

**Design**
- Prometheus scrapes Redpanda (`/public_metrics`), ClickHouse (`:9363`), signal, gateway and replayer (`observe/prometheus.yml:7-30`; `clickhouse/config.d/server.xml:7-13`).
- Grafana panels: throughput, per-instance ticks/s, lag, latency, WebSocket fan-out, ClickHouse inserted rows, Redpanda request bytes, and Go heap (`observe/grafana/dashboards/pipeline.json`).

**Limits**
- No container-level collector (cAdvisor or node-exporter), so CPU, network and disk per container are not scraped.
- The Go services export `process_cpu_seconds_total` through the default Prometheus registry on Linux, but no dashboard panel uses it, and it is missing for Redpanda and ClickHouse at the container level.
- Disk I/O is not observed anywhere.
- The `observe` profile is optional (`compose.yaml:211`, `223`), and no bench script starts it.

**Existing evidence**
- None. No result file contains CPU, memory, network or disk figures.
- `plans/performance-next-steps.md:146-149` proposes sampling `docker stats`, but no script does it.

---

## Gaps and risks

Ranked by how likely each is to cost points under Scalability, Performance, and Evaluation and performance.

1. **No scaling curve.** Every recorded result uses 1 signal instance; the K = 1/2/4 drain numbers are prose only. The top Scalability level asks for "capacity for … more resources", and this is the most direct evidence for it. (Req 5, 9)
2. **No report-grade numbers.** Everything ran on a laptop in dev-container mode (no fsync, no pinning), which the repo itself says not to report. Every Performance and Evaluation figure would have to be re-run on the perf setup. (Req 1, 3, 9)
3. **The whole week has never run in one go**, and `check.sh` has passed only on Monday. The Performance top level says "entire week processed smoothly". (Req 1, 8)
4. **No per-exchange results.** The rubric names "all exchanges", and there is no per-exchange metric, replay filter or analysis. (Req 2)
5. **Long-tail and skew are unmeasured at runtime.** Partition max/mean is 1.52 (1.62 at peak), which caps ideal speedup near 21× at K = 32. Processing load per partition and per instance is not exported, and there is no hot-vs-rare latency split. The rubric names "the long-tail symbol distribution" explicitly. (Req 4)
6. **No CPU or resource utilisation recorded.** Evaluation asks for CPU utilisation, and only throughput and latency exist. (Req 10)
7. **Correctness across parallelism is verified only in prose.** No scripted run compares K = 1 with K = N, or `PARTITIONS` = 16, 32 and 64, against the oracle. (Req 8)
8. **"289M events" is not what the stream processes.** About 79% of rows are dropped in batch prep. If the report quotes 289M as throughput, that is a factual error. If it quotes 59M, it needs to justify why filtering belongs offline. (Req 1)
9. **Subscriber scaling is untested.** Signals are broadcast to every client, history and symbol lists hit one ClickHouse, and there is no load balancer for M gateways. (Req 6, 7)
10. **No multi-VM path.** The advertised broker address, `depends_on`, Prometheus discovery and local Parquet mounts all assume one host. By decision this is out of scope for the evaluation, so the risk is low, provided the report states that containers on one host share memory bandwidth, disk and network, and sketches the multi-VM path from the Details of Req 5 as future work. (Req 5)
11. **Late and out-of-order events are removed by sorting rather than handled**, with zero lateness allowed. The rubric names "handling of late and out-of-order events", mainly in the report and evaluation rows. (Req 8)
12. **Quiet windows close late, and this gets worse with more partitions**, because there are no periodic watermarks. It affects latency in off-hours, and the scaling trade-off is unmeasured. (Req 4, 9)
13. **Single-goroutine signal instance.** About one core per instance is usable; scaling within an instance is unexplored. This is defensible as a design choice, but should be measured. (Req 5)
14. **Rebuild edge cases.** Rebuild cost grows with history, and retention could truncate history silently. Duplicate outputs after a crash reach WebSocket clients. Low likelihood in the evaluated runs. (Req 7, 8)

## Missing experiments

Every metric in the table that has no evidence, or only laptop evidence, with the smallest test that would produce it. Unless noted, run with `COMPOSE_FILE=compose.yaml:compose.perf.yaml` on the benchmark host and commit the output to `bench/results/`.

| Req | Metric with no evidence | Smallest test that produces it |
|---|---|---|
| 1 | Sustained events/s and lag over the whole week | `DAYS="081121 091121 101121 111121 121121" bench/full-speed.sh`, then `clickhouse/check.sh`. Run with the `observe` profile up and export `sum(signal_partition_lag_records)` over time from Prometheus as CSV. |
| 1 | Week at a fixed rate stays on schedule | The same, with `REPLAY_SPEED=300`. Report `max_lag_ms` from the replayer summary and p99 of `signal_tick_latency_seconds`. |
| 2 | Throughput and latency per exchange vs combined | Smallest version needs no code change: after a full run, `SELECT exchange, count() … FROM ticks JOIN instruments … GROUP BY exchange, toStartOfMinute(event_ts)` for rates. For latency, have probe analyze join bars to `instruments` and split percentiles by exchange. A true one-exchange-alone replay needs a prep switch, for example `WHERE symbol LIKE '%.ETR'`, written to a separate `TICKS_DIR`. |
| 3 | Peak capacity on report-grade hardware, with spread | `MODE=sweep SWEEP_WINDOWS=p100 SWEEP_SPEEDS="300 1000 1500 2000 3000" bench/realtime.sh`, run 3 times. Report the median and range. |
| 4 | Load per partition and per instance | Read-only: commit the bucket counts and peak-window max/mean computed in this review (one DuckDB query over `data/ticks`) as a notebook cell. Runtime: add a `partition` label to `signal_ticks_total` (one line), then plot `rate()` per partition and per instance during a sweep step. |
| 4 | Latency for hot vs rare symbols | Post-process an existing probe CSV: split `total` latency by the symbol's weekly tick-count decile. |
| 4 | Effect of partition count on skew and close latency | Re-run prep with `PARTITIONS=16` and `64` (separate `DATA_DIR`), then one sweep step each. |
| 5 | Throughput vs signal instances | `for k in 1 2 3 5; do bench/signal-drain.sh $k; done` on a pre-filled week, layout A (5 signal vCPUs). One table and one plot. |
| 5 | Throughput vs cores on one host | Out of scope: throughput vs VMs (see Decisions). Instead, repeat the K sweep while widening `CPUS_SIGNAL` (for example 1, 3, 5 vCPUs within layout A), so the curve shows both instances and cores. |
| 5 | Next bottleneck after the signal stage | One sweep step at the highest passing speed while varying `REDPANDA_SMP` (1, 2, 4) and `CH_KAFKA_CONSUMERS` (4, 8). |
| 6 | Behaviour as symbol count grows | Duplicate Monday with suffixed symbols (×2, ×4) in a separate `TICKS_DIR`, then `signal-drain.sh` at fixed K. Report throughput and the process RSS of the signal container. |
| 6, 7 | Processing latency with 0, 1 and many subscribers | Run one sweep step (for example p100 at 300×) three times: gateway stopped, 1 probe, and N probes. N probes means adding `--scale probe=N`, or a loop of `probe listen`. Compare p50/p99 of `signal_tick_latency_seconds` and `signal_batch_seconds` across the three. |
| 6 | Gateway capacity vs clients and instances | N probes at 1, 10, 100, 1000 against 1 gateway, then against `--scale gateway=M`. Record `gateway_ws_dropped_total` and gateway CPU. |
| 8 | Results identical across worker counts | `bench/reset-signal.sh`, then `bench/signal-drain.sh K`, then `clickhouse/check.sh`, for K in 1, 4, 32. Repeat once with `docker compose restart signal` mid-drain to force a rebuild. Commit the check output. |
| 8 | Results identical across partition counts | After the `PARTITIONS=16` and `64` runs above, run `check.sh` on each. Also run `EXCEPT` between their `bars_ema FINAL` tables and the 32-partition run (all three should be empty). |
| 8 | Cost of real disorder | Already planned (`plans/PLAN-2026-11-04.md:467-471`). Smallest first step is read-only: a DuckDB query over the raw CSVs in file order counting trade rows earlier than the running max per symbol. |
| 9 | Latency vs parallelism | `MODE=sweep SWEEP_WINDOWS=p100 SIGNAL_INSTANCES="1 2 3" bench/realtime.sh` (layout B, 3 signal vCPUs) |
| 10 | CPU, memory, network and disk per component | Wrap any of the runs above with `docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}'` sampled every second into a CSV next to the result. That covers all four resources without adding a collector. |

## Decisions (deployment)

- **One host.** Scale-out is demonstrated on a single machine by varying signal instances and pinned cores. A second VM may or may not become available; nothing in the plan depends on it. We do not expect graders to require a multi-VM demo.
- **Single Redpanda broker, unless measurements say otherwise.** On one host, extra brokers add no CPU, disk or memory; they only add replication work. Redpanda already scales inside a node through `REDPANDA_SMP`. A multi-broker setup is worth doing only if (a) a sweep shows the broker is the bottleneck and raising `REDPANDA_SMP` does not fix it, or (b) we want to claim fault tolerance. Otherwise the report states replication factor 1 and no fault tolerance as out of scope.

- **Benchmark machine.** One Linux VM on a host with 16 GB RAM and 12 hardware threads (not the MacBook Air used for the earlier results). `compose.perf.yaml` applies. The VM gets **10 vCPUs** and 12 GB: the host keeps 2 threads for itself and the hypervisor, so the vCPUs are not oversubscribed.

### Proposed layout for 10 vCPUs, 12 GB

`CPUS_*` pins a whole service, so scaled signal instances share `CPUS_SIGNAL`. Two layouts, because the drain tests (`signal-drain.sh`) read a pre-filled topic and need no replayer running.

| Service (vCPU ids) | Layout A: drain and scaling tests | Layout B: end-to-end and sweeps |
|---|---|---|
| Redpanda | 0-1 (`REDPANDA_SMP` 1 or 2) | 0-1 |
| ClickHouse | 2-3 | 2-3 |
| Replayer | not running (fill once beforehand) | 4-5 |
| Gateway (+ probe) | 4 | 6 |
| Signal | 5-9 (5 vCPUs: K = 1, 2, 3, 5) | 7-9 (3 vCPUs: K = 1, 2, 3) |

`.env` for layout A: `CPUS_REDPANDA=0-1`, `CPUS_CLICKHOUSE=2-3`, `CPUS_GATEWAY=4`, `CPUS_SIGNAL=5-9`, `CPUS_REPLAYER=5-9` (the replayer only fills the topic before the measured drain). Layout B: `CPUS_REDPANDA=0-1`, `CPUS_CLICKHOUSE=2-3`, `CPUS_REPLAYER=4-5`, `CPUS_GATEWAY=6`, `CPUS_SIGNAL=7-9`.

Memory: `REDPANDA_MEMORY=2G`; ClickHouse takes what is left (about 6 GB); the signal instances, gateway, replayer buffer and Prometheus need about 2–3 GB together. Disk: the ticks topic for the week is several GB, and ClickHouse and Prometheus add more, so give the VM at least 40 GB of disk on SSD.

**Hyperthreading.** The host CPU is an AMD Ryzen 5 5600-series. Every model in that series (5600, 5600X, 5600G, 5600H, 5600U) has 6 identical Zen 3 cores with 2 threads each, so there are no slow efficiency cores to worry about. The 10 vCPUs therefore sit on about 5 physical cores. Two vCPUs that land on sibling threads of one core share it, so expect the signal-instance curve to bend before it reaches 5. The guest cannot see which vCPUs are siblings, so the report should state "6 cores / 12 threads, 10 vCPUs to the VM" next to every scaling curve, and treat the flattening at higher K as an SMT limit rather than a pipeline limit. Before the final runs, record the exact model (`lscpu` in the VM shows it as "Model name") for the report's hardware section.

Caveats:
- `compose.perf.yaml` pins only redpanda, clickhouse, signal, replayer and gateway. The `probe`, `prometheus` and `grafana` containers are unpinned and can run on the signal cores. Either give them a cpuset (a few lines in `compose.perf.yaml`), or leave the `observe` profile off during measured runs and sample `docker stats` instead.
- Pinning inside the VM controls vCPUs, not physical cores. If the host has fewer physical cores than the vCPUs given to the VM, or runs other work, vCPUs share cores, and the scaling curve flattens for reasons outside the pipeline.

## Open questions

None about the deployment. The exact CPU model is still to be recorded from `lscpu` for the report.
