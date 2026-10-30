# Status and next steps

Status 30.10.2026, checked against the project description and the evaluation criteria (26 points:
implementation 12, report 12, teamwork 2). The numbers come from a MacBook Air M2 with Docker Desktop, in
`bench/results/*-20261030-*`. Use them for direction only; the report needs Linux runs with
`compose.perf.yaml`.

## Where we stand

| Criterion (max 3) | Exceptional asks for | Have | Missing |
| --- | --- | --- | --- |
| Functionality | Correct EMA38/EMA100 per window, correct advisories for all subscribed symbols | Bars, EMAs (bit for bit) and signals match `oracle.sql` on Monday, also when scaling 1→4 instances mid-drain | **EMA start and crossover rule differ from the spec** (below). No oracle check over the whole week |
| Performance | The entire week processed smoothly; high throughput; low, stable latency | Monday at full speed (~450k ticks/s through the signal service); real-time windows from every weekday; sweeps of two 1-hour spans | The whole week in one run. Latency stability over all of it |
| Scalability | All exchanges and volumes, peak rates, the long tail, room for more symbols, subscribers and resources | Peak window (p100) at up to 1000×; consumer-group scaling works | Scaling curves (instances, cores). Results per exchange. Partition skew from hot symbols. Subscriber scaling |
| Creative explorations | Trade-offs backed by measurements, and/or a smart visualization | Smart visualization: candles with EMAs and signal markers, live signal grid, throughput. Variants A–F written up in `stock-signals-implementation-plan.md` | Measurements behind the trade-offs, above all **late and out-of-order events** (below) |
| Report: design, implementation | Real-time, complex event processing (CEP), scalability; tick, housekeeping and metadata events; windowing, EMA state, crossovers, normalization | Material in `PLAN.md` and the plans | The report itself (ACM template, 4–5 pages). How housekeeping rows are dropped and `instruments` is used |
| Report: evaluation | Correctness, throughput and latency under load, scaling across exchanges and resources, CPU utilization, long tail | Correctness on Monday; latency against load for 1 instance | CPU utilization, scaling, per exchange, long tail, out-of-order handling |
| Report: writing | Clear, with a strong focus on dataset analysis | `notebooks/dataset_overview.ipynb`: hours, events per ID (long tail), activity over time | Per exchange and per security type; trade events versus all events; the peaks; how disordered the raw stream is |
| Teamwork (2) | Each member answers the three questions; clear roles | Roles in the implementation plan | The answers in the report |

## Spec deviation 1: EMA start

**Spec (section 3.2):** the EMA of the window before a symbol's first is 0, so the first window gives
EMA38 = 2/39·c ≈ 0.051·c and EMA100 = 2/101·c ≈ 0.020·c, where c is the window's close.

**Ours:** `EMA_INIT=first_close` is the default in `signal/main.go` and `.env`, and it sets both EMAs to c.
The service also accepts `zero`, but `oracle.sql` only implements first-close seeding, so `check.sh`
cannot verify zero mode.

**It isn't a small difference.** The gap between the two shrinks by a factor (1 − α) per traded window. For
EMA100 that factor is 0.98, so after 100 windows (about 8 trading hours) the gap is still 14% of the first
price, and 2% at the end of a full day (187 windows). EMA38 converges within a few hours. So Monday's
EMA100 values, and the signals that depend on them, differ from the spec's for most of the day. The top
Functionality level asks for "correct EMA38 and EMA100 per window", so the graded runs need `zero`.

## Spec deviation 2: crossover rule

**Spec (equations 1–4):** a signal at window wᵢ depends only on wᵢ and wᵢ₋₁. BUY if EMA38 > EMA100 in wᵢ
and EMA38 ≤ EMA100 in wᵢ₋₁; SELL the other way round.

**Ours (the flip rule):** BUY if sign(EMA38 − EMA100) is +1 now and the last *non-zero* sign was −1. Ties
(sign 0) are skipped and don't reset the comparison.

The two rules differ only when EMA38 = EMA100 exactly:

| sign(EMA38 − EMA100) over windows | Spec | Flip |
| --- | --- | --- |
| Zero start, first window: previous (0, 0), now + | BUY | nothing (sets the baseline) |
| First-close start: 0, then + | BUY in the 2nd window | nothing |
| −, 0, + | BUY | BUY |
| +, 0, + | BUY | nothing |
| −, 0, − | SELL | nothing |

Ties are common with first-close seeding: both EMAs start equal and stay equal, up to rounding, while the
price is flat. That is why we chose the flip rule, and where Monday's 229 rounding-noise signals come from.
With the spec's zero start the EMAs begin apart, so exact ties should be rare. The main remaining effect
should then be one BUY per symbol in its first window, which is what the spec literally defines. Both
claims need confirming by measurement.

**Action points (both deviations):**
1. Signal service: add `CROSSOVER_RULE=spec|flip`. For `spec`, keep the previous window's EMA38 and EMA100 in
   the symbol's state ((0, 0) before the first window under `zero`) and compare them directly, with no sign
   memory.
2. Oracle: add zero seeding (start the fold from (0, 0) over every close) and the spec rule
   (`lagInFrame` of both EMAs, default 0), chosen by the same two settings. Make `check.sh` pass them
   through, so it verifies whichever mode the service runs in.
3. Unit tests in `signal/state_test.go` for every row of the table above, plus a first-window EMA value
   under `zero`.
4. Defaults: `EMA_INIT=zero`, `CROSSOVER_RULE=spec` in `.env.example`, `compose.yaml` and the service.
   Update the signal rules and decisions in `PLAN.md`.
5. Run `check.sh` on the week in both modes. Report the signal counts, how many differ and why (first-window
   BUYs, ties). In the report, present first-close + flip as a variant we measured, not the system's
   answer.
6. UI: under `zero` the EMAs start near 0. Check that the price chart's axis doesn't flatten the first
   day; if it does, scale the axis to the price.

## Late and out-of-order events

File order is receipt order, not event order. The spec warns that the total order of events per symbol
cannot be assumed, and the rubric names "handling of late and out-of-order events". Today prep sorts each
bucket by event time, so the service never sees disorder. Its watermark (the highest event time seen in
the partition) has no slack, and a window also closes as soon as its symbol's next tick lands in a later
window. Fed the raw order, it would drop every tick that arrives after its window closed, and we haven't
measured how many that would be.

**Action points:**
1. **Measure the disorder** in the raw CSVs (DuckDB, per day), in file order. Find the share of trade rows
   whose event time is earlier than the latest event time before them, per symbol and per bucket. Then
   get the lateness distribution (p50, p99, max) per exchange. This also goes into the dataset analysis.
2. **Receipt-order mode:** `PREP_ORDER=receipt` writes each bucket in file order (`seq`) instead of
   `(event_ts, seq)`. The oracle stays on ClickHouse's event-time order, so `check.sh` shows exactly what
   disorder costs.
3. **Bounded out-of-orderness in the service:** watermark = highest event time − `ALLOWED_LATENESS`. Windows
   close only when the watermark passes their end; ticks later than that are counted as late and
   dropped. The replayer's periodic watermarks (step 6) must respect the same bound.
4. **Measure the trade-off:** for `ALLOWED_LATENESS` = 0, 1 s, 5 s, 30 s and more, report late ticks,
   bars/EMAs/signals that differ from the oracle, and window-end → UI latency. Pick the default from that.
   If the disorder is small, say that sorting in prep is the same as a lateness bound with no loss. The
   numbers then justify it.

## What the measurements show so far

1. **Real-time load is trivial.** The busiest 5 minutes (Wed 17:35) average 435 ticks/s, with a peak second of
   4,247. Results reach the UI 58–331 ms after the window ends.
2. **That latency comes from the design, not from load.** Most of it is the gateway's 250 ms flush. Waiting
   for the tick that closes the window adds up to ~110 ms in quiet periods. The signal service takes 10–16 ms.
3. **Capacity is about 1000× real time** for one signal instance (~360k ticks/s, p95 ~235 ms). At 3000× it
   falls behind, topping out around 530–560k ticks/s.
4. **The signal service is the bottleneck.** It runs one goroutine per instance. ClickHouse comes next, at
   ~400k ticks/s with 4 Kafka consumers. The replayer tops out at ~1M ticks/s.
5. **The laptop numbers are noisy:** ~15% between identical runs, ±10–20 ms clock error in the stage split,
   and fsync off.

Running the same test on more laptops adds little; a faster CPU just gives bigger numbers. What we need is
**one Linux machine with many cores, where we vary the resources deliberately**, plus optionally a second
machine for a distributed setup.

## Next steps

In priority order. Steps 1–2 change the results, so do them before the final runs; steps 3–6 are those
runs and the trade-offs.

**1. EMA start and crossover rule:** the action points above.

**2. Late and out-of-order events:** the action points above. Do the measurement first, since it decides
how much of the rest is needed.

**3. The whole week on Linux** (Performance, Functionality)

```sh
export COMPOSE_FILE=compose.yaml:compose.perf.yaml
DAYS="081121 091121 101121 111121 121121" bench/full-speed.sh   # fresh volumes
clickhouse/check.sh
```

Report ticks, wall time, ticks/s, latency distribution, late ticks and the check result. Then replay the
week at a fixed speed (for example 300×) to show that latency stays low and stable over all of it.

**4. Scaling with resources, with CPU utilization** (Scalability, Evaluation; about 1 h)

```sh
MODE=sweep SWEEP_WINDOWS=p100 SWEEP_SPEEDS="300 1000 1500 2000 3000" SIGNAL_INSTANCES="1 2 4 8" bench/realtime.sh
for k in 1 2 4 8; do bench/signal-drain.sh $k; done   # signal service alone, no replayer limit
```

Set `CPUS_*` in `.env`, and vary `CPUS_SIGNAL` as well as the instance count. Sample
`docker stats --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}'` every second into a CSV next to each result.
The report needs utilization, not just which container saturates. Plot throughput against instances and
cores; adding ideal linear scaling is optional. Commit everything in `bench/results/`.

**5. Exchanges, long tail, peaks and subscribers** (Scalability, Evaluation, dataset analysis)
- Extend the notebook: events and trade events per exchange (FR/NL/ETR) and per security type; why the
  17:35 window is the busiest; the highest per-second rate.
- Replay one exchange at a time, then all three, and compare throughput and latency.
- Ticks per partition and per signal instance. If the hottest symbols overload one partition, compare more
  partitions or a greedy symbol-to-bucket map. This measures the "partitioning by symbol" trade-off.
- Run 1, 10, 100 and 1000 probe clients with their own watchlists; record latency and gateway CPU; then
  `--scale gateway=M`. Optional: duplicate the week with renamed symbols (×2, ×4).

**6. Further measured trade-offs** (Creative explorations), re-measuring after each change:
- **Parallelism:** one goroutine per partition, and no blocking on produce and commit. Compare one instance
  with N goroutines against N instances.
- **Periodic watermarks** from the replayer, so quiet windows close on time.
- **Gateway flush:** make it configurable, and try ~50 ms against 250 ms.
- **Next bottlenecks:** `CH_KAFKA_CONSUMERS`, `REDPANDA_SMP`, several replayers.

**7. Report** (ACM template, 4–5 pages):
- Architecture and the reasons for it.
- Implementation: tick, housekeeping and metadata handling, normalization, windows and EMA state,
  crossovers, out-of-order handling, hardware and tools.
- Evaluation: steps 2–6.
- Dataset analysis.
- The visualization, with screenshots.
- The rule variants (step 1).
- Each member's answers to the three teamwork questions.

Also state in the report that windows without trades are skipped (the spec doesn't define them), and that
every client receives every signal while bars go only to subscribed symbols (a superset of the spec's
"updates for a subset of symbols").

## Running the tests

See "Performance test" in the README. It needs Docker, bash, perl and prepped data, and uses its own
Compose project, so stop the normal stack first. `MODE=realtime REALTIME_SPEED=20` is a 5-minute smoke test.
