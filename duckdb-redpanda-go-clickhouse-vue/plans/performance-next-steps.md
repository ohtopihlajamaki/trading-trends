# Performance: findings and next steps

Status 30.10.2026. Numbers come from a MacBook Air M2 with Docker Desktop, in
`bench/results/*-20261030-1649.*`. Use them for direction only; the report needs Linux runs.

## What we know

1. **The results are correct.** Bars, EMAs (bit for bit) and signals match the ClickHouse reference in
   every run, with nothing lost.
2. **Real-time load is trivial.** The busiest 5 minutes of the week (Wed 17:35) average 435 ticks/s, with a
   peak second of 4,247 ticks/s. Results reach the UI in 58–331 ms.
3. **That latency comes from the design, not from load.**
   - The gateway flushes every 250 ms, which is most of it.
   - A window closes only when its next tick arrives, adding up to ~110 ms in quiet periods.
   - The signal service itself takes 10–16 ms.
4. **Capacity is about 1000× real time.** One signal instance handles the busiest hour at 1000× (~360k
   ticks/s, p95 ~235 ms). At 3000× it falls behind, topping out around 530–560k ticks/s.
5. **The bottleneck is the signal service.** It processes everything on one goroutine per instance.
   - ClickHouse comes next, at ~400k ticks/s with 4 consumers.
   - The replayer can generate at most ~1M ticks/s.
6. **The laptop numbers are noisy.** Identical runs vary by ~15%, the stage split has about ±10–20 ms
   clock error, and fsync is off.

## What the course wants

From the project description (section 4.1, item 5) and the evaluation criteria. The top level of each
criterion asks for:

| Criterion | Exceptional (3) asks for | Where we are |
| --- | --- | --- |
| Functionality | Correct EMA38/EMA100 per window and correct advisories for all subscribed symbols | Done: `check.sh` matches the oracle exactly |
| Performance | **The entire week** processed smoothly, high throughput, low and stable latency | Only Monday has run end to end at full speed; the sweeps cover two 1-hour spans |
| Scalability | All exchanges and event volumes, **peak rates and the long-tail symbol distribution**, room to grow: more symbols, more subscribers, more resources | Peak rate covered (p100). Nothing yet per exchange, on symbol skew, on subscribers, or with more than 1 signal instance |
| Creative explorations | Design trade-offs (partitioning by symbol, state management, late and out-of-order events) **backed by measurements**, and/or a smart visualization | Trade-offs are written up in `plans/`, but none is measured yet |
| Report: evaluation | Correctness, throughput and latency under load, scaling across exchanges and with varying resources, **CPU utilization**, long-tail handling | Correctness, throughput and latency done. CPU utilization not recorded |
| Report: writing | Strong focus on **dataset analysis** | Only the window percentiles from `realtime.sh` |

Two things the rubric does **not** ask for: a comparison with ideal linear scaling, and a before/after
measurement for every change. Both are good ways to present scaling and design trade-offs, but they are
optional. Gaps to close first: the full week, varying resources, CPU utilization, per-exchange and long-tail
results, subscriber scaling, and measured trade-offs.

**Running the same test on more machines doesn't help much:** a faster CPU just gives bigger numbers.
What helps is **one Linux machine with many cores where we vary the resources deliberately**, plus,
optionally, a second machine for a distributed setup (for example broker and ClickHouse on one, signal
instances on the other).

## Next steps

Steps 1–4 are what the rubric needs. Steps 5–7 make the results better and give the trade-off
discussion its measurements.

**1. The whole week on Linux** (Performance)

Run `DAYS="081121 091121 101121 111121 121121 131121 141121" bench/full-speed.sh` on fresh volumes, then `clickhouse/check.sh`. Report total ticks, wall time,
ticks/s, the latency distribution, late ticks and the check results. Then replay the week at a fixed high
speed (for example 300×) to show that latency stays low and stable over all of it, not only while draining
a backlog.

**2. Scaling with resources, plus CPU utilization** (Scalability, Evaluation; about 1 h)

Set `CPUS_*` in `.env` to the machine's cores. All signal instances share `CPUS_SIGNAL`, so give it
enough cores for 8 instances.

```sh
export COMPOSE_FILE=compose.yaml:compose.perf.yaml
MODE=sweep SWEEP_WINDOWS=p100 SWEEP_SPEEDS="300 1000 1500 2000 3000" SIGNAL_INSTANCES="1 2 4 8" bench/realtime.sh
for k in 1 2 4 8; do bench/signal-drain.sh $k; done   # signal service alone, no replayer limit
```

Also vary the cores per instance (`CPUS_SIGNAL`), not only the instance count, since the rubric says
"varying resources (CPU)". Record CPU and memory per container while each run is going, for example
`docker stats --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}'` sampled every second into a CSV next to the
results. The report needs utilization numbers, not only which container saturates. Commit the files in
`bench/results/`.

**3. Exchanges, long tail and peaks** (Scalability, Evaluation, dataset analysis)

Most of this is queries on data we already have, with no new runs:
- **Dataset analysis:** events and price events per exchange (FR/NL/ETR), per security type, per hour of
  the week; ticks per symbol as a rank/frequency plot (the long tail); ticks/s per second at the peaks.
  Use these to explain the two load levels and why the 17:35 window is the busiest.
- **Per exchange:** replay one exchange at a time, then all three (filter in the replayer or in prep), and
  compare throughput and latency. This shows directly that the system scales "across all three exchanges".
- **Long tail and partitioning by symbol:** ticks per partition across the 32 partitions, and per signal
  instance. Do the hottest symbols make one partition, and so one instance, the bottleneck? If they do,
  compare more partitions or a different hash.
- Peak rates are already covered by p100. Also report the highest per-second rate inside it, and the
  latency during that second.

**4. More subscribers and more symbols** (Scalability: "capacity for further increases")
- Run 1, 10, 100 and 1000 probe clients, each subscribed to its own watchlist and some to every symbol.
  Measure latency to the clients and the gateway's CPU, then scale gateway instances (`--scale gateway=M`
  with `compose.perf.yaml`).
- Optional: simulate more symbols by duplicating the week with renamed symbols (×2, ×4), to show how state
  and throughput grow with the number of symbols.

**5. Parallelise the signal service** (Creative explorations, with measurements)
- Give each partition its own goroutine.
- Stop blocking on the output produce and the commit.

Rerun step 2, and compare one instance with N goroutines against N instances. That is a measured trade-off
between scaling inside one process and scaling across processes.

**6. Fix the next bottleneck**, re-measuring after each change:
- ClickHouse: raise `CH_KAFKA_CONSUMERS`.
- Redpanda: raise `REDPANDA_SMP`.
- The load generator, where it limits the test: run several replayers, or use `signal-drain.sh`.

**7. Latency costs of the design and late events** (Creative explorations, Performance), measured with
`MODE=realtime bench/realtime.sh`:
- Have the replayer write periodic watermarks, so windows close on time. That is the trade-off between
  closing windows promptly and dropping late ticks. Report late ticks against watermark lag, or allowed
  lateness, on the real data. The data set says there is no total order per symbol across exchanges.
- Make the gateway flush interval configurable and try ~50 ms.

**8. Report: performance evaluation section**
- Correctness: `check.sh` against the oracle over the whole week, plus late ticks and decode errors.
- The whole week: wall time, throughput and latency distribution (step 1).
- Throughput against instances and cores, with CPU utilization per container. Adding ideal linear scaling
  to the plot is optional.
- Latency against offered load for each configuration.
- Per exchange, long tail and partition skew (step 3); subscribers (step 4).
- The design trade-offs with their measurements: steps 5 and 7, including the latency breakdown before and
  after.

## Running the test

See the "Performance test" section of the README. The test needs Docker, bash, perl and prepped data. It
uses its own Compose project, so stop the normal stack first. `MODE=realtime REALTIME_SPEED=20` gives a
5-minute smoke test.
