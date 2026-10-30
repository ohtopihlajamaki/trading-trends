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

## What the course wants (check the assignment brief)

Throughput that grows with resources, compared with ideal linear scaling. Where scaling stops and why.
Latency against load. Each design change backed by a before/after measurement. So far we have only one
point on the scaling curve (1 signal instance) and no improvements yet.

**Running the same test on more machines doesn't help much:** a faster CPU just gives bigger numbers.
What helps is **one Linux machine with many cores where we vary the resources deliberately**, plus,
optionally, a second machine for a distributed setup (for example broker and ClickHouse on one, signal
instances on the other).

## Next steps

**1. Baseline scaling on Linux** (about 1 h)

Set `CPUS_*` in `.env` to the machine's cores. All signal instances share `CPUS_SIGNAL`, so give it
enough cores for 8 instances.

```sh
export COMPOSE_FILE=compose.yaml:compose.perf.yaml
MODE=sweep SWEEP_WINDOWS=p100 SWEEP_SPEEDS="300 1000 1500 2000 3000" SIGNAL_INSTANCES="1 2 4 8" bench/realtime.sh
for k in 1 2 4 8; do bench/signal-drain.sh $k; done   # signal service alone, no replayer limit
```

Run `docker stats` alongside to see which container saturates. Commit the files in `bench/results/`.

**2. Parallelise the signal service.**
- Give each partition its own goroutine.
- Stop blocking on the output produce and the commit.

Rerun step 1, and compare one instance with N goroutines against N instances.

**3. Fix the next bottleneck**, re-measuring after each change:
- ClickHouse: raise `CH_KAFKA_CONSUMERS`.
- Redpanda: raise `REDPANDA_SMP`.
- The load generator, where it limits the test: run several replayers, or use `signal-drain.sh`.

**4. Cut the latency design costs**, measured with `MODE=realtime bench/realtime.sh`:
- Have the replayer write periodic watermarks, so windows close on time.
- Make the gateway flush interval configurable and try ~50 ms.

**5. Report**
- Throughput against instances or cores, next to ideal linear scaling.
- Latency against offered load for each configuration.
- The latency breakdown before and after step 4.
- Correctness.

## Running the test

See the "Performance test" section of the README. The test needs Docker, bash, perl and prepped data. It
uses its own Compose project, so stop the normal stack first. `MODE=realtime REALTIME_SPEED=20` gives a
5-minute smoke test.
