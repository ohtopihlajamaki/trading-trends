# Prompt

```
We have a course project which requires us to build a UI for stock BUY and SELL signals. Our data is in the form of gigantic csv files. We need to be able to locally feed this data using some database system to a browser UI. The professor suggested Kafka, SQL, ClickHouse and many others. Give a detailed rundown of the 10 most appropriate data management software solutions for this task. The solution needs to be performant, as we need to show that throughput scales with hardware.
```

# Claude

I'll check the current state of the main candidates (versions, licensing, recent features) and then write up the rundown.

Your pipeline has four jobs, and no single tool does all of them well:

```
huge CSVs ─► 1. prep/bulk load ─► 2. replay as live stream ─► 3. compute BUY/SELL ─► 4. store & serve ─► gateway (WebSocket/SSE) ─► browser
```

Browsers can't talk to Kafka or most database protocols directly, so you'll almost always need a small gateway service (Go, Rust, Node or FastAPI) that pushes data over WebSockets. There are two exceptions, noted below. The ten tools are ranked by how well they fit a local, performance-focused course project.

## At a glance

| # | Tool | Role in your pipeline | How throughput scales | License | Setup effort |
|---|---|---|---|---|---|
| 1 | ClickHouse | History store, analytics, signals computed at insert | Cores, plus shards across nodes | Apache 2.0 | Low–medium |
| 2 | QuestDB | Purpose-built market-data store | Cores (single node in open source) | Apache 2.0 | Low |
| 3 | Apache Kafka | Replays CSVs as a live tick stream | Partitions × brokers × consumers | Apache 2.0 | Medium |
| 4 | Redpanda | Same job as Kafka, lighter | Thread-per-core, plus brokers | BSL | Low |
| 5 | DuckDB | CSV→Parquet prep, backtesting, in-browser queries | Cores (single process) | MIT | Very low |
| 6 | RisingWave | Signals as continuously updated SQL views | Compute nodes, parallelism | Apache 2.0 | Medium |
| 7 | Apache Flink | Heavy-duty stream processing | Operator parallelism, TaskManagers | Apache 2.0 | High |
| 8 | TimescaleDB (Postgres) | The "SQL" baseline | Mostly vertical | Apache 2 + TSL | Low |
| 9 | Apache Pinot | Many concurrent UI users | Servers and brokers, horizontally | Apache 2.0 | High |
| 10 | Valkey | Hot "latest state" cache and fan-out | I/O threads, cluster shards | BSD | Very low |

## 1. ClickHouse: the best all-round core

ClickHouse is a columnar, vectorized analytics database, and it's the strongest single choice here.

- **Loading CSVs:** it parses CSV in parallel through `INSERT … FROM INFILE`, the `file()` table function, or `clickhouse-local`, which needs no server.
- **Signals:** window functions handle moving averages, crossovers and RSI-style logic. Materialized views can compute signals as rows arrive.
- **Kafka:** the integration is built in. The standard pattern is a Kafka engine table feeding a MergeTree storage table through a materialized view.
- **Scaling:** `max_threads` and `max_insert_threads` give a clean curve as you add cores. Distributed tables over shards give the horizontal story.
- **Free benchmark data:** `system.query_log` records rows/s, memory and duration for every query.
- **Browser:** the HTTP interface can return JSON or Arrow directly.
- **Watch out:** it wants batched inserts (or `async_insert`), not row-by-row writes. It also moves fast, with frequent releases such as 26.5 in May 2026.

## 2. QuestDB: the most finance-native option

QuestDB's SQL dialect is built around market data:

- `SAMPLE BY` turns ticks into OHLC bars in one line.
- `LATEST ON` gives the last tick per symbol.
- `ASOF JOIN` aligns two streams by time.

Recent releases have added features that matter here. Version 9.0 added a TOLERANCE parameter to ASOF JOIN and new materialized view refresh modes. Version 9.4 added three window functions, and trading-specific functions such as OHLC bars and depth charts are in review.

- **Ingestion:** clients using the InfluxDB Line Protocol reach millions of rows/s. CSV import goes through `COPY`. Queries use the Postgres wire protocol.
- **Demo-friendly:** the built-in web console is handy for showing results.
- **Watch out:** the open-source edition is a single instance with no replication; read replicas are an Enterprise feature. Your scaling story would be about cores, not nodes.

## 3. Apache Kafka: the stream backbone

Kafka is probably what your professor expects. It turns static CSVs into a realistic live feed.

- **Replay:** a producer replays rows in timestamp order, optionally at N× speed. Keying messages by ticker keeps each ticker's ticks in order within a partition.
- **Scaling:** this is the textbook horizontal model. Run 1/2/4/8 partitions with matching consumer counts and plot the result.
- **Current state:**
  - ZooKeeper was removed in 4.0 (March 2025), so KRaft is now the only metadata mode.
  - Version 4.2 (February 2026) made Share Groups, which give queue-style processing, production-ready.
  - The producer performance test gained a warmup option that separates startup from steady-state numbers, which is directly useful for your benchmarks.
- **Watch out:** Kafka is a log, not a query engine. You still need a store for history and a gateway for the browser. It also runs on the JVM and needs tuning.

## 4. Redpanda: Kafka without the JVM

- **Compatibility:** it speaks the Kafka API, so the same client code works unchanged.
- **Scaling:** it's written in C++ with a thread-per-core design, so "more cores means more throughput" is a design property. It ships as a single binary with a nice Console UI.
- **Experiment idea:** run identical producer and consumer code against both Kafka and Redpanda on the same machine.
- **License:** the Community Edition is free but licensed under the Business Source License. That makes it source-available rather than OSI open source, which is fine for coursework.

## 5. DuckDB: the CSV crusher

DuckDB is your step zero.

- **Prep:** it reads globs of CSVs in parallel with type detection. One statement converts them to Parquet: `COPY (FROM read_csv('data/*.csv')) TO 'ticks.parquet'`. Every later stage loads Parquet far faster than CSV.
- **Backtesting:** it's also excellent for testing signal logic in plain SQL before you wire up streaming.
- **Scaling:** `SET threads = N` gives a clean single-machine scaling curve.
- **Browser:** DuckDB-WASM runs inside the browser and can query Parquet files over HTTP. That works for a backend-free "historical explorer" view.
- **Watch out:** it isn't a server and doesn't stream.
- **Versions:** the 1.5 line came out in March 2026, 1.4 is the LTS line, and 2.0 is the next planned release.

## 6. RisingWave: signals as SQL that never stops running

RisingWave is a Postgres-compatible streaming database.

- **How it works:** you ingest from Kafka or Redpanda, then write something like `CREATE MATERIALIZED VIEW signals AS …` (for example, a 20/50 moving-average crossover per ticker). The view updates incrementally as ticks arrive.
- **Fit:** it's Apache 2.0 licensed and explicitly targets live dashboards such as stock trading. The latest release is v3.0.1 from the end of June 2026.
- **Push to the UI:** subscriptions let your gateway receive changes instead of polling.
- **Scaling:** it adds compute nodes horizontally.
- **Alternative:** Materialize is the closest competitor, but it uses the Business Source License.

## 7. Apache Flink: the heavyweight stream processor

- **Role:** it computes signals between Kafka and your store, using windows, keyed per-ticker state and event-time watermarks. You can write jobs in Flink SQL, or in PyFlink if Java isn't your thing.
- **Scaling:** the `parallelism` setting is the cleanest scaling knob in this whole list. Add TaskManagers to scale across nodes.
- **Status:** the 2.1 series received its third bug-fix release in June 2026.
- **Watch out:** it has the highest learning and operations cost of the ten. Choose it if your course emphasizes stream-processing theory. Otherwise RisingWave gets you most of the way with plain SQL.

## 8. TimescaleDB (PostgreSQL): the "SQL" option

TimescaleDB is Postgres plus three additions:

- hypertables, which partition data by time automatically;
- continuous aggregates, which keep OHLC bars incrementally refreshed;
- columnar compression.

The `timescaledb-parallel-copy` tool loads CSVs with multiple workers.

- **Company and editions:** the company renamed itself TigerData in June 2025. Of the two editions, Apache 2 and Community, the Community edition is free to self-host and carries most of the newer features.
- **Scaling:** expect lower ceilings than ClickHouse or QuestDB. That's useful for your report: running the same workload on Postgres and on ClickHouse shows *why* the architecture matters.
- **Push:** `LISTEN/NOTIFY` works for low-rate signal events, but not for a tick firehose.

## 9. Apache Pinot: serving many concurrent users

Pinot is a distributed real-time analytics database built for user-facing queries at high request rates. It ingests directly from Kafka.

- **Recent features:** version 1.4 added pauseless consumption for fresher data and ASOF JOIN support in its time-series engine.
- **Scaling:** you scale data servers and query brokers independently. That's a great story if your scaling axis is "N simulated browser clients".
- **Watch out:** it has many moving parts and is overkill otherwise. Apache Druid is the comparable alternative.

## 10. Valkey: hot state and fan-out

Valkey is the BSD-licensed Redis fork.

- **Role:** it keeps the latest price and latest signal per ticker in memory. Pub/Sub or Streams fan updates out to several gateway processes.
- **Scaling:** version 9.1 reported up to 2.1M requests per second on one server using 9 I/O threads. Its benchmark tool now has warmup and duration options, which helps for controlled measurements.
- **Watch out:** it's not a system of record.

## Considered but left out

- **NATS JetStream:** a lighter alternative to Kafka. The server accepts WebSocket connections directly, and its JavaScript client runs in browsers. That means the UI can subscribe to signal topics with no custom gateway. It's worth a look if you'd rather skip Kafka.
- **kdb+/KDB-X:** the industry standard for tick data, and its Community Edition is now free, even for commercial use. However, it caps q at 16GB RAM and 4 secondary threads per process, which undercuts a hardware-scaling demo. The q language also has a steep learning curve.
- **Druid, Materialize, InfluxDB:** each is covered by a better-fitting alternative above.

## For the UI itself: Perspective

Perspective isn't a database, but it's ideal for your frontend. It was built for J.P. Morgan's trading business, uses a C++ engine compiled to WebAssembly, and streams Apache Arrow data. It can run fully in the browser or be served from a Python, Node or Rust WebSocket server. For candlestick charts specifically, TradingView's open-source Lightweight Charts library is the usual choice.

## Recommended stacks

1. **Default, with the strongest scaling story:** DuckDB (CSV→Parquet) → Redpanda or Kafka (partitioned by ticker) → ClickHouse (Kafka engine plus views computing signals) → WebSocket gateway → Perspective or Lightweight Charts.
2. **Simplest single machine:** DuckDB prep → QuestDB → gateway → UI.
3. **Streaming-SQL showcase:** Kafka → RisingWave (signals as views) → gateway, with ClickHouse holding history.

## How to actually demonstrate scaling

- **Remove fake bottlenecks first.** A single Python process parsing CSV will saturate long before Kafka or ClickHouse does. Run one producer per partition, or use the built-in performance tools, and pre-convert to Parquet.
- **Sweep cores deliberately.** Pin containers with `--cpuset-cpus`, then test 1/2/4/8/16 cores. Vary the matching software knobs together: partitions, `max_threads`, Flink parallelism.
- **Measure steady state, not startup.** After a warmup, record rows/s ingested, end-to-end latency at p50 and p99 (from event timestamp to the moment the browser receives it), and query latency under load.
- **Plot measured against ideal linear scaling.** Explain where the curve flattens: Amdahl's law, memory bandwidth, disk or network. Graders like seeing *where* scaling stops and why.
- **Keep the browser out of the throughput number.** No browser can draw millions of ticks per second. Aggregate on the server into OHLC bars and signals, and batch WebSocket messages every 50–100 ms. Prefer Arrow over JSON. Report rows processed by the backend, not points drawn.

If you tell me your CSV columns, file sizes and hardware, I can put together a starter docker-compose and the signal SQL for whichever stack you pick.

Sources:
- [Apache Kafka 4.2.0 release announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/)
- [Kafka 4.2.0 overview (AxonOps)](https://axonops.com/blog/apache-kafka-4-2-0-is-out/)
- [Kafka version history (AxonOps docs)](https://docs.axonops.com/data-platforms/kafka)
- [Redpanda licensing](https://docs.redpanda.com/streaming/25.2/get-started/licensing/overview.md)
- [QuestDB 9.4.0 release](https://questdb.com/blog/questdb-9-4-0-release/)
- [QuestDB 9.0 summary](https://www.plushcap.com/content/questdb/blog/questdb-questdb-90-armed-with-arrays)
- [QuestDB Open Source vs Enterprise](https://questdb.com/pricing)
- [ClickHouse changelog](https://clickhouse.com/docs/whats-new/changelog)
- [ClickHouse Kafka engine guide](https://oneuptime.com/blog/post/2026-03-31-clickhouse-kafka-engine/view)
- [DuckDB release lines](https://endoflife.ai/article-duckdb-eol)
- [RisingWave GitHub](https://github.com/risingwavelabs/risingwave)
- [RisingWave release info (dev.co)](https://dev.co/databases/open-source/risingwave)
- [Materialize alternatives (RisingWave)](https://risingwave.com/blog/materialize-alternatives-2026)
- [Flink 2.1.3 release](https://flink.apache.org/2026/06/14/apache-flink-2.1.3-release-announcement/)
- [TimescaleDB (Wikipedia)](https://en.wikipedia.org/wiki/TimescaleDB)
- [TimescaleDB editions](https://docs.tigerdata.com/about/latest/timescaledb-editions)
- [Apache Pinot 1.4](https://pinot.apache.org/blog/2025/09/30/Announce-Pinot-1-4/)
- [Valkey 9.1 release](https://valkey.io/blog/valkey-9-1-delivers-improvements-in-security-performance-and-more/)
- [Valkey 9.1 coverage (Linuxiac)](https://linuxiac.com/valkey-9-1-in-memory-data-store-released-with-database-level-acls/)
- [NATS 2.2 WebSockets](https://docs.nats.io/whats_new_22)
- [NATS.js client](https://github.com/nats-io/nats.js)
- [KX usage restrictions](https://code.kx.com/licensing/usage-restrictions.html)
- [KX Product Usage Restrictions (Insights docs)](https://code.kx.com/insights/licensing/usage-restrictions.html)
- [Perspective (GitHub fork)](https://github.com/ruthussanketh/perspective)
- [Perspective user guide](https://perspective.finos.org/guide)