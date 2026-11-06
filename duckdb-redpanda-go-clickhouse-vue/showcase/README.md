# Showcase: data on its way through the pipeline

Each step runs one or two nodes of the pipeline on a few lines of real data, and saves what arrives and what leaves. Together the six steps cover all 13 edges of the graph in [`plans/PLAN-2026-11-04.md`](../plans/PLAN-2026-11-04.md) (Architecture → Edges).

All steps follow one symbol, A1PHBB.ETR, on Monday 8 November 2021. The goal is the day's only signal for that symbol: a SELL in the 11:40 window, from 12 lines of the raw CSV to a marker on the chart in the browser.

| Step | Graph edges | Node | In | Out |
| --- | --- | --- | --- | --- |
| [01-raw-to-prep](01-raw-to-prep/) | 1, 2 | prep (DuckDB) | 12 raw CSV lines | 7 trades in one Parquet bucket (and 31 empty buckets), plus the same rows as CSV |
| [02-replayer-to-topic](02-replayer-to-topic/) | 4, 5 | replayer (Go) | step 01's 32 bucket files | 39 records in a demo `ticks` topic: the 7 ticks and a watermark in partition 27, a watermark in each other partition |
| [03-signal-to-bars-and-signals](03-signal-to-bars-and-signals/) | 6, 7 | signal (Go) | step 02's topic, 39 records | 4 bars in a demo `bars_ema` topic and the SELL in a demo `signals` topic, all in partition 27 |
| [04-topics-to-clickhouse](04-topics-to-clickhouse/) | 8, 3, 9 | ClickHouse; prep | the three demo topics; 6 lines of Sunday's CSV; step 01's buckets | database `showcase`: the ticks, watermarks, bars, the SELL and A1PHBB.ETR's `instruments` row; database `showcase_backfill`: the ticks again, from the files |
| [05-gateway](05-gateway/) | 11, 10 | gateway (Go) | database `showcase`; step 03's records, produced again | the `/symbols`, `/history` and `/signals` answers, and the WebSocket frames with the 4 bars and the SELL |
| [06-ui-and-browser](06-ui-and-browser/) | 12, 13 | Vite (ui); Chromium | the gateway, as in step 05 | the same answers through the Vite proxy, the browser's WebSocket messages, and screenshots of the chart with the SELL |

Run the steps in order: each reads what the steps before it left. A full pass takes a few minutes, mostly for builds, `npm ci` and the browser download in step 06.

Each step directory has the same parts:

- `input/` — data cut from the raw files, where a step needs some (steps 01 and 04). Otherwise a step reads the previous steps' `output/`, or the demo topics and database they left.
- `output/` — what the node sends on, plus anything worth seeing inside the node. Each run rewrites it. Git ignores it, so run the step (and the steps before it) to see it; the step's README quotes what it holds.
- `run.sh` — runs the step in the node's own container, built from the checkout. Only Docker and a `.env` are needed, and network access for npm in step 06. A step that needs Redpanda or ClickHouse:
  - starts it, and stops it again at the end if it wasn't running
  - uses its own `showcase-*` topics and consumer groups, and the databases `showcase` and `showcase_backfill`, so the pipeline's own topics, groups and `default` database stay untouched
  - names the containers it starts itself `showcase-*`
- `README.md` — what changed across the edge, and how the snippet differs from a full run.

To remove everything the steps leave in Redpanda and ClickHouse:

```sh
docker compose up -d --wait redpanda clickhouse
docker compose exec redpanda rpk topic delete showcase-ticks showcase-bars_ema showcase-signals \
  showcase-live-bars_ema showcase-live-signals
docker compose exec redpanda rpk group delete showcase-signal showcase-ch-ticks showcase-ch-bars-ema showcase-ch-signals
docker compose exec clickhouse sh -c 'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "DROP DATABASE IF EXISTS showcase" -q "DROP DATABASE IF EXISTS showcase_backfill"'
```

The raw inputs keep their exact bytes, including the CRLF line endings of the preamble. [`.gitattributes`](.gitattributes) stops git from normalizing them.

The data is the DEBS 2022 Grand Challenge Data Set Trading Data, © Infront Financial Technology GmbH, licensed under CC BY-NC-SA 4.0. Each raw input keeps the file's preamble, which carries this notice.
