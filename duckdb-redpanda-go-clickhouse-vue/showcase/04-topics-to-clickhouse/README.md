# 04 · Topics, prep and Parquet → ClickHouse

This step covers the three edges that end in ClickHouse:

| Edge | From | Through | Into |
| --- | --- | --- | --- |
| 8 | the demo topics of steps 02 and 03 | Kafka engine tables and materialized views (`clickhouse/init/20-kafka.sh`) | database `showcase`: `ticks`, `ticks_watermarks`, `bars_5m`, `bars_ema`, `signals`, `kafka_errors` |
| 3 | A1PHBB.ETR's lines of Sunday's file | prep's `build_instruments` and `load_instruments`, HTTP POST | `showcase.instruments` |
| 9 | step 01's Parquet buckets | `clickhouse/backfill.sql` | database `showcase_backfill`: `ticks`, and `bars_5m` from it |

Both databases get the project's own schema (`clickhouse/init/10-schema.sql`). The pipeline's `default` database, topics and consumer groups are untouched.

## Run

```sh
./run.sh        # Docker and a .env are enough
```

Run steps [01](../01-raw-to-prep/)–[03](../03-signal-to-bars-and-signals/) first. The script starts Redpanda and ClickHouse if they aren't running, and stops them again at the end. It rewrites `output/` and ends with:

```text
instruments from INSTRUMENT_DAYS=141121 in input/, into database showcase
instruments: 1 symbols from 141121 -> /showcase/04-topics-to-clickhouse/output/instruments/instruments.parquet
instruments: loaded 1 rows into ClickHouse
table                showcase  showcase_backfill
instruments                 1                  0
ticks                       7                  7
ticks_watermarks           32                  0
bars_5m                     4                  4
bars_ema                    4                  0
signals                     1                  0
kafka_errors                0                  0
oracle                      1                  -
ticks from the topic and from the backfill: identical; bars_5m: identical
```

`showcase` and `showcase_backfill` stay in ClickHouse for steps 05 and 06. To remove them:

```sh
docker compose exec clickhouse sh -c 'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "DROP DATABASE showcase" -q "DROP DATABASE showcase_backfill"'
docker compose exec redpanda rpk group delete showcase-ch-ticks showcase-ch-bars-ema showcase-ch-signals
```

## Edge 8: topics → Kafka engine tables → tables

**In.** The topics that steps 02 and 03 left, checked against what those steps saved:
- `showcase-ticks`: 7 ticks and 32 watermarks.
- `showcase-bars_ema`: 4 bars.
- `showcase-signals`: 1 SELL.

**How the step sets it up:**
- **The schema.** `10-schema.sql` runs unchanged in a fresh `showcase` database.
- **The Kafka tables.** They come from `20-kafka.sh` itself. The script runs it with a stand-in `clickhouse` command that prints the SQL instead of executing it, then swaps in the demo names:
  - `kafka_topic_list`: `ticks` → `showcase-ticks`, and likewise for `bars_ema` and `signals`.
  - `kafka_group_name`: `ch-ticks` → `showcase-ch-ticks`, and likewise for the other two groups.

  The result is saved as `output/kafka.sql`. For the ticks:

  ```sql
  CREATE TABLE ticks_queue (symbol String, sectype String, last String, ts_ms Int64, seq UInt64, sent_ms Int64)
  ENGINE = Kafka
  SETTINGS kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'showcase-ticks',
           kafka_group_name = 'showcase-ch-ticks', kafka_format = 'JSONEachRow',
           kafka_num_consumers = 4, kafka_thread_per_consumer = 1,
           kafka_handle_error_mode = 'stream',
           input_format_json_read_numbers_as_strings = 1, precise_float_parsing = 1,
           input_format_skip_unknown_fields = 1;

  -- A record with an empty symbol is a watermark from the replayer, not a tick
  CREATE MATERIALIZED VIEW ticks_queue_mv TO ticks AS
  SELECT symbol, sectype, toFloat64(last) AS last,
         fromUnixTimestamp64Milli(ts_ms, 'Europe/Berlin') AS event_ts, seq
  FROM ticks_queue WHERE length(_error) = 0 AND symbol != ''
  SETTINGS precise_float_parsing = 1;
  ```

- **The consumer groups** are deleted first, so they start with no commits and read each topic from the start.
- **Waiting and detaching.** The script waits until all records have arrived; ClickHouse writes what its consumers read every few seconds. It then drops the Kafka tables and views (the same `DROP`s `20-kafka.sh` starts with), so nothing in `showcase` keeps reading from Redpanda. The data tables stay.

**What changes across the edge.**

| Record field | Column | Change |
| --- | --- | --- |
| `ticks` `symbol`, `sectype`, `seq` | `ticks` columns of the same names | none |
| `ticks` `last` (read as text) | `last Float64` | `toFloat64` under `precise_float_parsing`, so `21.2` is exactly Go's 21.2 |
| `ticks` `ts_ms` | `event_ts DateTime64(3, 'Europe/Berlin')` | `1636358660825` → `2021-11-08 09:04:20.825` |
| `ticks` `sent_ms` | — | dropped (`bench/realtime.sh` reads it from the topic) |
| a watermark (empty symbol) | `ticks_watermarks` | partition, offset and `ts_ms`; it never reaches `ticks` |
| `bars_ema` `window_start_ms` | `window_start DateTime` | `1636368000000` → `2021-11-08 11:40:00` |
| `bars_ema` `trigger_sent_ms`, `emitted_ms` | — | dropped: `input_format_skip_unknown_fields` |
| `signals` `emitted_ms` | `emitted_at DateTime64(3)` | kept, as Berlin time |

**Out.** These are in `output/tables/showcase.*.csv`. `ticks` (`bars_5m` is filled from it by `bars_5m_mv`):

```text
"symbol","sectype","last","event_ts","seq"
"A1PHBB.ETR","E",21,"2021-11-08 09:04:20.825",2
"A1PHBB.ETR","E",21,"2021-11-08 09:04:20.825",3
"A1PHBB.ETR","E",21,"2021-11-08 09:04:20.825",4
"A1PHBB.ETR","E",21.2,"2021-11-08 09:05:02.509",5
"A1PHBB.ETR","E",21.2,"2021-11-08 10:38:48.573",6
"A1PHBB.ETR","E",21,"2021-11-08 10:38:48.573",7
"A1PHBB.ETR","E",20.6,"2021-11-08 11:43:17.871",8
```

`ticks_watermarks`, one row per partition:

```text
"partition","offset","watermark"
0,0,"2021-11-09 00:00:00.000"
…
27,7,"2021-11-09 00:00:00.000"
…
```

`bars_ema` and `signals`:

```text
"symbol","window_start","open","high","low","close","trades","ema38","ema100"
"A1PHBB.ETR","2021-11-08 09:00:00",21,21,21,21,3,21,21
"A1PHBB.ETR","2021-11-08 09:05:00",21.2,21.2,21.2,21.2,1,21.01025641025641,21.003960396039606
"A1PHBB.ETR","2021-11-08 10:35:00",21.2,21.2,21,21,2,21.00973044049967,21.00388197235565
"A1PHBB.ETR","2021-11-08 11:40:00",20.6,20.6,20.6,20.6,1,20.988718623038146,20.995884309536724

"symbol","window_start","side","close","ema38","ema100","emitted_at"
"A1PHBB.ETR","2021-11-08 11:40:00","SELL",20.6,20.988718623038146,20.995884309536724,"2026-11-05 17:33:13.647"
```

- **`emitted_at`** is step 03's `emitted_ms` (`1793896393647`, 16:33:13.647 UTC), shown in Berlin time.
- **`bars_5m`** holds the same four windows, with the same open, high, low, close and trades. ClickHouse built it from `ticks` alone, without the signal service.
- **The oracle.** `clickhouse/oracle.sql` computes the signals from `bars_5m` and finds the same SELL (`showcase.oracle.csv`). This is the check that `clickhouse/check.sh` makes over a whole day.
- **`kafka_errors`** is empty: every record parsed.
- **Offsets.** `output/group-offsets.txt` holds the ClickHouse groups' committed offsets:
  - `showcase-ch-ticks` has 8 in partition 27 and 1 in every other partition.
  - `showcase-ch-bars-ema` has 4 and `showcase-ch-signals` has 1, both in partition 27.
  - All lags are 0.

## Edge 3: prep → `instruments`

**In: `input/141121.csv`.** The 13-line preamble of `data/raw/141121.csv` (Sunday), plus A1PHBB.ETR's 6 lines from it, byte for byte. Only one of the 6 has data: Currency (col 11) and ISIN (col 14), and that line has no Time.

```text
A1PHBB.ETR,E,14-11-2021,,,,,,,,,EUR,,,DE000A1PHBB5,,,,,,,,,,,,,,,,,,,,,,,,,
```

Prep reads only the days in `INSTRUMENT_DAYS` (141121 by default) for this table, not `DAYS`.

**Inside prep.** [`instruments_snippet.py`](instruments_snippet.py) runs in the prep container. It calls prep's own `build_instruments()`, which calls `load_instruments()`:
- **The row.** `build_instruments` makes one row per symbol: the exchange is the ID's suffix, and `max()` over the day picks the type, ISIN and currency from whichever lines have them.
- **The file.** It writes the rows to `instruments.parquet`, which is saved here in `output/instruments/`.
- **What the snippet adds.** A wrapper around `urlopen` that saves the request and adds `X-ClickHouse-Database: showcase`. `load_instruments` builds its own URL, so a header is the way to name the database. Nothing in prep changes.

**Out: the request** (`output/instruments/request.txt`; urllib adds `Content-Type` and `Content-Length` when it sends it):

```text
POST /?query=INSERT INTO instruments FORMAT JSONEachRow
Host: clickhouse:8123
Authorization: (user:password, base64)
X-clickhouse-database: showcase

{"symbol": "A1PHBB.ETR", "exchange": "ETR", "sectype": "E", "isin": "DE000A1PHBB5", "currency": "EUR"}
```

And in `showcase.instruments`:

```text
"symbol","exchange","sectype","isin","currency"
"A1PHBB.ETR","ETR","E","DE000A1PHBB5","EUR"
```

In a full run the body has one line per symbol: 5,495 from Sunday's file.

## Edge 9: Parquet buckets → `backfill.sql`

**In.** Step 01's 32 bucket files. `backfill.sql` reads `data/ticks/bucket=*/*.parquet` under ClickHouse's `user_files`, where compose mounts `DATA_DIR`. The script copies step 01's buckets to `user_files/showcase/ticks/`, and changes only that path in the SQL (saved as `output/backfill.sql`):

```sql
INSERT INTO ticks (symbol, sectype, last, event_ts, seq)
SELECT symbol, sectype, last, toDateTime64(event_ts, 3, 'Europe/Berlin'), toUInt64(seq)
FROM file('showcase/ticks/bucket=*/*.parquet', Parquet,
          'symbol String, sectype String, last Float64, event_ts DateTime64(6, \'UTC\'), seq Int64');
```

It runs in a separate database, `showcase_backfill`, with the same schema. Run into `showcase`, it would double the ticks that came over Kafka. The copied files are removed at the end.

**Out.** `showcase_backfill.ticks` is identical to `showcase.ticks`, row for row and byte for byte in the CSVs. So is `bars_5m`, which `bars_5m_mv` builds from it. Everything else is empty:
- No watermarks, because the files have none.
- No `bars_ema` and no `signals`, because those only come from the signal service.
- No `instruments`, because prep loads that table.

That is the backfill gap in the plan: a ClickHouse filled only by backfill passes `oracle.sql`, but the gateway reads `bars_ema`, `signals` and `instruments`, so it serves nothing.

## Snippet against the full run

**The same.** `showcase` is built the way `default` is, with the same SQL. The local ClickHouse's `default` database holds the full Monday replay of 18 October 2026. Compared with it, A1PHBB.ETR's rows up to 11:40 match:
- **`bars_ema`, `signals` and `instruments`:** exactly, EMAs included, bit for bit.
- **`ticks`:** every column except `seq`, which step 01 numbers from 1.

**Different:**
- **The Kafka tables are detached here.** In the pipeline they stay, and keep reading.
- **The group names** carry a `showcase-` prefix.
- **Volume.** `default` holds:
  - 11,794,520 rows in `ticks` and 32 in `ticks_watermarks`
  - 373,126 in `bars_ema` and 5,735 in `signals`
  - 5,495 in `instruments`
