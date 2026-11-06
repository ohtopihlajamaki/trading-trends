# 01 · Raw CSV → prep → Parquet bucket

This step covers edges 1 and 2 of the graph: `data/raw → prep → data/ticks`. Prep reads a day's CSV with DuckDB, keeps the trades, and writes them into 32 Parquet buckets sorted by event time. Here, prep's own SQL (`prep/prep_day.sql`, loaded through `prep.py`) runs on 12 lines of Monday's file.

## Run

```sh
./run.sh        # Docker and a .env are enough; ClickHouse is not started
```

It rewrites `output/` and prints:

```text
081121: input/081121.csv, duckdb 1.5.6, 32 buckets
  preamble ok: 11 of 13 lines CRLF, data rows LF; header 39 names, data rows 40 fields
  12 data lines -> 8 rows in day (with Last and Trading time), 1 of them rejected (no event time, or Last <= 0)
  bucket 27: 7 rows -> output/ticks/bucket=27/081121.parquet (+ .csv)
  31 other buckets: empty files, as prep.py writes them
```

## In: `input/081121.csv`

The file holds the 13-line preamble of `data/raw/081121.csv` plus 12 of its 117 lines for A1PHBB.ETR, copied byte for byte. The 12 lines are:

- the zero-price snapshot
- the trades behind the symbol's first four 5-minute bars
- the quote updates from the same milliseconds

The feed writes a line whenever any field changes, in the order the lines arrive. Prep reads only five of the 40 columns: 0 ID, 1 SecType, 21 Last, 23 Trading time and 26 Trading date. Trading date is empty on all 12 lines.

| Line in 081121.csv | Last (col 21) | Trading time (col 23) | What it is | What prep does |
| ---: | --- | --- | --- | --- |
| 31243 | 0.000000 | 00:00:00.000 | snapshot before the first trade | keeps it in `day`, then rejects it (`last > 0`) |
| 1433723 | – | 09:04:20.825 | quote: ask 21.2, bid 21.0 | drops it: no Last |
| 1433730 | 21.000000 | 09:04:20.825 | trade, 80 shares; the day's first | keeps it |
| 1433731 | 21.000000 | 09:04:20.825 | trade, 3 shares | keeps it |
| 1433732 | 21.000000 | 09:04:20.825 | trade, 100 shares | keeps it |
| 1519615 | – | 09:05:02.509 | quote: ask 23.2, bid 21.2 | drops it |
| 1519616 | 21.200000 | 09:05:02.509 | trade, 224 shares | keeps it |
| 12730752 | – | 10:38:48.573 | quote: bid 21.0 | drops it |
| 12730756 | 21.200000 | 10:38:48.573 | trade, 62 shares | keeps it |
| 12730757 | 21.000000 | 10:38:48.573 | trade, 138 shares, same millisecond | keeps it; file order puts it last |
| 18960198 | – | 11:43:17.871 | quote: ask 20.6, bid 0 | drops it |
| 18960199 | 20.600000 | 11:43:17.871 | trade, 224 shares | keeps it |

The last line, as it arrives:

```text
A1PHBB.ETR,E,08-11-2021,11:43:17.000,,,,,,,,,,,,,,20.600000,,,,20.600000,224,11:43:17.871,831,,,,,,,,,11:43:17.000,,,11:43:17.871,,,
```

## Inside prep: `output/day.csv`

The `day` table holds every row that has both a Last and a Trading time. It stays inside the prep container; it is saved here so you can see it.

```text
symbol,sectype,last,event_ts,seq,bucket,date_filled
A1PHBB.ETR,E,0.0,2021-11-08 00:00:00+01,1,27,true
A1PHBB.ETR,E,21.0,2021-11-08 09:04:20.825+01,2,27,true
A1PHBB.ETR,E,21.0,2021-11-08 09:04:20.825+01,3,27,true
A1PHBB.ETR,E,21.0,2021-11-08 09:04:20.825+01,4,27,true
A1PHBB.ETR,E,21.2,2021-11-08 09:05:02.509+01,5,27,true
A1PHBB.ETR,E,21.2,2021-11-08 10:38:48.573+01,6,27,true
A1PHBB.ETR,E,21.0,2021-11-08 10:38:48.573+01,7,27,true
A1PHBB.ETR,E,20.6,2021-11-08 11:43:17.871+01,8,27,true
```

- **Converted.** The empty Trading date is filled with the file's day (`date_filled`). The time is read as Berlin time, so 11:43:17.871+01 is 10:43:17.871 UTC.
- **Added.**
  - `seq` numbers the kept rows in file order. It breaks ties: the two 10:38:48.573 trades are 6 and 7.
  - `bucket = hash('A1PHBB.ETR') % 32` = 27, the same on every row of the symbol.

## Out: `output/ticks/bucket=27/081121.parquet` and `.csv`

This is what crosses edge 2. The replayer reads the Parquet file; the CSV holds the same rows, read back from that file. The other 31 buckets get an empty Parquet file each (180 bytes, schema only), as in a full run: the replayer needs a file for every bucket of every day it replays.

```text
symbol,sectype,last,event_ts,seq
A1PHBB.ETR,E,21.0,2021-11-08 09:04:20.825+01,2
A1PHBB.ETR,E,21.0,2021-11-08 09:04:20.825+01,3
A1PHBB.ETR,E,21.0,2021-11-08 09:04:20.825+01,4
A1PHBB.ETR,E,21.2,2021-11-08 09:05:02.509+01,5
A1PHBB.ETR,E,21.2,2021-11-08 10:38:48.573+01,6
A1PHBB.ETR,E,21.0,2021-11-08 10:38:48.573+01,7
A1PHBB.ETR,E,20.6,2021-11-08 11:43:17.871+01,8
```

- **Dropped.** The `Last = 0` snapshot, plus `bucket` (it becomes the directory name) and `date_filled`. Overall, 12 lines became 7 rows, and 40 columns became 5.
- **Sorted.** The rows are in `(event_ts, seq)` order. In the full file this order becomes the Kafka offsets of partition 27.
- **Stored.** `event_ts` is an INT64 in microseconds, UTC (`TIMESTAMP_MICROS`), so 11:43:17.871+01 is `1636368197871000`. The CSV prints it in Berlin time.
- **What comes next.** These 7 trades make the symbol's first four 5-minute bars:

  | Window | Close |
  | --- | --- |
  | 09:00 | 21.0 |
  | 09:05 | 21.2 |
  | 10:35 | 21.0 (`seq` 7 is later than 6) |
  | 11:40 | 20.6 |

  The fourth bar is where EMA(38) falls below EMA(100): the SELL.

## Snippet against the full file

One thing differs from the real `data/ticks/bucket=27/081121.parquet`; everything else matches. The schema is the same, and so are `symbol`, `sectype`, `last` and `event_ts` for these 7 trades.

**`seq`.** `seq` is `row_number()` over the input file, so the snippet counts from 1. The order is the same, and order is all that `seq` is used for.

| Line | `seq` here | `seq` in the full Monday file |
| ---: | ---: | ---: |
| 31243 | 1 | 3522 |
| 1433730 | 2 | 636067 |
| 1433731 | 3 | 636068 |
| 1433732 | 4 | 636069 |
| 1519616 | 5 | 651394 |
| 12730756 | 6 | 2675338 |
| 12730757 | 7 | 2675339 |
| 18960199 | 8 | 4005181 |
