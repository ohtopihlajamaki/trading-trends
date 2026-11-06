# Data issue: Wednesday's 01:00 rows

Found on 4.11.2026 while tracing where rows are dropped in the pipeline. The rows are in
`data/raw/101121.csv` (Wednesday 10.11.2021): 5,506 rows with a Last price, for 1,812 symbols,
received 18:56:11–18:56:13 but stamped `Trading time = 01:00:00.000`. No other day has trades
before 07:00.

**Short answer.** The rows are in the right place in the file but carry a wrong timestamp. More
importantly, they aren't trades: each one repeats Wednesday's open, high or low, which had already
been traded at its real time.

## How they reach the EMAs

Prep is a batch step. It reads the whole day's CSV and writes each bucket with
`ORDER BY event_ts, seq` ([prep_day.sql](../prep/prep_day.sql)), so these rows, from the last 0.2%
of the file, open every bucket file (in bucket 7 they are the first 207 of 335,962 rows). The
replayer streams those sorted files and merges the buckets by event time, so the rows reach the
signal service first, almost six hours before the day's first real trade (07:00:00.964).

In receipt order (`PREP_ORDER=receipt`, not built yet) they would arrive at 18:56, and the signal
service would drop all 5,506 as late, because their 01:00–01:05 window ended long before the
watermark. They would still reach ClickHouse, though: `ticks_queue_mv` copies the topic into
`ticks` without a lateness check, and `backfill.sql` reads the Parquet directly. So the 01:00 prices
reach `ticks` and the `bars_5m` oracle in either order.

## What the rows are

All 1,812 symbols are Euronext indices: 1,441 Paris (`.FR`) and 371 Amsterdam (`.NL`). That covers
almost every Euronext index that had a price that day, and none of the stocks. Each index gets the
same three rows, in the same order. Here are the CAC 40's (PX1.FR), with one of its normal rows
from the morning for comparison:

| | Received | Open | Day's high | Day's low | **Last** | Trading time | Last trade time |
|---|---|---|---|---|---|---|---|
| Normal row | 09:05:02.000 | | | | 7046.17 | 09:05:02.006 | 09:05:02.000 |
| 01:00 row 1 | 18:56:11.000 | 7046.17 | | | **7046.17** | **01:00:00.000** | 18:56:11.000 |
| 01:00 row 2 | 18:56:11.000 | | 7053.27 | | **7053.27** | **01:00:00.000** | 18:56:11.000 |
| 01:00 row 3 | 18:56:11.000 | | | 7014.94 | **7014.94** | **01:00:00.000** | 18:56:11.000 |

The same three rows as they appear in the file:

```
PX1.FR,I,10-11-2021,18:56:11.000,,,,,,,,,,,,,,,,7046.170000,,7046.170000,,01:00:00.000,,,,,,,,,,18:56:11.000,,,,,,
PX1.FR,I,10-11-2021,18:56:11.000,,,,,,,,,,7053.270000,,,,,,,,7053.270000,,01:00:00.000,,,,,,,,,,18:56:11.000,,,,,,
PX1.FR,I,10-11-2021,18:56:11.000,,,,,,,,,,,,,,7014.940000,,,,7014.940000,,01:00:00.000,,,,,,,,,,18:56:11.000,,,,,,
```

To list all 5,506 (fields 22 and 24 are Last and Trading time; this takes a few minutes):

```sh
awk -F, '$24 == "01:00:00.000" && $22 != ""' data/raw/101121.csv
```

- **The positions are right.** The rows sit in the file among other symbols' 18:56 rows. Their
  `Time` and `Last trade time` say 18:56 too, but on trade rows those two columns are always equal,
  so they count as one piece of evidence, not two. That minute has 14,440 rows, against about 635
  per minute before and after it and on Tuesday. There's no gap before it, so the burst doesn't
  look like a reconnect after an outage.
- **The timestamp is wrong.** 01:00:00.000 in Berlin is 00:00 UTC, so it's a date with no time of
  day. The feed's normal placeholder is 00:00:00.000 (the 05:14 snapshot rows with `Last = 0`), so
  these rows came through some path that converted a UTC midnight to local time.
- **The prices aren't new.** Each Last copies the Open, Day's high or Day's low on the same row.
  They match Wednesday's real first price, highest and lowest for 1,811, 1,803 and 1,790 of the
  1,812 indices, and never fall outside the real range. The CAC 40 actually traded these prices at
  09:05:02 (open), 09:09:15 (high) and 13:34:15 (low).

**Probable cause:** a one-off re-send of the day's statistics for the Euronext indices, perhaps an
end-of-day summary or a correction. Whatever produced it filled Last with each statistic and
stamped only the date as the time. The feed then re-sends the high and low without Last, plus a
row with only the 01:00 Trading time. This is an inference from the data; nothing in the files
names the cause.

So giving them the right timestamp wouldn't help. At their real times they'd duplicate trades
already in the data, and at 18:56 they'd make a fake bar after the close. They should be dropped.

## What they do to the EMAs

Prep sorts them before Wednesday's open, so each index gets one extra bar at 01:00. That bar's
close is the last of the three rows, which is the **day's low for 96% of the indices**. A low that
the CAC 40 doesn't reach until 13:34 is already in its EMAs before the market opens.

The fake bar counts with weight 2/39 (5.1%) in EMA38 and 2/101 (2.0%) in EMA100. Gap means EMA38
minus EMA100; its sign decides BUY or SELL.

**CAC 40 (PX1.FR).** Tuesday's close is 7043.27; the fake bar closes 0.4% below it.

| Bar | Close | EMA38 with / without | EMA100 with / without | Gap with / without |
|---|---|---|---|---|
| Tue 18:05 | 7043.27 | 7051.71 / 7051.71 | 7056.31 / 7056.31 | −4.60 / −4.60 |
| **Wed 01:00 (fake)** | **7014.94** | 7049.83 / — | 7055.49 / — | −5.66 / — |
| Wed 09:05 | 7049.50 | 7049.81 / 7051.60 | 7055.37 / 7056.18 | −5.56 / −4.58 |
| Wed 10:00 | 7034.66 | 7046.12 / 7047.12 | 7052.73 / 7053.38 | −6.62 / −6.26 |
| Wed 12:00 | 7026.53 | 7031.46 / 7031.74 | 7042.59 / 7042.99 | −11.14 / −11.25 |
| Wed 16:00 | 7039.25 | 7027.62 / 7027.64 | 7032.49 / 7032.64 | −4.87 / −5.00 |

- At the open the gap is about 20% wider: −5.56 against −4.58.
- By 12:00 the error has reversed sign. EMA38 forgets the fake bar faster than EMA100, so after
  about 29 bars (roughly 2½ hours) the fake low makes the gap look slightly higher instead of lower.
- The afternoon BUY comes at 16:45 instead of 16:50.

**CAC Small (CACS.FR).** This is a case where the crossover was razor-thin (+0.11 on a price of
14,960):

| Bar | Close | Gap with / without | Signal with / without |
|---|---|---|---|
| Tue 18:05 | 14960.19 | +0.11 / +0.11 | BUY / BUY |
| **Wed 01:00 (fake)** | **14893.37** | −1.72 / — | **SELL** / — |
| Wed 09:05 | 14946.18 | −1.76 / −0.06 | — / SELL |

Here the SELL moves from 09:05 to 01:00, when the market is closed. The afternoon BUY moves by 5
minutes, and four of Thursday's signals still differ.

**The extreme case.** A 3× leveraged index (ITSL3.NL) gets a fake close of 7.91 against Tuesday's
10.58, which is −25%. Its EMA38 at the open is 11.29 instead of 11.47. Its gap was wide, so no
signal changed.

## Across all 1,812 indices

- **Fake close vs Tuesday's close:** median −0.29%; 5% of indices are below −3.4%.
- **At Wednesday's first real bar:**
  - The median gap shift is 0.009% of price, against a median gap of 0.091%, so typically about a
    tenth of the gap.
  - For 89 indices the shift is larger than the gap itself.
  - 43 indices open on the wrong side of the crossover.
- **Signals:** 1,834 differ between the two versions, on 674 indices (Wed 1,388, Thu 404, Fri 42).
  - 1,390 are the same signal moved by one bar (5 minutes).
  - 1,581 have a match within 30 minutes.
  - 253 have no counterpart within 30 minutes.
  - 51 fire on the fake 01:00 bar itself.

So the main damage is signals firing a bar or so early or late on Wednesday and Thursday, plus a
few hundred signals that are created or lost. That's small next to the week's 19,989 signals, but
all of it comes from four seconds of bad data.

## Fix

Drop the rows in prep. A filter on trades with a Trading time before 07:00 Berlin removes exactly
these rows: on every day of the week, no other positive-Last row is earlier than 07:00. See the
todo in [PLAN-2026-11-04.md](PLAN-2026-11-04.md).

## Method and caveats

- Only Monday is prepped in `data/ticks`. The Wednesday figures come from running prep's
  `read_csv` options and SQL expressions on the raw CSVs, and a Python copy of
  `signal/state.go` (first-close seeding, flip rule), run on Mon–Fri with and without the 01:00
  rows. On Monday the copy matches the real buckets and PLAN.md's checks (373,126 bars, 5,735
  signals). The scripts were scratch work and are not in the repo.
- This recount gives 899 removed signals (935 added) where an earlier count gave 900. The
  one-signal difference hasn't been tracked down.
