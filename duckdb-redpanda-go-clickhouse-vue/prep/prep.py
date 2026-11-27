#!/usr/bin/env python3
"""Turn each day's CSV into PARTITIONS Parquet buckets in receipt (file) order.

For every day in DAYS this reads RAW_DIR/<day>.csv (or the same file under its
Zenodo name, debs2022-gc-trading-day-dd-mm-yy.csv) and writes
TICKS_DIR/bucket=<kk>/<day>.parquet, one file per bucket, and prints the
checks from the plan: the preamble, the rejects, the trade-row share and the
rows per bucket. The days in INSTRUMENT_DAYS (or DAYS, when none of those
files is there) feed the instruments table, which is written to
TICKS_DIR/instruments.parquet and loaded into ClickHouse over HTTP when
CLICKHOUSE_URL is set.

    docker compose --profile prep run --rm prep
"""
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from string import Template

import duckdb

RAW_DIR = Path(os.environ.get("RAW_DIR", "/data/raw"))
TICKS_DIR = Path(os.environ.get("TICKS_DIR", "/data/ticks"))
DAYS = os.environ.get("DAYS", "081121").split()
INSTRUMENT_DAYS = os.environ.get("INSTRUMENT_DAYS", "141121").split()
PARTITIONS = int(os.environ.get("PARTITIONS", "32"))
THREADS = int(os.environ.get("PREP_THREADS", "4"))
MEMORY = os.environ.get("PREP_MEMORY", "3GB")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "")

PREAMBLE_LINES = 13

# prep_day.sql holds two statements: build the day table, then one COPY per bucket
_sql = "\n".join(line for line in (Path(__file__).parent / "prep_day.sql").read_text().splitlines()
                  if not line.lstrip().startswith("--"))
CREATE_DAY, COPY_BUCKET = (Template(s.strip()) for s in _sql.split(";") if s.strip())


# Comment lines (the 11-line preamble and the column descriptions) are dropped
# with comment='#' rather than skip=13: in DuckDB 1.5 skip=13 with this dialect
# silently drops the first ~339k data rows, and the sniffed dialect makes the
# parallel reader give up on full files. The header row (39 names) is null-padded
# to the 40 data fields and filtered out with column00 != 'ID'.
CSV_OPTIONS = ("header=false, comment='#', auto_detect=false, delim=',', quote='', escape='', "
               "new_line='\\n', null_padding=true, columns={"
               + ", ".join(f"'column{i:02d}': 'VARCHAR'" for i in range(40)) + "}")


def read_csv(paths):
    """read_csv over one path or a list of paths, with every column as VARCHAR."""
    src = f"[{', '.join(repr(str(p)) for p in paths)}]" if isinstance(paths, list) else repr(str(paths))
    return f"read_csv({src}, {CSV_OPTIONS})"


def log(msg):
    print(msg, file=sys.stderr)


def check_preamble(path):
    """The first 11 lines are comments, then the header, then a commented description."""
    with open(path, "rb") as f:
        lines = [f.readline() for _ in range(PREAMBLE_LINES + 1)]
    ok = (all(line.startswith(b"#") for line in lines[:11])
          and lines[11].startswith(b"ID,")
          and lines[12].startswith(b"#")
          and not lines[13].startswith(b"#"))
    if not ok:
        sys.exit(f"error: {path.name} does not start with the expected {PREAMBLE_LINES}-line preamble")
    crlf = sum(line.endswith(b"\r\n") for line in lines[:PREAMBLE_LINES])
    data_eol = "CRLF" if lines[13].endswith(b"\r\n") else "LF"
    header_fields = lines[11].count(b",") + 1
    data_fields = lines[13].count(b",") + 1
    log(f"  preamble ok: {crlf} of {PREAMBLE_LINES} lines CRLF, data rows {data_eol}; "
        f"header {header_fields} names, data rows {data_fields} fields")


def scan_stats(con, csv):
    """One pass over the whole file for the counts the day table cannot give."""
    return con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE column21 IS NOT NULL AND column23 IS NOT NULL),
               count(*) FILTER (WHERE column23 IS NOT NULL
                                  AND NOT regexp_full_match(column23, '\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d')),
               count(*) FILTER (WHERE starts_with(column00, '#'))
        FROM {read_csv(csv)} WHERE column00 != 'ID'""").fetchone()


def raw_csv_paths(day):
    """The names a day's CSV may have: <ddmmyy>.csv, or the Zenodo download's own name."""
    return [RAW_DIR / f"{day}.csv", RAW_DIR / f"debs2022-gc-trading-day-{day[:2]}-{day[2:4]}-{day[4:]}.csv"]


def raw_csv(day):
    """The day's CSV in RAW_DIR under either name, or None."""
    return next((p for p in raw_csv_paths(day) if p.exists()), None)


def prep_day(con, day):
    csv = raw_csv(day)
    if csv is None:
        sys.exit(f"error: {day}: neither {' nor '.join(map(str, raw_csv_paths(day)))} found")
    log(f"{day}: {csv} ({csv.stat().st_size / 1e9:.2f} GB)")
    check_preamble(csv)

    t0 = time.perf_counter()
    rows, candidates, bad_time, comments = scan_stats(con, csv)
    t1 = time.perf_counter()
    date = f"{day[:2]}-{day[2:4]}-20{day[4:]}"
    con.execute(CREATE_DAY.substitute(source=read_csv(csv), date=date, partitions=PARTITIONS, bucket="", out=""))
    t2 = time.perf_counter()

    (n_day, date_filled, bad_last, bad_ts, trades, zero_price, zero_mixed, other_day, ms_rows,
     first, last, symbols) = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE date_filled),
               count(*) FILTER (WHERE last IS NULL),
               count(*) FILTER (WHERE event_ts IS NULL),
               count(*) FILTER (WHERE event_ts IS NOT NULL AND last > 0),
               count(*) FILTER (WHERE last <= 0),
               count(DISTINCT symbol) FILTER (WHERE last <= 0 AND symbol IN (SELECT symbol FROM day WHERE last > 0)),
               count(*) FILTER (WHERE event_ts IS NOT NULL
                                  AND strftime(timezone('Europe/Berlin', event_ts), '%d%m%y') != '{day}'),
               count(*) FILTER (WHERE last > 0 AND millisecond(event_ts) % 1000 != 0),
               strftime(timezone('Europe/Berlin', min(event_ts) FILTER (WHERE last > 0)), '%Y-%m-%d %H:%M:%S.%g'),
               strftime(timezone('Europe/Berlin', max(event_ts) FILTER (WHERE last > 0)), '%Y-%m-%d %H:%M:%S.%g'),
               count(DISTINCT symbol) FILTER (WHERE event_ts IS NOT NULL AND last > 0)
        FROM day""").fetchone()
    assert n_day == candidates, (n_day, candidates)

    log(f"  rows {rows:,}; with Last and Trading time {candidates:,} "
        f"({date_filled:,} without Trading date, filled with {date}); "
        f"comment rows {comments:,}; Trading time not HH:MM:SS.mmm {bad_time:,}")
    log(f"  rejects: Last not a number {bad_last:,}, no event time {bad_ts:,}, "
        f"Last <= 0 {zero_price:,} (symbols that also trade above zero: {zero_mixed:,})")
    log(f"  trades {trades:,} ({trades / rows:.2%} of rows), {symbols:,} symbols, "
        f"{first} .. {last} Europe/Berlin")
    log(f"  event date not {day} {other_day:,}; "
        f"non-zero milliseconds {ms_rows / max(trades, 1):.1%}")

    per_bucket = []
    for k in range(PARTITIONS):
        out_dir = TICKS_DIR / f"bucket={k:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{day}.parquet"
        tmp = out_dir / f".{day}.parquet.tmp"
        con.execute(COPY_BUCKET.substitute(source="", date=date, partitions=PARTITIONS, bucket=k, out=tmp))
        os.replace(tmp, out)
        per_bucket.append(con.execute("SELECT count(*) FROM read_parquet(?)", [str(out)]).fetchone()[0])
    t3 = time.perf_counter()

    mean = sum(per_bucket) / PARTITIONS
    assert sum(per_bucket) == trades, (sum(per_bucket), trades)
    log(f"  buckets: min {min(per_bucket):,}, max {max(per_bucket):,}, mean {mean:,.0f}, "
        f"max/mean {max(per_bucket) / mean:.2f}")
    log("  rows per bucket: " + " ".join(f"{k:02d}:{n}" for k, n in enumerate(per_bucket)))
    log(f"  time: scan {t1 - t0:.0f}s, parse {t2 - t1:.0f}s, write {t3 - t2:.0f}s")
    con.execute("DROP TABLE day")


def build_instruments(con, days):
    """One row per symbol: exchange from the ID suffix, ISIN and currency where any row has them."""
    files = [str(p) for p in map(raw_csv, days) if p is not None]
    if not files:
        log("instruments: no files, skipped")
        return
    out = TICKS_DIR / "instruments.parquet"
    con.execute(f"""
        COPY (SELECT column00 AS symbol,
                     coalesce(regexp_extract(column00, '\\.([^.]+)$', 1), '') AS exchange,
                     coalesce(max(column01), '') AS sectype,
                     coalesce(max(column14), '') AS isin,
                     coalesce(max(column11), '') AS currency
              FROM {read_csv(files)}
              WHERE column00 IS NOT NULL AND column00 != 'ID'
              GROUP BY column00 ORDER BY column00)
        TO '{out}' (FORMAT parquet)""")
    rows = con.execute("SELECT * FROM read_parquet(?)", [str(out)]).fetchall()
    log(f"instruments: {len(rows):,} symbols from {', '.join(Path(f).stem for f in files)} -> {out}")
    if CLICKHOUSE_URL:
        load_instruments(rows)


def load_instruments(rows):
    cols = ("symbol", "exchange", "sectype", "isin", "currency")
    body = "".join(json.dumps(dict(zip(cols, r))) + "\n" for r in rows).encode()
    query = urllib.parse.quote("INSERT INTO instruments FORMAT JSONEachRow")
    req = urllib.request.Request(f"{CLICKHOUSE_URL}/?query={query}", data=body, method="POST")
    auth = f"{os.environ.get('CLICKHOUSE_USER', 'default')}:{os.environ.get('CLICKHOUSE_PASSWORD', '')}"
    req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=60) as resp:
        resp.read()
    log(f"instruments: loaded {len(rows):,} rows into ClickHouse")


def main():
    start = time.perf_counter()
    TICKS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = Path("/tmp/prep")
    tmp.mkdir(exist_ok=True)
    con = duckdb.connect(str(tmp / "prep.duckdb"))
    con.execute(f"SET threads = {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY}'")
    con.execute(f"SET temp_directory = '{tmp / 'spill'}'")
    con.execute("SET TimeZone = 'Europe/Berlin'")
    log(f"duckdb {duckdb.__version__}, {THREADS} threads, memory {MEMORY}, "
        f"{PARTITIONS} buckets, days {' '.join(DAYS)}")

    for day in DAYS:
        prep_day(con, day)
    instrument_days = list(dict.fromkeys(INSTRUMENT_DAYS))
    if not any(map(raw_csv, instrument_days)):
        log(f"instruments: no file for {' '.join(instrument_days)}, reading DAYS instead")
        instrument_days = DAYS
    build_instruments(con, instrument_days)
    log(f"done in {time.perf_counter() - start:.0f}s")


if __name__ == "__main__":
    main()
