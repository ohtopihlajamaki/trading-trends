#!/usr/bin/env python3
"""Showcase step 04, edge 3: prep -> ClickHouse instruments.

Runs prep's own build_instruments() and load_instruments() on the Sunday
snippet in input/ and saves in output/instruments/:

    instruments.parquet   the file prep writes next to the buckets
    request.txt           the HTTP request that load_instruments() sends

Two things are added around prep's code, and nothing in it changes:
  - urlopen is wrapped to save the request, and
  - to send it to the showcase database: load_instruments() builds its own
    URL, so the database travels in ClickHouse's X-ClickHouse-Database header.
Run it in the prep container from run.sh.
"""
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True  # keep prep/ free of __pycache__ when run from a checkout
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(next(d for d in (Path("/app"), HERE.parents[1] / "prep") if (d / "prep.py").exists())))

import duckdb  # noqa: E402
import prep  # noqa: E402  build_instruments, load_instruments, read_csv, INSTRUMENT_DAYS

OUT = HERE / "output" / "instruments"
DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "showcase")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    prep.RAW_DIR = HERE / "input"  # input/141121.csv: the Sunday snippet
    prep.TICKS_DIR = OUT           # where prep writes instruments.parquet
    if not prep.CLICKHOUSE_URL:
        sys.exit("error: CLICKHOUSE_URL is not set")

    real_urlopen = urllib.request.urlopen

    def urlopen(req, *args, **kwargs):
        req.add_header("X-ClickHouse-Database", DATABASE)
        url = urllib.parse.urlsplit(req.full_url)
        lines = [f"{req.get_method()} {url.path}?{urllib.parse.unquote(url.query)}",
                 f"Host: {url.netloc}"]
        lines += [f"{k}: {'(user:password, base64)' if k == 'Authorization' else v}"
                  for k, v in req.header_items()]
        (OUT / "request.txt").write_bytes(("\n".join(lines) + "\n\n").encode() + req.data)
        return real_urlopen(req, *args, **kwargs)

    urllib.request.urlopen = urlopen
    con = duckdb.connect()
    con.execute("SET TimeZone = 'Europe/Berlin'")
    days = list(dict.fromkeys(prep.INSTRUMENT_DAYS))  # as prep.main() passes them
    prep.log(f"instruments from INSTRUMENT_DAYS={' '.join(days)} in input/, into database {DATABASE}")
    prep.build_instruments(con, days)


if __name__ == "__main__":
    main()
