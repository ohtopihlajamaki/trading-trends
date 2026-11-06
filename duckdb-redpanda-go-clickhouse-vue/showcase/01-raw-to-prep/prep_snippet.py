#!/usr/bin/env python3
"""Showcase step 01: raw CSV -> prep -> Parquet bucket.

Runs prep's own SQL (prep/prep_day.sql, loaded by prep.py) on the snippet in
input/ and saves what prep makes of it in output/:

    output/day.csv                          the DuckDB table that stays inside prep
    output/ticks/bucket=KK/DDMMYY.parquet   what prep writes for the replayer
    output/ticks/bucket=KK/DDMMYY.csv       the same rows as CSV, for reading

Like prep.py it writes all 32 buckets, empty ones included; the CSV copy is
written only for buckets with rows. Run it in the prep container with ./run.sh.
"""
import shutil
import sys
from pathlib import Path

sys.dont_write_bytecode = True  # keep prep/ free of __pycache__ when run from a checkout
HERE = Path(__file__).resolve().parent
# prep.py is in /app in the prep image, and in ../../prep in a checkout
sys.path.insert(0, str(next(d for d in (Path("/app"), HERE.parents[1] / "prep") if (d / "prep.py").exists())))

import duckdb  # noqa: E402
import prep  # noqa: E402  CREATE_DAY, COPY_BUCKET, read_csv, check_preamble, PARTITIONS

IN, OUT = HERE / "input", HERE / "output"


def main():
    (csv,) = sorted(IN.glob("*.csv"))
    day = csv.stem
    date = f"{day[:2]}-{day[2:4]}-20{day[4:]}"  # as in prep.prep_day: 081121 -> 08-11-2021
    prep.log(f"{day}: {csv.relative_to(HERE)}, duckdb {duckdb.__version__}, {prep.PARTITIONS} buckets")
    prep.check_preamble(csv)

    shutil.rmtree(OUT, ignore_errors=True)
    OUT.mkdir()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'Europe/Berlin'")
    con.execute(prep.CREATE_DAY.substitute(source=prep.read_csv(csv), date=date,
                                           partitions=prep.PARTITIONS, bucket="", out=""))
    con.execute(f"COPY (SELECT * FROM day ORDER BY seq) TO '{OUT / 'day.csv'}' (HEADER)")

    data_lines = sum(1 for line in csv.open("rb") if not line.startswith((b"#", b"ID,")))
    kept, rejected = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE event_ts IS NULL OR last IS NULL OR last <= 0)
        FROM day""").fetchone()
    prep.log(f"  {data_lines} data lines -> {kept} rows in day (with Last and Trading time), "
             f"{rejected} of them rejected (no event time, or Last <= 0)")

    # Every bucket gets a file, empty ones included, as in prep.py: the replayer
    # (step 02) refuses to start if one is missing.
    empty = 0
    for k in range(prep.PARTITIONS):
        out_dir = OUT / "ticks" / f"bucket={k:02d}"
        out_dir.mkdir(parents=True)
        parquet = out_dir / f"{day}.parquet"
        con.execute(prep.COPY_BUCKET.substitute(source="", date=date, partitions=prep.PARTITIONS,
                                                bucket=k, out=parquet))
        n = con.execute("SELECT count(*) FROM read_parquet(?)", [str(parquet)]).fetchone()[0]
        if n == 0:
            empty += 1
            continue
        # Read back from the Parquet file, so the CSV shows exactly what was written.
        # hive_partitioning=false: otherwise DuckDB adds a bucket column from the path
        con.execute(f"""COPY (SELECT * FROM read_parquet('{parquet}', hive_partitioning = false))
                        TO '{parquet.with_suffix('.csv')}' (HEADER)""")
        prep.log(f"  bucket {k:02d}: {n} rows -> {parquet.relative_to(HERE)} (+ .csv)")
    prep.log(f"  {empty} other buckets: empty files, as prep.py writes them")


if __name__ == "__main__":
    main()
