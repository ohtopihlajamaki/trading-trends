#!/usr/bin/env python3
"""
Filter every CSV in data/ down to the columns in COLUMNS and merge them into
one file ordered by Trading date, then Trading time.

Each input file holds one day, so every day is sorted on its own, in its own
process, and the sorted days are concatenated in date order. Within a day the
kept rows are split into one temp file per hour of Trading time and each hour
is sorted in memory, so memory use stays small no matter how big the input is.

Only rows with a Last price are kept. Trading date is empty in most source
rows, so it is filled in with the file's day. Comment lines (starting with #)
are dropped. Rows with equal Trading time keep their order in the source file.
At most MAX_ROWS data rows are written.

Assumes comma-delimited input with one header row and no quoted fields.

Reads data/raw/*.csv and writes OUTPUT to refined/, both in the repository root
(the parent of this script's directory). Works from any working directory.

    python utils/select_columns.py
"""
import shutil
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import islice, repeat
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "raw"
REFINED_DIR = ROOT / "refined"
OUTPUT = REFINED_DIR / "small.csv"

# Columns to keep, in output order (looked up in the header row)
COLUMNS = ["ID", "SecType", "Last", "Trading time", "Trading date"]

# Rows are kept only if this column is non-empty
REQUIRED_COLUMN = "Last"
# Rows are ordered by the file's day (taken from DAY_COLUMN), then by TIME_COLUMN
DAY_COLUMN = "Date"
TIME_COLUMN = "Trading time"
# Filled in with the file's day
DATE_COLUMN = "Trading date"
DAY_FORMAT = "%d-%m-%Y"

# Maximum number of data rows to write (None for no limit)
MAX_ROWS = None

COMMENT = ord("#")


def resolve_columns(names, header):
    """Turn column names into a list of column indices."""
    indices = []
    for name in names:
        if name not in header:
            sys.exit(f"error: column {name!r} not found. Available: {', '.join(header)}")
        indices.append(header.index(name))
    return indices


def read_header_and_day(path):
    """Return the header row of a file and the day its rows belong to."""
    with open(path, "rb") as f:
        rows = (line.decode().rstrip("\r\n").split(",") for line in f if line[0] != COMMENT)
        header = next(rows, None)
        if header is None:
            sys.exit(f"error: {path} is empty")
        day_idx = resolve_columns([DAY_COLUMN], header)[0]
        # A few rows have no date, so take the first one that does
        day = next((row[day_idx] for row in rows if len(row) > day_idx and row[day_idx]), None)
        if day is None:
            sys.exit(f"error: no {DAY_COLUMN!r} value found in {path}")
    return header, day


def refine_day(path, header, day, tmp_dir):
    """Filter one day's file and write it sorted by time.

    Returns (sorted file, rows read, rows kept, malformed rows skipped).
    """
    cols = resolve_columns(COLUMNS, header)
    required_idx, time_idx, date_idx = resolve_columns([REQUIRED_COLUMN, TIME_COLUMN, DATE_COLUMN], header)
    # Fields past the last one we need are left unsplit
    maxsplit = max(cols + [required_idx, time_idx, date_idx]) + 1
    time_pos = COLUMNS.index(TIME_COLUMN)
    date_pos = COLUMNS.index(DATE_COLUMN)
    day_bytes = day.encode()

    buckets = {}  # hour of trading time -> temp file with that hour's rows
    n_read = n_kept = n_bad = 0
    with open(path, "rb") as fin:
        lines = iter(fin)
        for line in lines:  # skip up to and including the header row
            if line[0] != COMMENT:
                break
        for line in lines:
            if line[0] == COMMENT:
                continue
            n_read += 1
            row = line.split(b",", maxsplit)
            try:
                if not row[required_idx]:
                    continue
                if row[date_idx] and row[date_idx] != day_bytes:
                    # Sorting day by day is only valid if a file never mixes days
                    sys.exit(f"error: {path.name} is for {day} but has a row with "
                             f"{DATE_COLUMN} {row[date_idx].decode()}")
                out = [row[i] for i in cols]
            except IndexError:
                n_bad += 1
                continue
            out[date_pos] = day_bytes
            hour = out[time_pos][:2]
            bucket = buckets.get(hour)
            if bucket is None:
                bucket = buckets[hour] = open(Path(tmp_dir) / f"{day}.{len(buckets)}.part", "w+b",
                                              buffering=1 << 20)
            bucket.write(b",".join(out) + b"\n")
            n_kept += 1

    sorted_path = Path(tmp_dir) / f"{day}.csv"
    with open(sorted_path, "wb") as fout:
        for hour in sorted(buckets):
            with buckets[hour] as bucket:
                bucket.seek(0)
                rows = bucket.readlines()
            # list.sort is stable, so rows with equal times keep their source order
            rows.sort(key=lambda r: r.split(b",")[time_pos])
            fout.writelines(rows)
    return sorted_path, n_read, n_kept, n_bad


def main():
    start = time.perf_counter()

    paths = sorted(DATA_DIR.glob("*.csv"))
    if not paths:
        sys.exit(f"error: no .csv files found in {DATA_DIR}")

    # Each file holds one day; work out which, and put the files in date order
    files = {}
    for path in paths:
        header, day = read_header_and_day(path)
        if day in files:
            sys.exit(f"error: {files[day][0].name} and {path.name} are both for {day}")
        files[day] = (path, header)
    days = sorted(files, key=lambda day: datetime.strptime(day, DAY_FORMAT))
    args = ([files[day][0] for day in days], [files[day][1] for day in days], days)

    REFINED_DIR.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=REFINED_DIR) as tmp_dir, ProcessPoolExecutor() as pool:
        if MAX_ROWS is None:
            # Every day is needed, so work on them all in parallel
            results = pool.map(refine_day, *args, repeat(tmp_dir))
        else:
            # Only the earliest days are needed: take them one at a time and
            # stop as soon as there are enough rows
            results = map(refine_day, *args, repeat(tmp_dir))

        n = 0
        with open(OUTPUT, "wb") as fout:
            fout.write(",".join(COLUMNS).encode() + b"\n")
            for day, (sorted_path, n_read, n_kept, n_bad) in zip(days, results):
                skipped = f", {n_bad:,} malformed rows skipped" if n_bad else ""
                print(f"{day}: kept {n_kept:,} of {n_read:,} rows{skipped}", file=sys.stderr)
                with open(sorted_path, "rb") as fin:
                    if MAX_ROWS is None:
                        shutil.copyfileobj(fin, fout, 1 << 24)
                        n += n_kept
                    else:
                        rows = list(islice(fin, MAX_ROWS - n))
                        fout.writelines(rows)
                        n += len(rows)
                if n == MAX_ROWS:
                    break

    print(f"done: {n:,} data rows written to {OUTPUT} in {time.perf_counter() - start:.0f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
