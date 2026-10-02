#!/usr/bin/env python3
"""
Stream a (huge) CSV file and keep only the columns listed in COLUMNS.

The file is processed row by row, so memory use stays constant no matter
how big the input is. Header rows are kept (with the same columns removed).
Comment lines (starting with #) are dropped. At most MAX_ROWS data rows are
written.

Reads INPUT from data/ and writes OUTPUT to refined/, both next to this script.

Examples:
    python select_columns.py

    # file has 2 header rows, semicolon-delimited
    python select_columns.py --header-rows 2 -d ';'
"""
import argparse
import csv
import sys
from itertools import islice
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
REFINED_DIR = Path(__file__).resolve().parent / "refined"
INPUT = DATA_DIR / "081121.csv"
OUTPUT = REFINED_DIR / "small.csv"

# Columns to keep, in output order (looked up in the first header row)
COLUMNS = ["ID", "SecType", "Last", "Trading time", "Trading date"]

# Maximum number of data rows to write (None for no limit)
MAX_ROWS = 10_000


def resolve_columns(names, header):
    """Turn column names into a list of column indices."""
    indices = []
    for name in names:
        if name not in header:
            sys.exit(f"error: column {name!r} not found. Available: {', '.join(header)}")
        indices.append(header.index(name))
    return indices


def main():
    p = argparse.ArgumentParser(description="Keep header rows and the columns in COLUMNS of a large CSV.")
    p.add_argument("--header-rows", type=int, default=1,
                   help="number of header rows at the top of the file (default: 1)")
    p.add_argument("-d", "--delimiter", default=",", help="field delimiter (default: ,)")
    p.add_argument("--encoding", default="utf-8", help="file encoding (default: utf-8)")
    args = p.parse_args()

    # Allow very wide fields
    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

    if not INPUT.is_file():
        sys.exit(f"error: {INPUT} not found")

    with open(INPUT, newline="", encoding=args.encoding) as fin, \
            open(OUTPUT, "w", newline="", encoding=args.encoding) as fout:
        reader = csv.reader(fin, delimiter=args.delimiter)
        writer = csv.writer(fout, delimiter=args.delimiter)

        # Skip comment lines (licence preamble, column descriptions)
        rows = (row for row in reader if not (row and row[0].startswith("#")))

        headers = []
        for _ in range(args.header_rows):
            row = next(rows, None)
            if row is None:
                break
            headers.append(row)
        if not headers:
            sys.exit("error: input is empty")

        # Column names are looked up in the first header row
        cols = resolve_columns(COLUMNS, headers[0])

        def pick(row):
            return [row[i] if i < len(row) else "" for i in cols]

        for h in headers:
            writer.writerow(pick(h))

        n = 0
        for row in islice(rows, MAX_ROWS):
            writer.writerow(pick(row))
            n += 1
            if n % 1_000_000 == 0:
                print(f"{n:,} rows...", file=sys.stderr)

        print(f"done: {n:,} data rows written to {OUTPUT}", file=sys.stderr)


if __name__ == "__main__":
    main()
