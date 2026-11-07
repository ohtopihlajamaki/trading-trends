#!/usr/bin/env python3
"""
Generoi ja tallentaa DEBS 2022 -viikon (081121-121121) kaikki 19 989
referenssisignaalia (ground truth) suoraan CSV-tiedostoksi.

Käyttää ClickHousen virallista oracle.sql -laskentasääntöä
nykyisistä esikäsitellyistä ja aikajärjestetyistä Parquet-tiedostoista (data/ticks/bucket=*/*.parquet).

Sarakkeet:
 - symbol: osaketicker (esim. 703000.ETR, MC.FR)
 - window_start: 5 min ikkunan alkuaika (Europe/Berlin)
 - window_end: 5 min ikkunan päättymisaika (Europe/Berlin)
 - signal: BUY tai SELL
 - close: ikkunan viimeisen kaupan hinta
 - ema38: 38 ikkunan liukuva keskiarvo
 - ema100: 100 ikkunan liukuva keskiarvo

Käyttö:
    python utils/export_reference_signals.py [tulostiedosto.csv]
"""

import sys
import time
import subprocess
from pathlib import Path

DEFAULT_OUTPUT = Path("data/reference_signals_week.csv")

ORACLE_EXPORT_SQL = """
WITH 2 / 39 AS a38, 2 / 101 AS a100,
raw_ticks AS (
    SELECT symbol, toStartOfFiveMinutes(toDateTime64(event_ts, 3, 'Europe/Berlin')) AS window_start,
           last, toDateTime64(event_ts, 3, 'Europe/Berlin') AS ts, toUInt64(seq) AS seq
    FROM file('data/ticks/bucket=*/*.parquet', Parquet,
              'symbol String, sectype String, last Float64, event_ts DateTime64(6, \\'UTC\\'), seq Int64')
),
bars AS (
    SELECT symbol, window_start, argMax(last, (ts, seq)) AS close
    FROM raw_ticks
    GROUP BY symbol, window_start
),
series AS (
    SELECT symbol,
           arraySort(groupArray(window_start))                              AS w,
           arrayMap(x -> x.2, arraySort(groupArray((window_start, close)))) AS c
    FROM bars GROUP BY symbol
),
ema AS (
    SELECT symbol, w, c,
           arrayFold((acc, x) -> arrayPushBack(acc,
                        (x * a38  + acc[-1].1 * (1 - a38),
                         x * a100 + acc[-1].2 * (1 - a100))),
                     arraySlice(c, 2), [(c[1], c[1])]) AS e
    FROM series
),
flat AS (
    SELECT symbol, w_i AS window_start, c_i AS close, e_i.1 AS ema38, e_i.2 AS ema100,
           sign(e_i.1 - e_i.2) AS rel
    FROM ema ARRAY JOIN w AS w_i, c AS c_i, e AS e_i
),
cmp AS (
    SELECT *,
           anyLast(nullIf(rel, 0)) OVER (PARTITION BY symbol ORDER BY window_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_rel
    FROM flat
)
SELECT symbol, window_start, window_start + INTERVAL 5 MINUTE AS window_end,
       multiIf(rel = 1 AND prev_rel = -1, 'BUY', rel = -1 AND prev_rel = 1, 'SELL', '') AS signal,
       close, ema38, ema100
FROM cmp WHERE signal != ''
ORDER BY window_start, symbol
FORMAT CSVWithNames
"""

def export_signals(out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Lasketaan ja generoidaan referenssisignaalit ClickHousen oracle-säännöllä...")
    print(f"Lähde: data/ticks/bucket=*/*.parquet (kaikki 32 buckettia, 5 päivää)")
    print(f"Kohde: {out_path}")
    
    t0 = time.perf_counter()
    
    cmd = [
        "docker", "exec", "stock-signals-clickhouse-1",
        "clickhouse-client", "--query", ORACLE_EXPORT_SQL
    ]
    
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Virhe suoritettaessa ClickHouse-kyselyä:\n{e.stderr}", file=sys.stderr)
        sys.exit(1)
        
    csv_content = proc.stdout
    lines = csv_content.strip().splitlines()
    
    if not lines:
        print("Virhe: Ei tuloksia!", file=sys.stderr)
        sys.exit(1)
        
    header = lines[0]
    data_rows = lines[1:]
    num_signals = len(data_rows)
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(csv_content)
        
    elapsed = time.perf_counter() - t0
    
    buy_count = sum(1 for row in data_rows if ',"BUY",' in row)
    sell_count = sum(1 for row in data_rows if ',"SELL",' in row)
    
    print(f"\nValmis! Kesto: {elapsed:.2f} s")
    print(f"Tallennettu tiedostoon: {out_path} ({out_path.stat().st_size / 1024:.1f} KB)")
    print(f"Signaaleja yhteensä:    {num_signals:,} kpl")
    print(f"  - BUY-signaaleja:     {buy_count:,} kpl")
    print(f"  - SELL-signaaleja:    {sell_count:,} kpl")
    
    if num_signals == 19989:
        print(f"[OK] Täsmää täydellisesti viikon referenssiin (tasan 19 989 signaalia)!")
    else:
        print(f"[VAROITUS] Odotettiin 19 989 signaalia, saatiin {num_signals}")

if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT
    export_signals(target)
