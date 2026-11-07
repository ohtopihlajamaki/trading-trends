#!/usr/bin/env python3
"""
Tallentaa ClickHousen signals-taulusta uuden testiajon signaalit CSV-tiedostoksi
täsmälleen samassa formaatissa kuin reference_signals_week.csv.

Käyttö:
    python utils/export_run_signals.py [tulostiedosto.csv]

Esimerkki:
    python utils/export_run_signals.py data/signals_run1_receipt.csv
"""

import sys
import subprocess
from pathlib import Path

SQL_QUERY = """
SELECT symbol,
       window_start,
       window_start + INTERVAL 5 MINUTE AS window_end,
       toString(side) AS signal,
       close,
       ema38,
       ema100
FROM signals FINAL
ORDER BY window_start, symbol
FORMAT CSVWithNames
"""

def export_run(out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Haetaan signaalit ClickHousesta (signals FINAL)...")
    
    cmd = [
        "docker", "exec", "stock-signals-clickhouse-1",
        "clickhouse-client", "--query", SQL_QUERY
    ]
    
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Virhe haettaessa signaaleja ClickHousesta:\n{e.stderr}", file=sys.stderr)
        sys.exit(1)
        
    csv_content = proc.stdout.strip()
    if not csv_content:
        print("Varoitus: signals-taulu oli tyhjä!", file=sys.stderr)
        sys.exit(1)
        
    lines = csv_content.splitlines()
    num_signals = len(lines) - 1
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(csv_content + "\n")
        
    buy_count = sum(1 for r in lines[1:] if ',"BUY",' in r)
    sell_count = sum(1 for r in lines[1:] if ',"SELL",' in r)
    
    print(f"Tallennettu: {out_path}")
    print(f"Signaaleja:  {num_signals:,} kpl (BUY: {buy_count:,}, SELL: {sell_count:,})")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Käyttö: python utils/export_run_signals.py <kohdetiedosto.csv>")
        sys.exit(1)
    export_run(Path(sys.argv[1]))
