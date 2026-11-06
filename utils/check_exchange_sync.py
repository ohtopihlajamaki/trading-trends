#!/usr/bin/env python3
"""
Tarkistaa ja ottaa ylös (tallentaa), kuinka suuri osa tapahtumista on
pörssin (Frankfurt .ETR, Pariisi .FR, Amsterdam .NL) sisällä epäsynkassa.

Tarkistus laskee ja raportoi:
 - Epäsynkassa olevien kauppojen määrän ja osuuden kauppariveistä (%)
 - Epäsynkassa olevien kauppojen osuuden kaikista pörssin riveistä (%)
 - Kuinka moni pörssin osakesymboleista kärsii epäsynkasta (%)
 - Viiveiden jakauman (<=10ms, <=50ms, <=200ms, <=1s, >1s) ja persentiilit (p50, p90, p99, max)
 - Top-osakkeet, joilla epäsynkkaa esiintyy eniten
 - Tallentaa yhteenvedon JSON-tiedostoon myöhempää käyttöä ja raportteja varten

Käyttö:
    python utils/check_exchange_sync.py [tiedostopolku] [pörssikoodi] [--json raportti.json]

Esimerkkejä:
    python utils/check_exchange_sync.py data/raw/081121_1m.csv ETR
    python utils/check_exchange_sync.py data/raw/081121.csv FR
    python utils/check_exchange_sync.py data/raw/081121_1m.csv ALL --json sync_tulokset.json
"""

import sys
import time
import json
import array
from collections import Counter
from pathlib import Path

# Varmistetaan UTF-8 yhteensopivuus Windowsin konsolissa
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

EXCHANGES = {
    'ETR': 'Frankfurt (Xetra)',
    'FR': 'Pariisi (Euronext Paris)',
    'NL': 'Amsterdam (Euronext Amsterdam)',
}

def parse_time_ms(time_str: str) -> int:
    """Muuntaa 'HH:MM:SS.mmm' kokonaismillisekunneiksi päivän alusta."""
    try:
        h, m, s = time_str.split(':')
        sec, ms = s.split('.')
        return (int(h) * 3600 + int(m) * 60 + int(sec)) * 1000 + int(ms)
    except Exception:
        return -1

def format_ms(ms: int) -> str:
    h = ms // 3600000
    rem = ms % 3600000
    m = rem // 60000
    rem = rem % 60000
    s = rem // 1000
    millis = rem % 1000
    return f"{h:02d}:{m:02d}:{s:02d}.{millis:03d}"

def calculate_percentiles(delays_arr):
    if not delays_arr:
        return {'p50_ms': 0, 'p90_ms': 0, 'p95_ms': 0, 'p99_ms': 0, 'max_ms': 0}
    s = sorted(delays_arr)
    n = len(s)
    return {
        'p50_ms': s[int(n * 0.50)],
        'p90_ms': s[min(int(n * 0.90), n - 1)],
        'p95_ms': s[min(int(n * 0.95), n - 1)],
        'p99_ms': s[min(int(n * 0.99), n - 1)],
        'max_ms': s[-1],
    }

def check_sync(csv_path: str, target_exchange: str = 'ALL', json_out_path: str = None):
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"Virhe: Tiedostoa ei löydy: {csv_path}", file=sys.stderr)
        sys.exit(1)

    target_exchange = target_exchange.upper()
    if target_exchange in ('ALL', '', 'KAIKKI'):
        active_exchanges = list(EXCHANGES.keys())
    else:
        active_exchanges = [target_exchange]

    stats = {}
    for ex in active_exchanges:
        stats[ex] = {
            'total_rows': 0,
            'trades': 0,
            'max_ts_ms': -1,
            'ooo_trades': 0,
            'max_delay_ms': 0,
            'delays': array.array('I'),  # 32-bit unsigned intit viiveille (ms)
            'buckets': {
                '<= 10 ms': 0,
                '11 - 50 ms': 0,
                '51 - 200 ms': 0,
                '201 - 1000 ms': 0,
                '> 1000 ms': 0,
            },
            'sym_trades': Counter(),
            'sym_ooo_counts': Counter(),
            'sym_max_ts': {},
            'sym_ooo_trades': 0,
            'examples': []
        }

    print(f"Luetaan tiedostoa: {csv_file} ({csv_file.stat().st_size / (1024*1024):.1f} MB)")
    print(f"Tarkistettavat pörssit: {', '.join(active_exchanges)}")
    t0 = time.perf_counter()

    line_count = 0
    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line_count += 1
            if line.startswith('#') or line.startswith('ID'):
                continue

            parts = line.split(',', 24)
            if len(parts) < 24:
                continue

            symbol = parts[0]
            if '.' not in symbol:
                continue
            ex = symbol.split('.')[-1]
            if ex not in stats:
                continue

            st = stats[ex]
            st['total_rows'] += 1

            last_str = parts[21]
            trade_time_str = parts[23]

            if not last_str or not trade_time_str or trade_time_str == '00:00:00.000':
                continue

            try:
                last_val = float(last_str)
            except ValueError:
                continue

            if last_val <= 0:
                continue

            ts_ms = parse_time_ms(trade_time_str)
            if ts_ms < 0:
                continue

            st['trades'] += 1
            st['sym_trades'][symbol] += 1

            # 1. Pörssitason tarkistus (kaikki osakkeet keskenään)
            if st['max_ts_ms'] != -1 and ts_ms < st['max_ts_ms']:
                st['ooo_trades'] += 1
                delay = st['max_ts_ms'] - ts_ms
                st['delays'].append(delay)
                st['sym_ooo_counts'][symbol] += 1

                if delay > st['max_delay_ms']:
                    st['max_delay_ms'] = delay

                # Jakaumabucketit
                if delay <= 10:
                    st['buckets']['<= 10 ms'] += 1
                elif delay <= 50:
                    st['buckets']['11 - 50 ms'] += 1
                elif delay <= 200:
                    st['buckets']['51 - 200 ms'] += 1
                elif delay <= 1000:
                    st['buckets']['201 - 1000 ms'] += 1
                else:
                    st['buckets']['> 1000 ms'] += 1

                if len(st['examples']) < 3:
                    st['examples'].append({
                        'line': line_count,
                        'symbol': symbol,
                        'trade_time': trade_time_str,
                        'previous_max_time': format_ms(st['max_ts_ms']),
                        'delay_ms': delay
                    })
            else:
                st['max_ts_ms'] = ts_ms

            # 2. Osakekohtainen tarkistus
            sym_prev = st['sym_max_ts'].get(symbol, -1)
            if sym_prev != -1 and ts_ms < sym_prev:
                st['sym_ooo_trades'] += 1
            else:
                st['sym_max_ts'][symbol] = ts_ms

    elapsed = time.perf_counter() - t0
    print(f"Luettu {line_count:,} riviä ajassa {elapsed:.2f} s ({line_count / max(elapsed, 0.001):,.0f} riviä/s)\n")

    report_data = {
        'file': str(csv_file),
        'total_lines_read': line_count,
        'elapsed_seconds': round(elapsed, 2),
        'exchanges': {}
    }

    for ex, st in stats.items():
        name = EXCHANGES.get(ex, ex)
        trades = max(st['trades'], 1)
        total_rows = max(st['total_rows'], 1)
        ooo = st['ooo_trades']
        sym_ooo = st['sym_ooo_trades']

        trade_share_pct = (ooo / trades) * 100
        all_rows_share_pct = (ooo / total_rows) * 100

        total_symbols = len(st['sym_trades'])
        affected_symbols = sum(1 for count in st['sym_ooo_counts'].values() if count > 0)
        symbol_share_pct = (affected_symbols / max(total_symbols, 1)) * 100

        pctiles = calculate_percentiles(st['delays'])

        top_symbols = []
        for sym, count in st['sym_ooo_counts'].most_common(5):
            sym_total = st['sym_trades'].get(sym, 1)
            top_symbols.append({
                'symbol': sym,
                'ooo_count': count,
                'total_trades': sym_total,
                'ooo_pct': round((count / sym_total) * 100, 2)
            })

        ex_summary = {
            'exchange_name': name,
            'exchange_code': ex,
            'total_rows': st['total_rows'],
            'trade_rows': st['trades'],
            'total_symbols': total_symbols,
            'affected_symbols_count': affected_symbols,
            'affected_symbols_share_pct': round(symbol_share_pct, 2),
            'out_of_order_trades': ooo,
            'share_of_trade_rows_pct': round(trade_share_pct, 2),
            'share_of_all_rows_pct': round(all_rows_share_pct, 2),
            'delay_percentiles_ms': pctiles,
            'delay_distribution': st['buckets'],
            'symbol_level_out_of_order': sym_ooo,
            'top_affected_symbols': top_symbols,
            'examples': st['examples']
        }
        report_data['exchanges'][ex] = ex_summary

        print("=" * 65)
        print(f"  PÖRSSI: {name} [.{ex}]")
        print("=" * 65)
        print(f"  Rivejä yhteensä pörssistä:       {st['total_rows']:,}")
        print(f"  Kaupparivejä (Last > 0):         {st['trades']:,}")
        print(f"  Uniikkeja osakkeita pörssissä:   {total_symbols:,}")
        print()
        print(f"  [1] KUINKA ISO OSA ON EPÄSYNKASSA (PÖRSSIN SISÄLLÄ):")
        print(f"  -------------------------------------------------------------")
        print(f"  * Osuus KAUPPARIVEISTÄ:          {trade_share_pct:6.2f} %  ({ooo:,} / {st['trades']:,})")
        print(f"  * Osuus KAIKISTA RIVEISTÄ:       {all_rows_share_pct:6.2f} %  ({ooo:,} / {st['total_rows']:,})")
        print(f"  * Osuus OSAKESYMBOLEISTA:        {symbol_share_pct:6.2f} %  ({affected_symbols:,} / {total_symbols:,} symbolia)")
        print()
        print(f"  [2] VIIVEIDEN JAKAUTUMINEN (kuinka paljon hypätään taaksepäin):")
        print(f"  -------------------------------------------------------------")
        print(f"    Mediaani (p50):                {pctiles['p50_ms']} ms")
        print(f"    90 % persentiili (p90):        {pctiles['p90_ms']} ms")
        print(f"    99 % persentiili (p99):        {pctiles['p99_ms']} ms")
        print(f"    Suurin viive (max):            {pctiles['max_ms']} ms ({pctiles['max_ms']/1000:.3f} s)")
        print()
        print(f"    Viiveen suuruusluokat:")
        for bucket, b_count in st['buckets'].items():
            b_pct = (b_count / max(ooo, 1)) * 100
            bar = "#" * int(b_pct / 5)
            print(f"      {bucket:14s}: {b_count:7,} kpl ({b_pct:5.1f} %)  {bar}")

        if top_symbols:
            print()
            print(f"  [3] Eniten epäsynkassa saapuvat osakkeet:")
            for item in top_symbols:
                print(f"      {item['symbol']:12s}: {item['ooo_count']:,} kpl out-of-order ({item['ooo_pct']} % osakkeen kaupoista)")

        print()
        print(f"  [4] Osakekohtainen tarkistus (saman symbolin omat kaupat):")
        if sym_ooo == 0:
            print(f"      -> 0 kpl epäsynkassa (yksittäisen osakkeen omat kaupat ovat 100 % synkassa)")
        else:
            print(f"      -> {sym_ooo:,} kpl epäsynkassa osakkeen omien kauppojen välillä")
        print()

    # Tallennetaan JSON-raportti
    if not json_out_path:
        stem = csv_file.stem
        json_out_path = f"sync_report_{stem}.json"

    with open(json_out_path, 'w', encoding='utf-8') as jf:
        json.dump(report_data, jf, indent=2, ensure_ascii=False)
    print(f"-> Tulokset otettu ylös ja tallennettu tiedostoon: {json_out_path}")

if __name__ == '__main__':
    args = sys.argv[1:]
    csv_arg = 'data/raw/081121_1m.csv'
    ex_arg = 'ALL'
    json_arg = None

    i = 0
    clean_args = []
    while i < len(args):
        if args[i] == '--json' and i + 1 < len(args):
            json_arg = args[i+1]
            i += 2
        else:
            clean_args.append(args[i])
            i += 1

    if len(clean_args) > 0:
        csv_arg = clean_args[0]
    if len(clean_args) > 1:
        ex_arg = clean_args[1]

    check_sync(csv_arg, ex_arg, json_arg)
