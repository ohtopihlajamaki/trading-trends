import sys

if len(sys.argv) < 3:
    print("Usage: python check_sync_minimal.py <EXCHANGE> <CSV_FILE>")
    print("Example: python check_sync_minimal.py ETR data/raw/081121.csv")
    sys.exit(1)

a, b = sys.argv[1], sys.argv[2]
path, ex = (a, b.upper()) if a.lower().endswith('.csv') else (b, a.upper())

def to_ms(s):
    h, m, sec = s.split(':')
    s1, ms = sec.split('.')
    return (int(h) * 3600 + int(m) * 60 + int(s1)) * 1000 + int(ms)

total = max_ms = 0
delays = []

with open(path, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        if line[0] == '#' or line.startswith('ID'): continue
        p = line.split(',', 24)
        if len(p) > 23 and p[0].endswith('.' + ex) and p[21] and p[23] and p[23] != '00:00:00.000':
            try:
                if float(p[21]) <= 0: continue
            except ValueError:
                continue
            t = to_ms(p[23])
            total += 1
            if max_ms and t < max_ms:
                delays.append(max_ms - t)
            else:
                max_ms = max(max_ms, t)

ooo = len(delays)
pct = (ooo / total * 100) if total else 0
delays.sort()
median = delays[ooo // 2] if ooo else 0
mean = (sum(delays) / ooo) if ooo else 0
max_jump = delays[-1] if ooo else 0

print(f"Exchange: {ex} | File: {path}")
print(f"Total trades:        {total:,}")
print(f"Out of order:        {ooo:,} ({pct:.2f} %)")
print(f"  Mean jump:         {mean:.1f} ms ({mean / 1000:.3f} s)")
print(f"  Median jump:       {median} ms ({median / 1000:.3f} s)")
print(f"  Max jump backward: {max_jump} ms ({max_jump / 1000:.3f} s)")
