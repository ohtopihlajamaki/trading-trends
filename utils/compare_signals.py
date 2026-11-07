#!/usr/bin/env python3
"""
Signal Comparison Tool: compares test run signals against ground truth reference.

Reports:
 - Exact matching signals (count and %)
 - Missing signals (False Negatives: existed in reference, missing in test)
 - Spurious signals (False Positives: generated in test, did not exist in reference)
 - Directional flips (BUY -> SELL or vice versa)
 - Maximum EMA differences for matched signals

Usage:
    python utils/compare_signals.py <test_run.csv> [reference.csv]

Example:
    python utils/compare_signals.py data/signals_receipt_order_no_delay_allowed.csv
"""

import sys
import os
import csv
from pathlib import Path

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Repository root directory (trading-trends)
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REF = REPO_ROOT / "data" / "reference_signals_week.csv"

def resolve_path(p_str: str) -> Path:
    p = Path(p_str)
    if p.exists():
        return p
    cleaned = p_str.lstrip("/\\")
    candidates = [
        REPO_ROOT / cleaned,
        Path("..") / cleaned,
        REPO_ROOT / p_str,
        Path(p_str)
    ]
    for c in candidates:
        if c.exists():
            return c
    return p

def load_signals(path: Path):
    if not path.exists():
        print(f"Error: File not found: {path}", file=sys.stderr)
        sys.exit(1)
        
    signals = {}
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["symbol"], row["window_start"])
            signals[key] = {
                "signal": row["signal"],
                "window_end": row.get("window_end", ""),
                "close": float(row.get("close", 0.0) or 0.0),
                "ema38": float(row.get("ema38", 0.0) or 0.0),
                "ema100": float(row.get("ema100", 0.0) or 0.0),
            }
    return signals

def compare(test_path: Path, ref_path: Path):
    test_path = resolve_path(str(test_path))
    ref_path = resolve_path(str(ref_path))

    test_display = os.path.relpath(test_path)
    ref_display = os.path.relpath(ref_path)

    print("=" * 60)
    print("  SIGNAL COMPARISON")
    print("=" * 60)
    print(f"  Test run:   {test_display}")
    print(f"  Reference:  {ref_display}\n")
    
    test_signals = load_signals(test_path)
    ref_signals = load_signals(ref_path)
    
    n_test = len(test_signals)
    n_ref = len(ref_signals)
    
    all_keys = set(test_signals.keys()).union(set(ref_signals.keys()))
    
    exact_matches = 0
    side_mismatches = []
    missing_in_test = []
    extra_in_test = []
    
    max_ema_diff = 0.0
    
    for key in sorted(all_keys):
        in_ref = key in ref_signals
        in_test = key in test_signals
        
        if in_ref and in_test:
            ref_item = ref_signals[key]
            test_item = test_signals[key]
            
            if ref_item["signal"] == test_item["signal"]:
                exact_matches += 1
                diff_38 = abs(ref_item["ema38"] - test_item["ema38"])
                diff_100 = abs(ref_item["ema100"] - test_item["ema100"])
                max_ema_diff = max(max_ema_diff, diff_38, diff_100)
            else:
                side_mismatches.append((key, ref_item["signal"], test_item["signal"]))
        elif in_ref and not in_test:
            missing_in_test.append((key, ref_signals[key]["signal"]))
        else:
            extra_in_test.append((key, test_signals[key]["signal"]))
            
    print(f"Total signals in reference: {n_ref:,}")
    print(f"Total signals in test run:  {n_test:,}\n")
    
    match_pct = (exact_matches / n_ref * 100) if n_ref else 0
    print("Results Summary:")
    print(f"  * Exact matches:             {exact_matches:,} ({match_pct:.2f}% of reference)")
    print(f"  * Missing (False Negatives): {len(missing_in_test):,} ({len(missing_in_test) / max(n_ref, 1):.2%})")
    print(f"  * Spurious (False Positives):{len(extra_in_test):,} ({len(extra_in_test) / max(n_ref, 1):.2%})")
    print(f"  * Inverted side (BUY/SELL):  {len(side_mismatches):,}")
    print(f"  * Max EMA absolute diff:     {max_ema_diff:.8f}")
    print()
    
    if side_mismatches:
        print("Sample directional mismatches:")
        for (sym, w_start), ref_s, test_s in side_mismatches[:5]:
            print(f"  - {sym} at {w_start}: Ref was {ref_s}, test run produced {test_s}")
        print()
        
    if missing_in_test:
        print("Sample missing signals (False Negatives):")
        for (sym, w_start), s in missing_in_test[:5]:
            print(f"  - {sym} at {w_start} [{s}]")
        print()

    if extra_in_test:
        print("Sample spurious signals (False Positives):")
        for (sym, w_start), s in extra_in_test[:5]:
            print(f"  - {sym} at {w_start} [{s}]")
        print()
        
    total_diffs = len(missing_in_test) + len(extra_in_test) + len(side_mismatches)
    if exact_matches == n_ref and total_diffs == 0:
        print("[OK] Test run signals match the reference 100.00%!")
    else:
        print(f"Total divergences: {total_diffs:,}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python utils/compare_signals.py <test_run.csv> [reference.csv]")
        sys.exit(1)
        
    test_arg = sys.argv[1]
    ref_arg = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_REF
    compare(Path(test_arg), Path(ref_arg))
