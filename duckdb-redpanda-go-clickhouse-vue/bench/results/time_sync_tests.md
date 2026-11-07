# Receipt Order vs. Ground Truth Signals (No Allowed Lateness)

**Date:** 2026-11-07  
**Scope:** Monday, tuesday 
**Configuration:** `ORDER BY seq` (file receipt order), `ALLOWED_LATENESS = 0` (late ticks dropped immediately), `EMA_INIT = first_close`, flip crossover rule.



============================================================
  SIGNAL COMPARISON (2021-11-08, 2021-11-09)
============================================================
  Test run:   ..\data\signals_receipt_order_no_delay_allowed.csv
  Reference:  ..\data\reference_signals_week.csv

Total signals in reference (2021-11-08, 2021-11-09): 10,255
Total signals in test run  (2021-11-08, 2021-11-09): 10,255

Results Summary:
  * Exact matches:             10,254 (99.99% of reference)
  * Missing (False Negatives): 1 (0.01%)
  * Spurious (False Positives):1 (0.01%)
  * Inverted side (BUY/SELL):  0
  * Max EMA absolute diff:     0.13321096
  * Bit-for-bit identical EMA: 10,221 / 10,254 (99.68%)

Sample missing signals (False Negatives):
  - PHIA.NL at 2021-11-09 16:40:00 [BUY]

Sample spurious signals (False Positives):
  - PHIA.NL at 2021-11-09 16:45:00 [BUY]

Sample EMA differences:
  - A0C4CT.ETR at 2021-11-08 15:45:00 (SELL): diff=0.00021798 (ref close=1495.88, test close=1495.88)
  - A0C4CT.ETR at 2021-11-09 16:25:00 (BUY): diff=0.00002142 (ref close=1496.21, test close=1496.21)
  - A0SNAL.ETR at 2021-11-08 15:45:00 (SELL): diff=0.00006207 (ref close=151.32, test close=151.32)
  - A0SNAL.ETR at 2021-11-09 16:25:00 (BUY): diff=0.00000610 (ref close=151.46, test close=151.46)
  - A0SNAM.ETR at 2021-11-08 15:45:00 (BUY): diff=0.03327170 (ref close=113195.9, test close=113195.9)

Total divergences: 2
============================================================
  SIGNAL COMPARISON (2021-11-08, 2021-11-09)
============================================================
  Test run:   ..\data\signals_hybrid_1000ms.csv
  Reference:  ..\data\reference_signals_week.csv

Total signals in reference (2021-11-08, 2021-11-09): 10,255
Total signals in test run  (2021-11-08, 2021-11-09): 10,255

Results Summary:
  * Exact matches:             10,255 (100.00% of reference)
  * Missing (False Negatives): 0 (0.00%)
  * Spurious (False Positives):0 (0.00%)
  * Inverted side (BUY/SELL):  0
  * Max EMA absolute diff:     0.00000000
  * Bit-for-bit identical EMA: 10,255 / 10,255 (100.00%)

[OK] Signals for 2021-11-08, 2021-11-09 match the reference 100.00% with exact bit-for-bit EMAs!

Current hybrid approach has watermark for each stock so their windows can be closed immediately. Also windows for illiquid tickers get closed no later than 1000ms after first closing inside the same bucket. Testing shows hybrid synchronizing approach is enough to completety erase the light error which was caused by not allowing lateness at all.