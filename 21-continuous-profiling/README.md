# Part 21 lab: continuous profiling

`profiling.py` — a simplified ticket handler (load, PII masking, render, JSON) with a v2 "regression", and a 20-line
sampling profiler (`sys._current_frames()` from a background thread). Measures sampler overhead at 19/99/999 Hz, CPU per
request for v1 vs v2, and compares the two profiles both as shares of samples and as CPU ms per request.
Results in `profiling_results.json`. Last run (after the 2026-10-25 audit): 3/3 passed. Pure Python; about 50 seconds.

```bash
python3 profiling.py
```
