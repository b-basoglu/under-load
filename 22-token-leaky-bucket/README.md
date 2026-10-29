# Part 22 lab: token bucket vs leaky bucket

`buckets.py` — deterministic comparison of fixed window, sliding window (log), token bucket (capacity 10 and 100) and a
leaky-bucket queue (drain 10/s, queue 100), all at 10 req/s, on four traffic patterns: steady 8/s, a 300-request burst
once a minute, a window-boundary burst, and a constant 50/s client. Results in `buckets_results.json`.
Last run (after the 2026-10-25 audit): 5/5 passed. Pure Python; under a second.

```bash
python3 buckets.py
```
