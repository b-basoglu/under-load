# Part 18 lab: tail latency

`tail.py` — Monte Carlo with a latency model (lognormal body around 40 ms + 1% slow path of 0.8–1.5 s) and a
single-server queue simulation: same mean/different P99, fan-out 1–100, sum of P99s vs real P99, averaging per-instance
P99s vs the true fleet P99, P99.9 stability by sample size, and P99 vs utilization. Fixed seeds; pure Python; ~6 s.
Results in `tail_results.json`. Last run (after the 2026-10-25 audit): 7/7 passed.

```bash
python3 tail.py
```
