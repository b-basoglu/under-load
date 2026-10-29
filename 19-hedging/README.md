# Part 19 lab: request hedging

`hedging.py` — discrete-event simulation: 3 read replicas (FIFO, lognormal service time, median 20 ms), each pausing
~0.1×/s for 300 ms. Compares no hedging, hedging every request immediately, hedging after the measured P95, and the same
with a 5% token-bucket budget, at moderate (~40%) and high (~80%) utilization, plus an aggressive fixed 30 ms delay at
high load. The losing copy is dropped if it hasn't started. Results in `hedging_results.json`. Last run: 4/4 passed.

```bash
python3 hedging.py
```
