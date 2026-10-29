# Part 16 lab: circuit breakers

`breaker.py` — real threads, wall-clock time: a 20-worker ticket service at 100 req/s calling a simulated notification
service that hangs from 5–15 s. Compares no breaker, a breaker with a 2 s timeout and one with a 300 ms timeout; also a
healthy service with 8% random errors under an aggressive vs a reasonable threshold. Results in `breaker_results.json`.
Last run (after the 2026-10-25 audit): 5/5 passed. Pure Python, no dependencies; takes about two minutes.

```bash
python3 breaker.py
```
