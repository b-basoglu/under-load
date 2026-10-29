# Part 15 lab: retries, backoff and jitter

`retry_sim.py` — a discrete-event simulation (10 ms steps, 90 s, fixed seed): 400 new sends/s against a provider with
600/s capacity that is down from 10–20 s and halves its capacity while overloaded. Compares no retries, immediate
retries, fixed delay, exponential backoff, exponential + full jitter, and jitter + a 10% retry budget, with and without
the overload degradation. Also: 2,000 synchronized failures with and without jitter, and a directly measured count of
layered retries (3 layers × 3 attempts). Results in `retry_sim_results.json`. Pure Python, no dependencies.

```bash
python3 retry_sim.py
```
The model is intentionally simple; change the constants at the top to explore other scenarios.

Last run (after the 2026-10-25 audit): 6/6 passed.
