# Part 25 lab: load shedding

`shedding.py` — seeded discrete-event simulation: 8 workers × ~20 ms per request (capacity ≈ 400 req/s), traffic
jumping from 300 to 1,200 req/s for 20 s, 30% interactive / 70% bulk, clients giving up after 1 s, 0.5 ms CPU per
rejection. Compares accept-everything, a queue-length limit, a queue-age drop, priority-aware shedding, immediate
retries vs a 10% retry budget, and length vs age limits when work gets 3× slower. Results in `shedding_results.json`.
Last run: 7/7 passed. ~10 s.

```bash
python3 shedding.py
```
