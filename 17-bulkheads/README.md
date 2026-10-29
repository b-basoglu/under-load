# Part 17 lab: bulkheads

`bulkhead.py` — real threads, wall-clock time: 20 workers serving 100 ticket pages/s (30 ms) plus an export burst of
5/s (4 s each) from 5–15 s, with one shared pool vs an export bulkhead of 4 slots; plus a quiet day with a bulkhead of 1
vs the shared pool. Results in `bulkhead_results.json`. Last run: 3/3 passed. Pure Python; about 80 seconds.

```bash
python3 bulkhead.py
```
