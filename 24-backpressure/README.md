# Part 24 lab: backpressure

`backpressure.py` — real threads: a reader pulling up to 500 msg/s from a simulated broker into an in-memory queue,
4 sender threads at 20 ms per send. Compares an unbounded queue, a bounded queue (200) that blocks the reader, and a
bounded queue that rejects (nacks) when full. Results in `backpressure_results.json`. Last run (after the 2026-10-25 audit): 4/4 passed. ~50 s.

```bash
python3 backpressure.py
```
