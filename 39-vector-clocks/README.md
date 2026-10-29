# Part 39 lab: vector clocks

`vclock.py` — deterministic model of three replicas (web, mobile, region2) that accept edits to 200 tickets and sync
only occasionally; the mobile clock runs 2 s slow. Every edit adds one tag and has an id, so ground truth is exact.
Compares last-write-wins on wall-clock time, last-write-wins on Lamport clocks, and vector clocks with siblings merged
on read (tag union). Also checks the vector comparison rule against true history inclusion on 20,000 random pairs,
and computes per-writer vector size.

Results in `vclock_results.json`. Last run: 5/5 passed. ~10 s. Pure Python, no dependencies.

```bash
python3 vclock.py
```
