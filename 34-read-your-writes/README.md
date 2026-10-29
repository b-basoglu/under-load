# Part 34 lab: read-your-writes

`ryw.py` — PostgreSQL 17 primary + two streaming replicas (`../../33-replication/lab/cluster.sh`). r2 gets
`recovery_min_apply_delay = 2s`; a bulk import loads r1. 20 agent sessions rename themselves and read their profile
(immediately, after 300 ms, after 1.5 s, and while browsing). Routing strategies: round-robin replicas, primary for 1 s
after a write, sticky replica per user, LSN token (replay positions polled every 5 ms, wait ≤ 50 ms, then primary),
and primary-only. Measures stale reads, reads that went backwards, primary share and read latency.

Results in `ryw_results.json`. Last run: 5/5 passed. ~2 min.

```bash
../../33-replication/lab/cluster.sh
pip install "psycopg[binary]"
python3 ryw.py
```
