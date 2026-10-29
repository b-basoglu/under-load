# Part 33 lab: replication and consistency

`replication.py` — a PostgreSQL 17 primary (55440) with two streaming replicas r1 (55441) and r2 (55442), created by
`cluster.sh`. Measures: stale reads right after writes on an async replica (quiet and during a bulk import); time until
a write is visible; a long report on r1 with `hot_standby_feedback` off vs on; commit latency and replica staleness for
each `synchronous_commit` level; a synchronous standby cut off; an async replica partitioned, the primary killed and
the replica promoted. The last step destroys the primary — run `cluster.sh` again before re-running.

Results in `replication_results.json`. Last run: 7/7 passed. ~2 min.

```bash
./cluster.sh
pip install "psycopg[binary]"
python3 replication.py
```
