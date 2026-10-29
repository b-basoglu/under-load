# Part 37 lab: partition rebalancing

`rebalance.py` — two PostgreSQL 17 shards (1 CPU each): ul-src (55460) and ul-dst (55461). Tenant 7 (300,000 tickets)
moves from src to dst under 800 ops/s of live traffic (70% reads, 30% updates, 25% on the moving tenant). A trigger on
src records changes to the moving tenant and implements the fence (rejects writes once the tenant is marked moved).
Measures: foreground p99 during an unthrottled vs throttled copy; copy-then-switch vs copy + catch-up + fence + switch;
clients with 2 s stale routing with and without the fence; a killed and resumed copy.

Results in `rebalance_results.json`. Last run (after the 2026-10-25 audit): 7/7 passed. ~3 min.

```bash
docker run -d --rm --name ul-src --cpus=1 -p 55460:5432 -e POSTGRES_PASSWORD=test postgres:17
docker run -d --rm --name ul-dst --cpus=1 -p 55461:5432 -e POSTGRES_PASSWORD=test postgres:17
pip install "psycopg[binary]"
python3 rebalance.py
```
