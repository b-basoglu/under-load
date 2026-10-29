# Part 35 lab: sharding and hot partitions

`sharding.py` — four PostgreSQL 17 shards (containers on ports 55450–55453), each served by a pool of 2 connections;
every write holds a connection ~4 ms, which caps a shard at ~280 writes/s (measured first). 50 tenants, Halcyon = 20% of
writes; offered load = 75% of the cluster's capacity for 15 s per shard key: tenant hash, ticket-id hash, tenant hash
with the hot tenant split into 8 buckets, and range by month. Also measures a one-shard vs four-shard (scatter-gather)
query for a tenant's latest 50 events.

Results in `sharding_results.json`. Last run (after the 2026-10-25 audit): 5/5 passed. ~2 min.

```bash
for i in 0 1 2 3; do docker run -d --rm --name ul-shard$i -p 5545$i:5432 -e POSTGRES_PASSWORD=test postgres:17 -c synchronous_commit=off; done
pip install "psycopg[binary]"
python3 sharding.py
```
