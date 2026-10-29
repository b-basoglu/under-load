# Part 13 lab: cache coherence across layers

`coherence.py` — 4 (and 8) simulated app servers, each with an in-process cache in front of Redis 7.4 and PostgreSQL 17:
stale window with TTL only vs pub/sub invalidation, servers disagreeing when cached at different moments, a missed
pub/sub message, out-of-order update messages with and without versions, and invalidation propagation latency.
Results in `coherence_results.json`. Last run: 5/5 passed.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install "psycopg[binary]" redis
python3 coherence.py
```
