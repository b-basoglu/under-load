# Part 23 lab: distributed rate limiting

`distributed.py` — Redis 7.4; 10 API servers simulated as threads; one client sending 200 req/s for 5 s against a
20 req/s limit. Compares per-server buckets, a shared token bucket via an atomic Lua script, the same bucket with
read-then-write from the application, a 1/N split quota (even vs sticky routing), and store-down behavior (fail-open,
fail-closed, local fallback). Results in `distributed_results.json`. Last run (after the 2026-10-25 audit): 6/6 passed.

```bash
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install redis
python3 distributed.py
```
