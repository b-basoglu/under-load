# Part 9 lab: cache-aside, races and eviction

`cache_tests.py` — PostgreSQL 17 + Redis 7.4: cheap vs expensive query from DB vs cache, stale data without
invalidation, the two cache races replayed step by step (update-on-write out of order, delete-on-write with a slow
reader), the versioned-write Lua fix with tombstones, negative caching, and `noeviction` vs `allkeys-lru` at maxmemory.
Results in `cache_results.json`. Last run: 8/8 passed.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install "psycopg[binary]" redis
python3 cache_tests.py
```
