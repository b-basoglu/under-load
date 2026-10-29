# Part 11 lab: stampede, avalanche, penetration, outage

`cache_failures.py` — PostgreSQL 17 limited to 2 CPUs + Redis 7.4: 200 simultaneous misses on an expensive key (no
protection, rebuild lock with re-check, stale-while-revalidate), 5,000 keys with one TTL vs jittered TTLs, random
nonexistent ids vs negative caching and a Bloom filter, and a Redis outage with an uncapped vs capped fallback.
Results in `cache_failures_results.json`. Last run: 4/4 passed (twice after stronger asserts).

```bash
docker run -d --rm --name ul-pg --cpus=2 -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17 -c max_connections=300
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install "psycopg[binary]" psycopg_pool redis
python3 cache_failures.py
```
