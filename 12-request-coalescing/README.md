# Part 12 lab: request coalescing

`coalescing.py` — a ~30-line single-flight implementation against PostgreSQL 17 limited to 2 CPUs: 100 concurrent
identical requests with and without it, 4 servers with process-local single-flight, a failing leader (with and without
stale fallback), a slow leader (with and without a follower deadline), a key that's too coarse, and non-overlapping
requests. Results in `coalescing_results.json`. Last run: 6/6 passed (twice after stronger asserts).
Needs Part 11 tables (`tickets` / `comments`) and `max_connections` ≥ pool size (120).

```bash
docker run -d --rm --name ul-pg --cpus=2 -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17 -c max_connections=300
pip install "psycopg[binary]" psycopg_pool
python3 ../../11-cache-failures/lab/cache_failures.py   # creates the tables (or run its setup())
python3 coalescing.py
```
