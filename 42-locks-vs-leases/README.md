# Part 42 lab: locks vs leases

`locks.py` — Redis 7.4 holds per-tenant locks; PostgreSQL 17 stores invoices. Three workers issue invoices for 20
tenants (check → billing call → write). Scenarios: lock without expiry with a crashed holder; lease (TTL 2 s) with a
crashed holder; a holder paused 3 s past the TTL; the same with a plain DEL release (and slow peers) vs compare-and-delete;
fencing tokens (INCR per grant) checked by PostgreSQL; a renewal watchdog.

Results in `locks_results.json`. Last run: 6/6 passed. ~1 min.

```bash
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install "psycopg[binary]" redis
python3 locks.py
```
