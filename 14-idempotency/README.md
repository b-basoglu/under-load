# Part 14 lab: idempotency

`idempotency.py` — PostgreSQL 17: lost response + retry with and without a key, a new key per retry, same key with a
different body, two concurrent requests with the same key (check-then-insert vs `INSERT … ON CONFLICT`), a crash between
the side effect and saving the key (separate steps vs one transaction), and a simulated payment provider called with and
without a forwarded key. Results in `idempotency_results.json`. Last run: 7/7 passed.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install "psycopg[binary]"
python3 idempotency.py
```
