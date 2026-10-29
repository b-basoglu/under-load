# Part 1 — Database indexes lab

Companion lab for **Under Load** Part 1 (*Database Indexes & the Query Planner*) and Part 1.1 (measured results).

## Requirements

- Docker: PostgreSQL 17 as `ul-pg` on port `55432`  
  (`postgres` / `test`)
- Python 3 + `psycopg` 3

```bash
docker run -d --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install 'psycopg[binary]'
python indexes.py
```

Expect `16/16 passed` in about 4–5 minutes. The script creates database `ul_indexes`, loads 5M tickets, and prints `[PASS]` / `[FAIL]` for each claim.
