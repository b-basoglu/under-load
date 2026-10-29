# Part 6 — WAL lab
Starts its own PostgreSQL 17 container. Needs Docker.
```bash
pip install 'psycopg[binary]'
python wal_tests.py
```
Expect `8/8 passed`.
