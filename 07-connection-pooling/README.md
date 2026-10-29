# Part 7 — Connection pooling lab
Needs Docker network, Postgres with `--cpus=2`, and PgBouncer.
```bash
docker network create ul-net
docker run -d --name ul-pg --cpus=2 --network ul-net -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17 -c max_connections=100
docker run -d --name ul-bouncer --network ul-net -p 56432:5432 \
  -e DATABASE_URL=postgres://postgres:test@ul-pg:5432/postgres -e POOL_MODE=transaction \
  -e DEFAULT_POOL_SIZE=20 -e MAX_CLIENT_CONN=1000 -e AUTH_TYPE=scram-sha-256 edoburu/pgbouncer
pip install 'psycopg[binary]' psycopg_pool
python pool_tests.py
```
Expect `6/6 passed`.
