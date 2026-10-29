# Part 3 — MVCC lab
```bash
docker run -d --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17 -c autovacuum=off
docker run -d --name ul-mysql -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=test -p 53306:3306 mysql:8.4
pip install 'psycopg[binary]' pymysql
python mvcc_tests.py && python bloat_experiment.py
```
Expect `11/11 passed` from mvcc_tests.
