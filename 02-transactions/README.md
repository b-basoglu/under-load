# Part 2 — Transactions lab

Companion lab for **Under Load** Part 2 and Part 2.1.

```bash
docker run -d --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
docker run -d --name ul-mysql -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=test -p 53306:3306 mysql:8.4
pip install 'psycopg[binary]' pymysql
python race_tests.py
```

Expect `18/18 passed`.
