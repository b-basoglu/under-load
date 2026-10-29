# Part 40 lab: leader election

`election.py` — etcd 3.5 (leases and compare-and-swap through its JSON gateway) + PostgreSQL 17. Three scheduler
instances (threads) and a job that should run every 100 ms on one instance; each run writes 5 rows with the instance
and its epoch (the etcd revision at which it won). Scenarios: no election; lease election; leader crash with TTL
2/5/10 s; a 3 s pause on a healthy leader with TTL 2 vs 5 s; a leader frozen mid-job for 4 s (TTL 2 s) with and
without an epoch fence in the database.

Results in `election_results.json`. Last run: 6/6 passed. ~2 min.

```bash
docker run -d --rm --name ul-etcd -p 52379:2379 quay.io/coreos/etcd:v3.5.17 etcd \
  --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install "psycopg[binary]"
python3 election.py
```
