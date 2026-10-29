# Part 41 lab: split brain

`split.py` — two "data centers": DC-A = PostgreSQL ul-pga (55470) + etcd e1 (52381); DC-B = PostgreSQL ul-pgb (55471)
+ etcd e2, e3 (52382, 52383). Containers talk over the `ul-cluster` network; the partition disconnects DC-A's
containers from it (host ports keep working, so clients in each DC keep writing to their local database).
Scenarios: heartbeat failover with no quorum; etcd writes on the minority vs majority side; a quorum lease for the
primary role without and with self-fencing (read-only when the lease can't be renewed within TTL/3).

Results in `split_results.json`. Last run: 4/4 passed. ~1 min.

```bash
docker network create ul-cluster
CL="e1=http://ul-e1:2380,e2=http://ul-e2:2380,e3=http://ul-e3:2380"
for i in 1 2 3; do
  docker run -d --name ul-e$i -p 5238$i:2379 quay.io/coreos/etcd:v3.5.17 etcd --name e$i \
    --initial-advertise-peer-urls http://ul-e$i:2380 --listen-peer-urls http://0.0.0.0:2380 \
    --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379 \
    --initial-cluster $CL --initial-cluster-state new --heartbeat-interval 100 --election-timeout 1000
  docker network connect ul-cluster ul-e$i
done
docker run -d --name ul-pga -p 55470:5432 -e POSTGRES_PASSWORD=test postgres:17 && docker network connect ul-cluster ul-pga
docker run -d --name ul-pgb -p 55471:5432 -e POSTGRES_PASSWORD=test postgres:17 && docker network connect ul-cluster ul-pgb
pip install "psycopg[binary]"
python3 split.py
```
