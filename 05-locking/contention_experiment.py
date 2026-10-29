"""Measures optimistic vs pessimistic vs atomic updates under increasing contention on PostgreSQL 17.

Each worker repeatedly 'assigns a ticket' to an agent, i.e. increments that agent's open_count,
with 3 ms of application logic between reading and writing (as a real request would have).
- hot: every worker updates the same agent row
- spread: each operation picks one of 1,000 agents at random
"""
import json
import random
import threading
import time

import psycopg

DURATION = 4.0
THINK = 0.003


def pg():
    return psycopg.connect("host=localhost port=55432 user=postgres password=test", autocommit=True)


def setup():
    with pg() as c:
        c.execute("DROP TABLE IF EXISTS agents")
        c.execute("CREATE TABLE agents (id int PRIMARY KEY, open_count int NOT NULL, version int NOT NULL)")
        c.execute("INSERT INTO agents SELECT g, 0, 1 FROM generate_series(1, 1000) g")


def op_optimistic(c, aid, stats):
    while True:
        n, v = c.execute("SELECT open_count, version FROM agents WHERE id = %s", (aid,)).fetchone()
        time.sleep(THINK)
        if c.execute("UPDATE agents SET open_count = %s, version = version + 1 WHERE id = %s AND version = %s",
                     (n + 1, aid, v)).rowcount == 1:
            return
        stats["retries"] += 1


def op_pessimistic(c, aid, stats):
    with c.transaction():
        n = c.execute("SELECT open_count FROM agents WHERE id = %s FOR UPDATE", (aid,)).fetchone()[0]
        time.sleep(THINK)
        c.execute("UPDATE agents SET open_count = %s WHERE id = %s", (n + 1, aid))


def op_atomic(c, aid, stats):
    time.sleep(THINK)  # the same application work, done before the single statement
    c.execute("UPDATE agents SET open_count = open_count + 1 WHERE id = %s", (aid,))


def run(strategy, workers, hot):
    setup()
    fn = {"optimistic": op_optimistic, "pessimistic": op_pessimistic, "atomic": op_atomic}[strategy]
    stats = [{"ops": 0, "retries": 0, "lat": []} for _ in range(workers)]
    stop = time.time() + DURATION

    def worker(i):
        c = pg()
        rnd = random.Random(i)
        while time.time() < stop:
            aid = 1 if hot else rnd.randint(1, 1000)
            t0 = time.time()
            fn(c, aid, stats[i])
            stats[i]["lat"].append(time.time() - t0)
            stats[i]["ops"] += 1
        c.close()
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in ts: t.start()
    for t in ts: t.join()
    ops = sum(s["ops"] for s in stats)
    retries = sum(s["retries"] for s in stats)
    lat = sorted(x for s in stats for x in s["lat"])
    with pg() as c:
        total = c.execute("SELECT sum(open_count) FROM agents").fetchone()[0]
    return {"strategy": strategy, "workers": workers, "hot": hot,
            "ops_per_s": round(ops / DURATION), "retries_per_op": round(retries / max(ops, 1), 2),
            "p99_ms": round(lat[int(len(lat) * 0.99) - 1] * 1000, 1) if lat else None,
            "lost_updates": ops - total}


if __name__ == "__main__":
    out = []
    for hot in (True, False):
        for workers in (1, 4, 16, 32):
            for strategy in ("atomic", "pessimistic", "optimistic"):
                r = run(strategy, workers, hot)
                out.append(r)
                print(r, flush=True)
    with open(__file__.replace(".py", "_results.json"), "w") as f:
        json.dump(out, f, indent=1)
