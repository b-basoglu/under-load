"""Sharding and hot partitions, measured for Part 35.

Four PostgreSQL 17 shards (ports 55450–55453), each served by a pool of 2 connections; each write holds a connection
for ~4 ms plus overhead, which caps each shard (measured first, ~280 writes/s); offered load = 75% of the cluster. Writes: ticket events from 50 tenants; Halcyon sends 20% of all traffic. Offered load is fixed
(open loop); latency includes time waiting for a connection, so an overloaded shard shows up as queueing.
"""
import hashlib
import json
import queue
import random
import threading
import time

import psycopg

SHARDS = [f"host=localhost port={55450 + i} user=postgres password=test dbname=postgres" for i in range(4)]
results, data = [], {}
HALCYON = 1


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else 0


def h(s):
    return int(hashlib.md5(str(s).encode()).hexdigest(), 16)


def setup():
    for d in SHARDS:
        with psycopg.connect(d, autocommit=True) as c:
            c.execute("DROP TABLE IF EXISTS ticket_events")
            c.execute("""CREATE TABLE ticket_events (id bigserial PRIMARY KEY, tenant_id int, ticket_id bigint, bucket int,
                         kind text, body text, created_at timestamptz DEFAULT now())""")
            c.execute("CREATE INDEX ON ticket_events (tenant_id, created_at DESC)")
            c.execute("CREATE INDEX ON ticket_events (ticket_id)")


def tenant_mix(rnd):
    """Halcyon 20%; the other 49 tenants share the rest with a long tail."""
    if rnd.random() < 0.20:
        return HALCYON
    return rnd.choices(range(2, 51), weights=[1 / (r ** 0.5) for r in range(1, 50)])[0]


WORK = "WITH w AS (SELECT pg_sleep(0.004)) INSERT INTO ticket_events (tenant_id, ticket_id, bucket, kind, body) SELECT %s, %s, %s, 'reply', %s FROM w"

KEYS = {
    "tenant": lambda t, tk, month: h(t) % 4,
    "ticket": lambda t, tk, month: h(tk) % 4,
    "tenant+bucket": lambda t, tk, month: h((t, tk % 8)) % 4 if t == HALCYON else h(t) % 4,   # only the hot tenant is split; others stay put
    "month": lambda t, tk, month: month % 4,                                           # range by time: this month = one shard
}


def run(key, rate, secs=15, seed=1, pool=2):
    rnd = random.Random(seed)
    qs = [queue.Queue() for _ in SHARDS]
    lat = [[] for _ in SHARDS]
    done = [0] * 4
    stop = threading.Event()
    lock = threading.Lock()

    def worker(i):
        with psycopg.connect(SHARDS[i], autocommit=True) as c:
            while True:
                item = qs[i].get()
                if item is None:
                    return
                t_arr, t, tk = item
                c.execute(WORK, (t, tk, tk % 8, "x" * 400))     # an insert that holds its connection ~4 ms (work, I/O, triggers)
                with lock:
                    lat[i].append((time.perf_counter() - t_arr) * 1000)
                    done[i] += 1
    ws = [threading.Thread(target=worker, args=(i,)) for i in range(4) for _ in range(pool)]
    for w in ws: w.start()
    offered = [0] * 4
    t0 = time.perf_counter()
    n = 0
    month = 10
    while time.perf_counter() - t0 < secs:
        target = int((time.perf_counter() - t0) * rate)
        while n < target:
            t = tenant_mix(rnd)
            tk = t * 1_000_000 + rnd.randint(1, 200_000)
            s = KEYS[key](t, tk, month)
            offered[s] += 1
            qs[s].put((time.perf_counter(), t, tk))
            n += 1
        time.sleep(0.001)
    served_in_window = list(done)
    backlog = [q.qsize() for q in qs]
    for q in qs:
        while not q.empty():
            try: q.get_nowait()
            except queue.Empty: break
        for _ in range(pool):
            q.put(None)
    for w in ws: w.join()
    tot = sum(offered)
    r = {"key": key, "rate": rate, "share_pct": [round(100 * o / tot, 1) for o in offered],
         "served_per_s": [round(s / secs) for s in served_in_window], "backlog": backlog,
         "p99_ms": [round(pct(l, .99), 1) for l in lat], "p50_ms": [round(pct(l, .5), 1) for l in lat],
         "overall_p99_ms": round(pct(sum(lat, []), .99), 1)}
    print(r, flush=True)
    return r


def capacity():
    """Max inserts/s one shard sustains with its 2 connections (closed loop, 5 s)."""
    stop = time.perf_counter() + 5
    cnt = [0]
    lock = threading.Lock()

    def w():
        with psycopg.connect(SHARDS[0], autocommit=True) as c:
            while time.perf_counter() < stop:
                c.execute(WORK, (1, 1, 1, "x" * 400))
                with lock:
                    cnt[0] += 1
    ts = [threading.Thread(target=w) for _ in range(2)]
    for t in ts: t.start()
    for t in ts: t.join()
    return round(cnt[0] / 5)


def scatter(n=300):
    """'Halcyon's latest 50 events': one shard when sharded by tenant, all four when sharded by ticket."""
    conns = [psycopg.connect(d, autocommit=True) for d in SHARDS]
    sql = "SELECT id, ticket_id, created_at FROM ticket_events WHERE tenant_id = %s ORDER BY created_at DESC LIMIT 50"
    one, fan = [], []
    pool = [threading.Thread] * 4
    for _ in range(n):
        t0 = time.perf_counter()
        conns[h(HALCYON) % 4].execute(sql, (HALCYON,)).fetchall()
        one.append((time.perf_counter() - t0) * 1000)
        t0 = time.perf_counter()
        res = [None] * 4

        def go(i):
            res[i] = conns[i].execute(sql, (HALCYON,)).fetchall()
        # each shard has its own connection; query them in parallel, then merge
        ts = [threading.Thread(target=go, args=(i,)) for i in range(4)]
        for t in ts: t.start()
        for t in ts: t.join()
        merged = sorted(sum(res, []), key=lambda r: r[2], reverse=True)[:50]
        fan.append((time.perf_counter() - t0) * 1000)
    for c in conns:
        c.close()
    return {"one_shard_p50_ms": round(pct(one, .5), 2), "one_shard_p99_ms": round(pct(one, .99), 2),
            "all_shards_p50_ms": round(pct(fan, .5), 2), "all_shards_p99_ms": round(pct(fan, .99), 2)}


if __name__ == "__main__":
    setup()
    cap = capacity()
    setup()
    rate = int(cap * 3.0)                     # 75% of the cluster total capacity
    print("capacity per shard", cap, "offered", rate, flush=True)
    R = {k: run(k, rate) for k in KEYS}
    S = scatter()
    print(S, flush=True)
    rows = []
    for d in SHARDS:
        with psycopg.connect(d) as c:
            rows.append(c.execute("SELECT count(DISTINCT tenant_id) FROM ticket_events").fetchone()[0])
    data.update({"capacity_per_shard": cap, "offered_per_s": rate, "runs": R, "scatter": S, "tenants_per_shard": rows})
    t, tk, tb, m = R["tenant"], R["ticket"], R["tenant+bucket"], R["month"]
    hot = max(range(4), key=lambda i: t["share_pct"][i])
    report("Shard by tenant", "Halcyon's shard gets far more than a quarter of the traffic and saturates while others idle",
           f"traffic share per shard {t['share_pct']}%; p99 per shard {t['p99_ms']} ms; backlog {t['backlog']}",
           t["share_pct"][hot] > 35 and t["p99_ms"][hot] > 10 * min(x for x in t["p99_ms"] if x > 0) and max(t["backlog"]) > 100)
    report("Shard by ticket id", "traffic spreads evenly", f"share {tk['share_pct']}%; p99 {tk['p99_ms']} ms",
           max(tk["share_pct"]) < 30 and max(tk["p99_ms"]) < t["p99_ms"][hot] / 5 and max(tk["backlog"]) <= 5)
    report("…but a tenant's query must ask every shard", "fan-out costs latency on every tenant page",
           f"Halcyon's latest 50: one shard p50 {S['one_shard_p50_ms']} ms / p99 {S['one_shard_p99_ms']} ms; all four p50 {S['all_shards_p50_ms']} ms / p99 {S['all_shards_p99_ms']} ms",
           S["all_shards_p99_ms"] > S["one_shard_p99_ms"] * 1.5)
    report("Tenant key, hot tenant split into 8 buckets", "Halcyon spreads out; every other tenant still lives on one shard",
           f"share {tb['share_pct']}%; p99 {tb['p99_ms']} ms", max(tb["share_pct"]) < max(t["share_pct"]) - 5 and max(tb["p99_ms"]) < t["p99_ms"][hot] / 5)
    report("Range by month", "every write lands on the current month's shard (by construction: all writes happen this month)", f"share {m['share_pct']}%; p99 {m['p99_ms']} ms",
           max(m["share_pct"]) == 100.0 and m["p99_ms"][m["share_pct"].index(100.0)] > t["p99_ms"][hot])
    with open(__file__.replace("sharding.py", "sharding_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
