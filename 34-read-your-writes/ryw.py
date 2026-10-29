"""Read-your-writes, measured for Part 34.

PostgreSQL 17 primary + two streaming replicas (../../33-replication/lab/cluster.sh). r2 is made a slow replica with
recovery_min_apply_delay = 2s; a bulk import keeps r1 under load. 20 agent sessions repeatedly: rename themselves
(write on the primary), then read their profile right away, after 300 ms, and after 1.5 s. Each read is routed by
one of five strategies.
"""
import json
import random
import threading
import time
import zlib

import psycopg

P = "host=localhost port=55440 user=postgres password=test dbname=postgres"
REPLICAS = {"r1": "host=localhost port=55441 user=postgres password=test dbname=postgres",
            "r2": "host=localhost port=55442 user=postgres password=test dbname=postgres"}
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def q(dsn, sql, *a):
    with psycopg.connect(dsn, autocommit=True) as c:
        cur = c.execute(sql, a or None)
        return cur.fetchall() if cur.description else None


class Router:
    """Keeps each replica's applied WAL position, polled every 5 ms — what a proxy or driver does."""
    def __init__(self):
        self.pos, self.stop = {}, threading.Event()
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()
        time.sleep(0.3)

    def run(self):
        conns = {n: psycopg.connect(d, autocommit=True) for n, d in REPLICAS.items()}
        while not self.stop.is_set():
            for n, c in conns.items():
                self.pos[n] = int(c.execute("SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0')").fetchone()[0])
            time.sleep(0.005)


def session(k, strategy, router, cycles, out, lock):
    rnd = random.Random(k)
    uid = k + 1
    conns = {"primary": psycopg.connect(P, autocommit=True), **{n: psycopg.connect(d, autocommit=True) for n, d in REPLICAS.items()}}
    sticky = "r1" if zlib.crc32(str(uid).encode()) % 2 == 0 else "r2"
    last_write_at, token, written, seen_max = 0.0, 0, 0, 0
    stats = {"reads": 0, "stale": 0, "backwards": 0, "primary": 0, "lat": [], "waits": 0}

    def route():
        nonlocal token
        if strategy == "round-robin":
            return rnd.choice(["r1", "r2"])
        if strategy == "primary-for-1s":
            return "primary" if time.time() - last_write_at < 1.0 else rnd.choice(["r1", "r2"])
        if strategy == "sticky-replica":
            return sticky
        if strategy == "lsn-token":
            ok = [n for n in ("r1", "r2") if router.pos.get(n, 0) >= token]
            if ok:
                return rnd.choice(ok)
            t0 = time.time()                       # wait briefly for a replica to catch up, then fall back to the primary
            while time.time() - t0 < 0.05:
                ok = [n for n in ("r1", "r2") if router.pos.get(n, 0) >= token]
                if ok:
                    stats["waits"] += 1
                    return rnd.choice(ok)
                time.sleep(0.002)
            return "primary"
        return "primary"

    def read():
        nonlocal seen_max
        t0 = time.perf_counter()
        n = route()
        v = conns[n].execute("SELECT v FROM profiles WHERE id = %s", (uid,)).fetchone()[0]
        stats["lat"].append((time.perf_counter() - t0) * 1000)
        stats["reads"] += 1
        stats["primary"] += n == "primary"
        stats["stale"] += v < written
        stats["backwards"] += v < seen_max
        seen_max = max(seen_max, v)

    for _ in range(cycles):
        written = conns["primary"].execute("UPDATE profiles SET v = v + 1, display_name = 'renamed' WHERE id = %s RETURNING v", (uid,)).fetchone()[0]
        token = int(conns["primary"].execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')").fetchone()[0])   # the session's token
        last_write_at = time.time()
        read()
        time.sleep(0.3)
        read()
        time.sleep(1.2)
        read()
        for _ in range(rnd.randint(0, 3)):         # browsing between edits; a token-less strategy may hop replicas
            time.sleep(0.2)
            read()
    with lock:
        for key in ("reads", "stale", "backwards", "primary", "waits"):
            out[key] = out.get(key, 0) + stats[key]
        out.setdefault("lat", []).extend(stats["lat"])
    for c in conns.values():
        c.close()


def run(strategy, router, sessions=20, cycles=10):
    out, lock = {}, threading.Lock()
    ts = [threading.Thread(target=session, args=(k, strategy, router, cycles, out, lock)) for k in range(sessions)]
    for t in ts: t.start()
    for t in ts: t.join()
    lat = out.pop("lat")
    r = {"strategy": strategy, "reads": out["reads"], "stale_pct": round(100 * out["stale"] / out["reads"], 1),
         "backwards_pct": round(100 * out["backwards"] / out["reads"], 1), "primary_reads_pct": round(100 * out["primary"] / out["reads"], 1),
         "read_p50_ms": round(pct(lat, .5), 2), "read_p99_ms": round(pct(lat, .99), 1), "waited_for_replica": out["waits"]}
    print(r, flush=True)
    return r


if __name__ == "__main__":
    q(P, "DROP TABLE IF EXISTS profiles, bulk")
    q(P, "CREATE TABLE profiles (id int PRIMARY KEY, display_name text, v int)")
    q(P, "INSERT INTO profiles SELECT g, 'agent ' || g, 0 FROM generate_series(1, 100) g")
    q(P, "CREATE TABLE bulk (id bigserial, body text)")
    q(REPLICAS["r2"], "ALTER SYSTEM SET recovery_min_apply_delay = '2s'")
    q(REPLICAS["r2"], "SELECT pg_reload_conf()")
    time.sleep(3)
    stop = threading.Event()

    def bulk():
        with psycopg.connect(P, autocommit=True) as c:
            while not stop.is_set():
                c.execute("INSERT INTO bulk (body) SELECT repeat('x', 300) FROM generate_series(1, 2000)")
                time.sleep(0.05)
    bt = threading.Thread(target=bulk)
    bt.start()
    router = Router()
    R = {s: run(s, router) for s in ("round-robin", "primary-for-1s", "sticky-replica", "lsn-token", "primary-only")}
    stop.set()
    bt.join()
    router.stop.set()
    q(REPLICAS["r2"], "ALTER SYSTEM RESET recovery_min_apply_delay")
    q(REPLICAS["r2"], "SELECT pg_reload_conf()")
    data.update(R)
    rr, p1, st, lt, po = (R[k] for k in ("round-robin", "primary-for-1s", "sticky-replica", "lsn-token", "primary-only"))
    report("Reads spread across replicas", "agents often don't see their own rename, and sometimes see time go backwards",
           f"stale {rr['stale_pct']}%, went backwards {rr['backwards_pct']}%", rr["stale_pct"] > 30 and rr["backwards_pct"] > 10)
    report("'Read the primary for 1 s after a write'", "fixes the first second only; a replica 2 s behind still serves old data",
           f"stale {p1['stale_pct']}%, primary reads {p1['primary_reads_pct']}%",
           10 < p1["stale_pct"] < rr["stale_pct"] and p1["primary_reads_pct"] > 20)
    report("Sticky session to one replica", "no time travel, but stickiness doesn't prove the replica has the write",
           f"stale {st['stale_pct']}%, backwards {st['backwards_pct']}%", st["stale_pct"] > 20 and st["backwards_pct"] == 0)
    report("LSN token: read only from a replica that has applied the session's last write", "no stale reads, most reads stay on replicas",
           f"stale {lt['stale_pct']}%, backwards {lt['backwards_pct']}%, primary reads {lt['primary_reads_pct']}%, read p99 {lt['read_p99_ms']} ms",
           lt["stale_pct"] == 0 and lt["backwards_pct"] == 0 and lt["primary_reads_pct"] < 30 and lt["waited_for_replica"] > 0)
    report("All reads on the primary", "correct, and the replicas do nothing", f"stale {po['stale_pct']}%, primary reads {po['primary_reads_pct']}%",
           po["stale_pct"] == 0 and po["primary_reads_pct"] == 100)
    with open(__file__.replace("ryw.py", "ryw_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
