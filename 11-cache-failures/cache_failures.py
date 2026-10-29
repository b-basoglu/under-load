"""Stampede, avalanche, penetration and cache outage, measured for Part 11.

PostgreSQL 17 limited to 2 CPUs (port 55432), Redis 7.4 (port 56379).
"""
import hashlib
import json
import random
import statistics
import threading
import time

import psycopg
from psycopg_pool import ConnectionPool
import redis

PG = "host=localhost port=55432 user=postgres password=test"
R = redis.Redis(host="localhost", port=56379, decode_responses=True, max_connections=1000)
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def setup():
    with psycopg.connect(PG, autocommit=True) as k:
        k.execute("DROP TABLE IF EXISTS comments, tickets")
        k.execute("CREATE TABLE tickets (id int PRIMARY KEY, workspace_id int, priority text)")
        k.execute("CREATE TABLE comments (id serial PRIMARY KEY, ticket_id int, created_at timestamptz)")
        k.execute("INSERT INTO tickets SELECT g, g % 20, (ARRAY['low','normal','high','urgent'])[1 + g % 4] FROM generate_series(1, 20000) g")
        k.execute("INSERT INTO comments (ticket_id, created_at) SELECT 1 + g % 20000, now() - g * interval '1 minute' FROM generate_series(1, 300000) g")
        k.execute("CREATE INDEX ON comments (ticket_id)")
        k.execute("VACUUM ANALYZE")


DASH = """SELECT t.priority, count(*), count(DISTINCT c.ticket_id) FROM comments c JOIN tickets t ON t.id = c.ticket_id
          WHERE t.workspace_id <> %s GROUP BY t.priority"""  # the org-wide dashboard: an expensive rebuild
TICKET = "SELECT id, priority FROM tickets WHERE id = %s"
POOL = ConnectionPool(PG, min_size=95, max_size=95, open=False)   # PostgreSQL allows 100; leave room for the probe and setup connections
db_queries = {"n": 0, "attempts": 0, "inflight": 0, "peak": 0}
lock = threading.Lock()


def run_db(sql, args):
    """attempts: requests that asked for a database connection; n / peak: queries that actually ran on PostgreSQL."""
    with lock:
        db_queries["attempts"] += 1
    with POOL.connection() as k:            # may wait for a free connection, or time out
        with lock:
            db_queries["n"] += 1; db_queries["inflight"] += 1
            db_queries["peak"] = max(db_queries["peak"], db_queries["inflight"])
        try:
            return k.execute(sql, args).fetchall()
        finally:
            with lock:
                db_queries["inflight"] -= 1


def reset_counters():
    db_queries.update(n=0, attempts=0, inflight=0, peak=0)


errors = [0]


def burst(n, fn):
    start = threading.Barrier(n)
    lat = []
    errors[0] = 0

    def w(i):
        start.wait()
        t0 = time.perf_counter()
        try:
            fn(i)
        except Exception:
            with lock:
                errors[0] += 1
        lat.append(time.perf_counter() - t0)
    ts = [threading.Thread(target=w, args=(i,)) for i in range(n)]
    for t in ts: t.start()
    for t in ts: t.join()
    lat.sort()
    return round(statistics.median(lat) * 1000), round(lat[-1] * 1000)


# ---------------------------------------------------------------- stampede
def get_naive(key, ws):
    v = R.get(key)
    if v is None:
        v = json.dumps(run_db(DASH, (ws,)))
        R.set(key, v, ex=60)
    return v


def get_locked(key, ws):
    """Only one request rebuilds; the others wait briefly and re-read the cache."""
    for _ in range(200):
        v = R.get(key)
        if v is not None:
            return v
        if R.set(f"lock:{key}", "1", nx=True, px=5000):
            try:
                v = R.get(key)  # someone may have finished the rebuild between our GET and our lock
                if v is not None:
                    return v
                v = json.dumps(run_db(DASH, (ws,)))
                R.set(key, v, ex=60)
                return v
            finally:
                R.delete(f"lock:{key}")
        time.sleep(0.02)
    return json.dumps(run_db(DASH, (ws,)))  # give up waiting


def get_swr(key, ws, refresh_after=30):
    """Stale-while-revalidate: the value carries a 'fresh until' time; past it, one request refreshes in the background
    and everyone keeps getting the stale value."""
    raw = R.get(key)
    if raw is not None:
        v = json.loads(raw)
        if time.time() > v["fresh_until"] and R.set(f"lock:{key}", "1", nx=True, px=5000):
            def refresh():
                try:
                    R.set(key, json.dumps({"rows": run_db(DASH, (ws,)), "fresh_until": time.time() + refresh_after}), ex=600)
                finally:
                    R.delete(f"lock:{key}")
            threading.Thread(target=refresh).start()
        return v["rows"]
    rows = run_db(DASH, (ws,))
    R.set(key, json.dumps({"rows": rows, "fresh_until": time.time() + refresh_after}), ex=600)
    return rows


def stampede():
    out = {}
    for name, fn in (("no protection", get_naive), ("rebuild lock", get_locked)):
        R.flushall(); reset_counters()
        med, worst = burst(200, lambda i: fn("dash:7", 7))
        out[name] = {"db_queries": db_queries["n"], "attempts": db_queries["attempts"], "peak_concurrent_db": db_queries["peak"], "median_ms": med,
                     "max_ms": worst, "failed": errors[0]}
        print("  ", name, out[name], flush=True)
    R.flushall(); reset_counters()
    get_swr("dash:7", 7)
    v = json.loads(R.get("dash:7")); v["fresh_until"] = time.time() - 1; R.set("dash:7", json.dumps(v), ex=600)  # it just went stale
    reset_counters()
    med, worst = burst(200, lambda i: get_swr("dash:7", 7))
    time.sleep(1)
    out["stale-while-revalidate"] = {"db_queries": db_queries["n"], "peak_concurrent_db": db_queries["peak"], "median_ms": med, "max_ms": worst}
    print("   stale-while-revalidate", out["stale-while-revalidate"], flush=True)
    data["stampede"] = out
    a, b, c = out["no protection"], out["rebuild lock"], out["stale-while-revalidate"]
    report(
        "200 requests hit the dashboard key the moment it expires",
        "without protection every request rebuilds it; a lock or stale-while-revalidate rebuilds it once",
        f"DB queries: none={a['db_queries']} ran ({a['attempts']} tried, at most {a['peak_concurrent_db']} at once on a {POOL.max_size}-connection pool, {a['failed']} timed out waiting; max {a['max_ms']} ms), lock={b['db_queries']} (max {b['max_ms']} ms), "
        f"SWR={c['db_queries']} (max {c['max_ms']} ms)",
        a["attempts"] >= 150
        and a["db_queries"] >= 100
        and a["peak_concurrent_db"] >= 50
        and b["db_queries"] == 1
        and c["db_queries"] == 1
        and a["max_ms"] > 5 * b["max_ms"]
        and c["median_ms"] < b["median_ms"],
    )


# ---------------------------------------------------------------- avalanche
def avalanche():
    out = {}
    for name, jitter in (("same TTL", 0), ("TTL + up to 100% jitter", 1.0)):
        R.flushall()
        base = 6
        pipe = R.pipeline()
        for t in range(1, 5001):
            ttl = base + (random.random() * base * jitter if jitter else 0)
            pipe.set(f"ticket:{t}", "x", px=int(ttl * 1000))
        pipe.execute()
        t0 = time.time()
        buckets = [0] * 36  # half-second buckets over 18 s
        stop = t0 + 18

        def reader(seed):
            rnd = random.Random(seed)
            while time.time() < stop:
                t = rnd.randint(1, 5000)
                if R.get(f"ticket:{t}") is None:
                    run_db(TICKET, (t,))
                    R.set(f"ticket:{t}", "x", ex=600)
                    b = int((time.time() - t0) / 0.5)
                    if b < 36:
                        with lock:
                            buckets[b] += 1
        ts = [threading.Thread(target=reader, args=(s,)) for s in range(16)]
        for t in ts: t.start()
        for t in ts: t.join()
        out[name] = {"misses_per_half_second": buckets, "peak": max(buckets)}
        print("  ", name, "peak", max(buckets), buckets, flush=True)
    data["avalanche"] = out
    a, b = out["same TTL"]["peak"], out["TTL + up to 100% jitter"]["peak"]
    report(
        "5,000 keys cached at the same moment, read continuously",
        "with one TTL, misses arrive in one spike when they all expire; jitter spreads them out",
        f"peak misses per 0.5 s: same TTL={a}, with jitter={b}",
        a > 2 * b and a > 500,
    )


# ---------------------------------------------------------------- penetration
class Bloom:
    def __init__(self, bits=1 << 20, k=5):
        self.bits, self.k, self.arr = bits, k, bytearray(bits // 8)

    def _h(self, s):
        d = hashlib.sha256(s.encode()).digest()
        return [int.from_bytes(d[i * 4:(i + 1) * 4], "big") % self.bits for i in range(self.k)]

    def add(self, s):
        for h in self._h(s):
            self.arr[h // 8] |= 1 << (h % 8)

    def might_contain(self, s):
        return all(self.arr[h // 8] >> (h % 8) & 1 for h in self._h(s))


def penetration():
    bloom = Bloom()
    for t in range(1, 20001):
        bloom.add(str(t))
    rnd = random.Random(1)
    ids = [rnd.randint(10_000_000, 99_999_999) for _ in range(5000)]  # random ids that never existed
    out = {}
    for name in ("cache only", "negative caching", "bloom filter"):
        R.flushall(); reset_counters()
        for t in ids:
            if name == "bloom filter" and not bloom.might_contain(str(t)):
                continue
            v = R.get(f"ticket:{t}")
            if v is None:
                rows = run_db(TICKET, (t,))
                if not rows and name == "negative caching":
                    R.set(f"ticket:{t}", "null", ex=30)
        out[name] = db_queries["n"]
    fp = sum(bloom.might_contain(str(rnd.randint(10_000_000, 99_999_999))) for _ in range(100000)) / 100000
    data["penetration"] = {"db_queries": out, "requests": len(ids), "bloom_false_positive_rate": fp, "bloom_kb": len(bloom.arr) // 1024}
    report(
        "5,000 requests for random ticket ids that never existed",
        "every one misses the cache — negative caching can't help with unique ids; a Bloom filter stops them",
        f"DB queries: cache only={out['cache only']}, negative caching={out['negative caching']}, bloom filter={out['bloom filter']} "
        f"(false-positive rate {fp:.4%}, {len(bloom.arr) // 1024} KB)",
        out["cache only"] == len(ids)
        and out["negative caching"] == len(ids)
        and out["bloom filter"] < 10
        and fp < 0.01,
    )


# ---------------------------------------------------------------- total cache outage
def outage():
    out = {}
    sem = threading.Semaphore(8)
    for name in ("unbounded fallback", "fallback capped at 8 queries"):
        reset_counters()
        def handle(i):
            if name.startswith("unbounded"):
                run_db(DASH, (i % 20,))
            else:
                if sem.acquire(timeout=0.5):
                    try:
                        run_db(DASH, (i % 20,))
                    finally:
                        sem.release()
                else:
                    out.setdefault(name + " shed", 0)
                    out[name + " shed"] = out[name + " shed"] + 1
        probe, stop_probe = [], threading.Event()

        def probe_fn():  # an unrelated agent request (indexed ticket lookup) running during the storm
            k = psycopg.connect(PG, autocommit=True)
            while not stop_probe.is_set():
                t0 = time.perf_counter(); k.execute(TICKET, (42,)).fetchall(); probe.append(time.perf_counter() - t0)
                time.sleep(0.01)
            k.close()
        pt = threading.Thread(target=probe_fn); pt.start(); time.sleep(0.3)
        med, worst = burst(100, handle)
        stop_probe.set(); pt.join()
        probe.sort()
        out[name] = {"db_queries": db_queries["n"], "peak_concurrent_db": db_queries["peak"], "median_ms": med, "max_ms": worst,
                     "other_request_p99_ms": round(probe[int(len(probe) * .99) - 1] * 1000, 1), "other_request_max_ms": round(probe[-1] * 1000, 1)}
        print("  ", name, out[name], flush=True)
    data["outage"] = out
    a, b = out["unbounded fallback"], out["fallback capped at 8 queries"]
    shed = out.get("fallback capped at 8 queries shed", 0)
    report(
        "Redis is down: 100 dashboard requests fall back to the database at once",
        "uncapped, the database runs everything at once and unrelated queries suffer; capped, they stay fast and excess requests are shed",
        f"uncapped: peak {a['peak_concurrent_db']} concurrent, unrelated query worst {a['other_request_max_ms']} ms; capped: peak "
        f"{b['peak_concurrent_db']}, unrelated query worst {b['other_request_max_ms']} ms; shed {shed}",
        b["peak_concurrent_db"] <= 8
        and a["peak_concurrent_db"] > 50
        and shed >= 50
        and b["other_request_max_ms"] < a["other_request_max_ms"] / 2,
    )


if __name__ == "__main__":
    setup()
    POOL.open(); POOL.wait()
    stampede()
    avalanche()
    penetration()
    outage()
    POOL.close()
    with open(__file__.replace("cache_failures.py", "cache_failures_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
