"""Connection pooling measurements for Part 7.

Setup: PostgreSQL 17 in Docker limited to 2 CPUs with max_connections=100 (port 55432),
and PgBouncer 1.26 in transaction mode with default_pool_size=20 in front of it (port 56432).
"""
import json
import statistics
import threading
import time

import psycopg
from psycopg_pool import ConnectionPool, PoolTimeout

PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
BOUNCER = "host=localhost port=56432 user=postgres password=test dbname=postgres"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def setup():
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS tickets CASCADE")
        c.execute("CREATE TABLE tickets (id int PRIMARY KEY, workspace_id int, assignee_id int, subject text)")
        c.execute("INSERT INTO tickets SELECT g, g % 50, g % 400, repeat('x', 80) FROM generate_series(1, 200000) g")
        c.execute("CREATE INDEX ON tickets (workspace_id, assignee_id)")
        c.execute("VACUUM ANALYZE tickets")


QUERY = "SELECT id, subject FROM tickets WHERE workspace_id = %s AND assignee_id = %s LIMIT 50"
# A CPU-heavy query, so the database's 2 CPUs are the bottleneck.
HEAVY = "SELECT count(*) FROM tickets t WHERE t.subject LIKE %s AND (t.id * 7) %% 13 = %s"


def connect_cost():
    n = 200
    t0 = time.time()
    for i in range(n):
        with psycopg.connect(PG) as c:
            c.execute(QUERY, (i % 50, i % 400)).fetchall()
    fresh = (time.time() - t0) / n * 1000
    with psycopg.connect(PG) as c:
        t0 = time.time()
        for i in range(n):
            c.execute(QUERY, (i % 50, i % 400)).fetchall()
        reused = (time.time() - t0) / n * 1000
    with psycopg.connect(PG) as c:
        mem = c.execute("SELECT pg_size_pretty(sum(total_bytes)) FROM pg_backend_memory_contexts").fetchone()[0]
    data["connect_cost_ms"] = {"new_connection_per_query": round(fresh, 2), "reused_connection": round(reused, 2),
                               "backend_memory_one_idle_session": mem}
    report("New connection per query vs reusing one", "opening a connection costs far more than the query itself",
           f"new connection each time: {fresh:.2f} ms/query; reused: {reused:.2f} ms/query; memory of one session: {mem}",
           fresh > 5 * reused)


def too_many():
    conns, err = [], None
    try:
        for _ in range(150):
            conns.append(psycopg.connect(PG))
    except psycopg.Error as e:
        err = (e.sqlstate, str(e).strip().splitlines()[-1].split("FATAL:")[-1].strip())
    opened = len(conns)
    for c in conns:
        c.close()
    data["max_connections"] = {"opened": opened, "error": err}
    report("Opening more connections than max_connections=100",
           "new connections are refused (FATAL: sorry, too many clients already, SQLSTATE 53300)",
           f"opened {opened} connections, then: {err}", err and "too many clients" in err[1])


def throughput_vs_connections():
    rows = []
    for n in (1, 2, 4, 8, 16, 32, 64):
        lat, count = [], [0]
        stop = time.time() + 5
        lock = threading.Lock()

        def worker(i):
            c = psycopg.connect(PG, autocommit=True)
            c.execute("SET max_parallel_workers_per_gather = 0")  # one CPU per query, no parallel helpers
            while time.time() < stop:
                t0 = time.time()
                c.execute(HEAVY, (f"%{i % 10}%", i % 13)).fetchone()
                with lock:
                    lat.append(time.time() - t0); count[0] += 1
            c.close()
        ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in ts: t.start()
        for t in ts: t.join()
        lat.sort()
        row = {"connections": n, "queries_per_s": round(count[0] / 5, 1),
               "median_ms": round(statistics.median(lat) * 1000), "p99_ms": round(lat[int(len(lat) * .99) - 1] * 1000)}
        rows.append(row); print("  ", row, flush=True)
    data["throughput_vs_connections"] = rows
    best = max(rows, key=lambda r: r["queries_per_s"])
    last = rows[-1]
    # Peak must be among a handful of connections (not at 32/64). Comparing last to
    # max(all) would be tautological — last can never beat the max.
    early = [r for r in rows if r["connections"] <= 8]
    early_best = max(r["queries_per_s"] for r in early)
    ok = (
        best["connections"] <= 8
        and last["queries_per_s"] <= early_best * 1.15
        and last["median_ms"] > 5 * rows[0]["median_ms"]
    )
    report("Throughput vs active connections (database has 2 CPUs)",
           "throughput peaks at a handful of connections; at high concurrency latency keeps rising and qps does not beat the early peak",
           f"best: {best['queries_per_s']}/s at {best['connections']} connections; at {last['connections']}: "
           f"{last['queries_per_s']}/s with median {last['median_ms']} ms (vs {rows[0]['median_ms']} ms at {rows[0]['connections']})",
           ok)


def pool_exhaustion():
    pool = ConnectionPool(PG, min_size=10, max_size=10, timeout=1.0, open=True)
    pool.wait()
    stop = threading.Event()

    def slow():
        with pool.connection() as c:
            c.execute("SELECT pg_sleep(8)")
    slowers = [threading.Thread(target=slow) for _ in range(10)]
    for t in slowers: t.start()
    time.sleep(0.3)
    stats_busy = pool.get_stats()
    outcomes = []
    for _ in range(5):
        t0 = time.time()
        try:
            with pool.connection() as c:
                c.execute("SELECT 1"); outcomes.append(("ok", round(time.time() - t0, 2)))
        except PoolTimeout:
            outcomes.append(("timeout", round(time.time() - t0, 2)))
    for t in slowers: t.join()
    t0 = time.time()
    with pool.connection() as c:
        c.execute("SELECT 1")
    after = round((time.time() - t0) * 1000, 1)
    pool.close()
    data["pool_exhaustion"] = {"outcomes_while_busy": outcomes, "ms_after_slow_queries_finish": after,
                               "pool_stats_while_busy": {k: stats_busy.get(k) for k in ("pool_size", "pool_available", "requests_waiting")}}
    report("Pool of 10, all held by an 8-second query", "every other request waits for the acquisition timeout (1 s) and fails",
           f"5 fast requests while busy: {outcomes}; after the slow queries finished: {after} ms",
           all(o == "timeout" for o, _ in outcomes) and after < 50)


def pgbouncer():
    clients = 200
    errs, done = [], [0]
    lock = threading.Lock()
    stop = time.time() + 5

    def worker(i):
        try:
            c = psycopg.connect(BOUNCER, autocommit=True, prepare_threshold=None)
            while time.time() < stop:
                c.execute(QUERY, (i % 50, i % 400)).fetchall()
                with lock:
                    done[0] += 1
            c.close()
        except psycopg.Error as e:
            with lock:
                errs.append(e.sqlstate)
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for t in ts: t.start()
    time.sleep(2.5)
    with psycopg.connect(PG) as c:
        server = c.execute("SELECT count(*) FROM pg_stat_activity WHERE usename='postgres' AND backend_type='client backend' "
                           "AND application_name <> 'psql' AND pid <> pg_backend_pid()").fetchone()[0]
    for t in ts: t.join()
    data["pgbouncer"] = {"client_connections": clients, "server_connections_seen": server, "queries": done[0], "errors": errs}
    report("200 clients through PgBouncer (transaction mode, pool size 20)",
           "200 client connections share ~20 real database connections, with no errors",
           f"database saw {server} connections; {done[0]} queries in 5 s; errors={errs[:3]}",
           15 <= server <= 22 and not errs)

    # Session state doesn't survive in transaction mode
    a = psycopg.connect(BOUNCER, autocommit=True)
    a.execute("SET statement_timeout = '1234ms'")
    seen = set()
    stop2 = time.time() + 4

    def hammer(i):
        o = psycopg.connect(BOUNCER, autocommit=True)
        while time.time() < stop2:
            o.execute("SELECT pg_sleep(0.002)")
        o.close()
    hs = [threading.Thread(target=hammer, args=(i,)) for i in range(60)]
    for h in hs: h.start()
    while time.time() < stop2:
        seen.add(a.execute("SHOW statement_timeout").fetchone()[0])
    for h in hs: h.join()
    data["pgbouncer_session_state"] = sorted(seen)
    report("SET in transaction mode", "session settings don't reliably stick: later statements may run on another server connection",
           f"values of statement_timeout seen on the same client connection after SET 1234ms: {sorted(seen)}", len(seen) > 1)


if __name__ == "__main__":
    setup()
    connect_cost()
    too_many()
    throughput_vs_connections()
    pool_exhaustion()
    pgbouncer()
    with open(__file__.replace("pool_tests.py", "pool_results.json"), "w") as f:
        json.dump(data, f, indent=1, default=str)
    print(f"{sum(results)}/{len(results)} passed")
