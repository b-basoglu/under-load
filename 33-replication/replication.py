"""Replication and consistency, measured for Part 33.

PostgreSQL 17 streaming replication: primary ul-pgp (55440), replicas ul-pgr1 (55441) and ul-pgr2 (55442), see README.
"""
import json
import statistics
import subprocess
import threading
import time

import psycopg

P = "host=localhost port=55440 user=postgres password=test dbname=postgres"
R1 = "host=localhost port=55441 user=postgres password=test dbname=postgres"
R2 = "host=localhost port=55442 user=postgres password=test dbname=postgres"
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


def lag_bytes():
    return {r[0]: int(r[1] or 0) for r in q(P, "SELECT application_name, pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) FROM pg_stat_replication")}


def wait_caught_up(timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        l = lag_bytes()
        if l and all(v == 0 for v in l.values()):
            return True
        time.sleep(0.2)
    return False


def setup():
    q(P, "ALTER SYSTEM RESET synchronous_standby_names")
    q(P, "ALTER SYSTEM RESET synchronous_commit")
    q(P, "SELECT pg_reload_conf()")
    q(P, "DROP TABLE IF EXISTS profiles, bulk, events")
    q(P, "CREATE TABLE profiles (id int PRIMARY KEY, display_name text, v int)")
    q(P, "INSERT INTO profiles SELECT g, 'agent ' || g, 0 FROM generate_series(1, 1000) g")
    q(P, "CREATE TABLE bulk (id bigserial, body text)")
    q(P, "CREATE TABLE events (id bigserial PRIMARY KEY, n int)")
    wait_caught_up()


# ---- 1. read right after write, from an async replica ----------------------------------------------
def read_after_write(n=1000, load=False):
    stop = threading.Event()

    def writer():                                         # a bulk import running on the primary
        with psycopg.connect(P, autocommit=True) as c:
            while not stop.is_set():
                c.execute("INSERT INTO bulk (body) SELECT repeat('x', 500) FROM generate_series(1, 5000)")
    ts = [threading.Thread(target=writer) for _ in range(3)] if load else []
    for t in ts: t.start()
    time.sleep(1 if load else 0)
    stale, lags = 0, []
    with psycopg.connect(P, autocommit=True) as p, psycopg.connect(R1, autocommit=True) as r:
        for i in range(n):
            pid = 1 + i % 1000
            v = p.execute("UPDATE profiles SET display_name = %s, v = v + 1 WHERE id = %s RETURNING v", (f"renamed {i}", pid)).fetchone()[0]
            got = r.execute("SELECT v FROM profiles WHERE id = %s", (pid,)).fetchone()[0]
            stale += got < v
            if i % 50 == 0:
                lags.append(lag_bytes().get("r1", 0))
    stop.set()
    for t in ts: t.join()
    wait_caught_up()
    return {"load": load, "reads": n, "stale_pct": round(100 * stale / n, 1), "lag_kb_p50": round(pct(lags, .5) / 1024), "lag_kb_max": round(max(lags) / 1024)}


def time_lag(load=True, secs=6):
    """How long until a write is visible on the replica, under a bulk import."""
    stop = threading.Event()

    def writer():
        with psycopg.connect(P, autocommit=True) as c:
            while not stop.is_set():
                c.execute("INSERT INTO bulk (body) SELECT repeat('x', 500) FROM generate_series(1, 5000)")
    ts = [threading.Thread(target=writer) for _ in range(3)] if load else []
    for t in ts: t.start()
    time.sleep(1)
    vis = []
    with psycopg.connect(P, autocommit=True) as p, psycopg.connect(R1, autocommit=True) as r:
        t0 = time.time()
        k = 0
        while time.time() - t0 < secs:
            k += 1
            p.execute("UPDATE profiles SET v = v + 1 WHERE id = 1")
            target = p.execute("SELECT v FROM profiles WHERE id = 1").fetchone()[0]
            tw = time.perf_counter()
            while r.execute("SELECT v FROM profiles WHERE id = 1").fetchone()[0] < target:
                time.sleep(0.0005)
            vis.append((time.perf_counter() - tw) * 1000)
    stop.set()
    for t in ts: t.join()
    wait_caught_up()
    return {"load": load, "samples": len(vis), "visible_p50_ms": round(pct(vis, .5), 1), "visible_p99_ms": round(pct(vis, .99), 1), "visible_max_ms": round(max(vis), 1)}


# ---- 2. a long query on a replica holds back replay ------------------------------------------------
def replica_conflict(feedback):
    q(R1, f"ALTER SYSTEM SET hot_standby_feedback = {'on' if feedback else 'off'}")
    q(R1, "ALTER SYSTEM SET max_standby_streaming_delay = '8s'")
    q(R1, "SELECT pg_reload_conf()")
    q(P, "DROP TABLE IF EXISTS report_src")
    q(P, "CREATE TABLE report_src AS SELECT g AS id, repeat('x', 100) AS body FROM generate_series(1, 200000) g")
    wait_caught_up()
    time.sleep(1)
    out = {"feedback": feedback, "error": None}
    series = []

    def report_query():                                  # an analytics query on the replica, inside a long snapshot
        try:
            with psycopg.connect(R1) as c:
                c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                c.execute("SELECT count(*) FROM report_src").fetchone()
                c.execute("SELECT pg_sleep(15)")
                c.execute("SELECT count(*) FROM report_src").fetchone()
                c.execute("COMMIT")
        except Exception as e:
            out["error"] = type(e).__name__ + ": " + str(e).split("\n")[0][:120]
    t = threading.Thread(target=report_query)
    t.start()
    time.sleep(1)
    q(P, "DELETE FROM report_src WHERE id % 2 = 0")       # rows the replica's snapshot still needs
    q(P, "VACUUM report_src")                             # cleanup on the primary → a conflicting record in the WAL
    t0 = time.time()
    while t.is_alive() and time.time() - t0 < 20:
        q(P, "UPDATE profiles SET v = v + 1 WHERE id = 2")
        target = q(P, "SELECT v FROM profiles WHERE id = 2")[0][0]
        rv = q(R2, "SELECT v FROM profiles WHERE id = 2")[0][0]
        tr = q(R1, "SELECT now() - pg_last_xact_replay_timestamp()")[0][0]
        series.append({"t": round(time.time() - t0, 1), "r1_behind_s": round(tr.total_seconds(), 1) if tr else 0})
        time.sleep(0.5)
    t.join()
    wait_caught_up()
    dead = q(P, "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'report_src'")[0][0]
    out.update({"series": series, "max_behind_s": max(s["r1_behind_s"] for s in series), "dead_tuples_left_on_primary": dead})
    q(R1, "ALTER SYSTEM RESET hot_standby_feedback")
    q(R1, "ALTER SYSTEM RESET max_standby_streaming_delay")
    q(R1, "SELECT pg_reload_conf()")
    return out


# ---- 3. synchronous commit levels --------------------------------------------------------------------
def commit_levels(n=400):
    q(P, "ALTER SYSTEM SET synchronous_standby_names = 'r1'")
    q(P, "SELECT pg_reload_conf()")
    time.sleep(1)
    out = {}
    for level in ("off", "local", "remote_write", "on", "remote_apply"):
        lat, stale = [], 0
        with psycopg.connect(P, autocommit=True) as p, psycopg.connect(R1, autocommit=True) as r:
            p.execute(f"SET synchronous_commit = {level}")
            for i in range(n):
                t0 = time.perf_counter()
                p.execute("INSERT INTO events (n) VALUES (%s)", (i,))
                lat.append((time.perf_counter() - t0) * 1000)
                if i % 4 == 0:
                    rid = p.execute("SELECT max(id) FROM events").fetchone()[0]
                    stale += (r.execute("SELECT max(id) FROM events").fetchone()[0] or 0) < rid
        out[level] = {"p50_ms": round(pct(lat, .5), 3), "p99_ms": round(pct(lat, .99), 3), "stale_reads_pct": round(100 * stale / (n / 4), 1)}
    q(P, "ALTER SYSTEM RESET synchronous_standby_names")
    q(P, "SELECT pg_reload_conf()")
    return out


# ---- 4. failover: what an async replica never received -----------------------------------------------
def failover():
    out = {}
    # sync: a paused standby blocks commits instead of losing them
    q(P, "ALTER SYSTEM SET synchronous_standby_names = 'r2'")
    q(P, "SELECT pg_reload_conf()")
    time.sleep(1)
    subprocess.run(["docker", "pause", "ul-pgr2"], capture_output=True)
    acked_sync, waited, warning = 0, 0, None
    res = {}

    def attempt():
        with psycopg.connect(P, autocommit=True) as p:
            res["pid"] = p.execute("SELECT pg_backend_pid()").fetchone()[0]
            notices = []
            p.add_notice_handler(lambda d: notices.append(d.message_primary))
            try:
                p.execute("INSERT INTO events (n) VALUES (-1)")
                res["acked"] = True
            except Exception as e:
                res["error"] = str(e).split("\n")[0][:120]
            res["notices"] = notices
    t = threading.Thread(target=attempt)
    t0 = time.time()
    t.start()
    t.join(5)                                             # the client waits 5 s, then gives up
    waited = round(time.time() - t0, 1)
    still_waiting = t.is_alive()
    q(P, "SELECT pg_cancel_backend(%s)", res.get("pid"))  # cancel the waiting commit
    t.join(5)
    on_primary = q(P, "SELECT count(*) FROM events WHERE n = -1")[0][0]
    out["sync_standby_paused"] = {"acked_within_5s": 0 if still_waiting else int(bool(res.get("acked"))),
                                  "acked_after_cancel": int(bool(res.get("acked"))), "still_waiting_after_s": waited if still_waiting else None,
                                  "notice": (res.get("notices") or [None])[0], "row_exists_on_primary": on_primary}
    subprocess.run(["docker", "unpause", "ul-pgr2"], capture_output=True)
    q(P, "ALTER SYSTEM RESET synchronous_standby_names")
    q(P, "SELECT pg_reload_conf()")
    wait_caught_up()
    # async: commits are acknowledged while the replica is behind; then the primary dies
    before = q(P, "SELECT count(*) FROM events")[0][0]
    subprocess.run(["docker", "network", "disconnect", "ul-rep", "ul-pgr2"], capture_output=True)   # network partition
    time.sleep(1)
    acked = 0
    with psycopg.connect(P, autocommit=True) as p:
        for i in range(500):
            p.execute("INSERT INTO events (n) VALUES (%s)", (i,))
            acked += 1
    subprocess.run(["docker", "kill", "ul-pgp"], capture_output=True)      # primary lost
    subprocess.run(["docker", "network", "connect", "ul-rep", "ul-pgr2"], capture_output=True)
    time.sleep(2)
    def r2(sql):                                          # via docker exec: the published port does not survive the network reconnect
        return subprocess.run(["docker", "exec", "ul-pgr2", "psql", "-U", "postgres", "-tAc", sql], capture_output=True, text=True).stdout.strip()
    r2("SELECT pg_promote()")
    time.sleep(3)
    after = int(r2("SELECT count(*) FROM events"))
    out["async_failover"] = {"acked_while_replica_behind": acked, "rows_before": before, "rows_on_new_primary": after,
                             "acknowledged_writes_lost": before + acked - after}
    return out


if __name__ == "__main__":
    setup()
    A0, A1 = read_after_write(load=False), read_after_write(load=True)
    print(A0, A1, flush=True)
    V0, V1 = time_lag(load=False), time_lag(load=True)
    print(V0, V1, flush=True)
    C0, C1 = replica_conflict(False), replica_conflict(True)
    print({k: v for k, v in C0.items() if k != "series"}, {k: v for k, v in C1.items() if k != "series"}, flush=True)
    L = commit_levels()
    print(L, flush=True)
    F = failover()
    print(F, flush=True)
    data.update({"raw_idle": A0, "raw_load": A1, "visible_idle": V0, "visible_load": V1, "conflict_off": C0, "conflict_on": C1, "commit": L, "failover": F})
    report("Read right after write, async replica, idle system", "even with near-zero lag some reads are stale",
           f"{A0['stale_pct']}% stale of {A0['reads']} reads; time until visible p50 {V0['visible_p50_ms']} ms, p99 {V0['visible_p99_ms']} ms",
           A0["stale_pct"] > 50)
    report("Same, during a bulk import on the primary", "lag grows with write volume; more reads are stale",
           f"{A1['stale_pct']}% stale; lag up to {A1['lag_kb_max']:,} KB; visible p99 {V1['visible_p99_ms']} ms, max {V1['visible_max_ms']} ms",
           A1["stale_pct"] >= 90 and A1["lag_kb_max"] > A0["lag_kb_max"] + 1000 and V1["visible_p99_ms"] > V0["visible_p99_ms"] * 5)
    report("Long query on a replica (hot_standby_feedback off)", "replay pauses up to max_standby_streaming_delay, then the query is cancelled",
           f"replica fell {C0['max_behind_s']} s behind; query: {C0['error']}", C0["max_behind_s"] >= 5 and C0["error"] is not None)
    report("Same with hot_standby_feedback on", "no lag and no cancellation — the primary keeps dead rows instead",
           f"replica max {C1['max_behind_s']} s behind; query error: {C1['error']}; dead tuples left on primary {C1['dead_tuples_left_on_primary']:,}",
           C1["error"] is None and C1["max_behind_s"] < 2 and C1["dead_tuples_left_on_primary"] > 50000)
    report("synchronous_commit levels", "each stronger level waits for more of the replica; remote_apply makes the replica readable at commit",
           " · ".join(f"{k} p50 {v['p50_ms']} ms / stale {v['stale_reads_pct']}%" for k, v in L.items()),
           L["off"]["stale_reads_pct"] > 50 and L["remote_apply"]["stale_reads_pct"] == 0 and L["remote_apply"]["p50_ms"] > L["off"]["p50_ms"])
    fs, fa = F["sync_standby_paused"], F["async_failover"]
    report("Synchronous standby unreachable", "the commit waits instead of being acknowledged — availability traded for durability",
           f"still waiting after {fs['still_waiting_after_s']} s; cancelled → client sees success ({fs['acked_after_cancel']}) with notice '{fs['notice']}'; row on primary, not on standby: {fs['row_exists_on_primary']}",
           fs["acked_within_5s"] == 0 and fs["still_waiting_after_s"] is not None and fs["still_waiting_after_s"] >= 4)
    report("Async replica promoted after the primary dies", "writes acknowledged to clients are gone",
           f"{fa['acked_while_replica_behind']} acknowledged while the replica was cut off → {fa['acknowledged_writes_lost']} lost after failover",
           fa["acknowledged_writes_lost"] >= 100 and fa["acknowledged_writes_lost"] == fa["acked_while_replica_behind"])
    with open(__file__.replace("replication.py", "replication_results.json"), "w") as fh:
        json.dump(data, fh, default=str)
    print(f"{sum(results)}/{len(results)} passed")
