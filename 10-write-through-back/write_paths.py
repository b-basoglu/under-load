"""Write-through vs write-back, measured on PostgreSQL 17 + Redis 7.4, for Part 10.

Uses a separate Redis container (ul-wb, port 56380) that the script restarts to simulate crashes.
"""
import json
import subprocess
import threading
import time

import psycopg
import redis

PG = "host=localhost port=55432 user=postgres password=test"
results, data = [], {}
TICKETS = 200


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout


def start_redis(*args):
    sh("docker rm -f ul-wb")
    sh(f"docker run -d --name ul-wb -p 56380:6379 redis:7.4 redis-server {' '.join(args)}")
    r = redis.Redis(host="localhost", port=56380, decode_responses=True)
    for _ in range(50):
        try:
            r.ping(); return r
        except redis.ConnectionError:
            time.sleep(0.2)


def crash_redis():
    sh("docker kill -s KILL ul-wb"); sh("docker start ul-wb")
    r = redis.Redis(host="localhost", port=56380, decode_responses=True)
    for _ in range(50):
        try:
            r.ping(); return r
        except (redis.ConnectionError, redis.BusyLoadingError):
            time.sleep(0.2)


def db():
    return psycopg.connect(PG, autocommit=True)


def reset_db():
    with db() as k:
        k.execute("DROP TABLE IF EXISTS ticket_stats")
        k.execute("CREATE TABLE ticket_stats (ticket_id int PRIMARY KEY, views bigint NOT NULL DEFAULT 0, "
                  "status text, version int NOT NULL DEFAULT 0)")
        k.execute("INSERT INTO ticket_stats (ticket_id) SELECT generate_series(1, %s)", (TICKETS,))


def db_views():
    with db() as k:
        return k.execute("SELECT sum(views) FROM ticket_stats").fetchone()[0]


def run_workers(n, seconds, fn):
    counts, lat = [0] * n, []
    lock = threading.Lock()
    stop = time.time() + seconds

    def w(i):
        ctx = fn.__defaults__ and None
        state = {}
        while time.time() < stop:
            t0 = time.perf_counter(); fn(i, state); dt = time.perf_counter() - t0
            counts[i] += 1
            with lock:
                lat.append(dt)
    ts = [threading.Thread(target=w, args=(i,)) for i in range(n)]
    for t in ts: t.start()
    for t in ts: t.join()
    lat.sort()
    return sum(counts), round(lat[len(lat) // 2] * 1000, 3), round(lat[int(len(lat) * .99) - 1] * 1000, 2)


def flush_once(r, k, stats):
    """One pending→Postgres batch. Safe if the hash is empty."""
    if not r.exists("views:pending"):
        return
    r.rename("views:pending", "views:flushing")
    pending = r.hgetall("views:flushing")
    if pending:
        with k.transaction():
            with k.cursor() as cur:
                cur.executemany(
                    "UPDATE ticket_stats SET views = views + %s WHERE ticket_id = %s",
                    [(int(v), int(t)) for t, v in pending.items()],
                )
        stats["flushes"] += 1
        stats["rows_written"] += len(pending)
        stats["increments"] += sum(int(v) for v in pending.values())
    r.delete("views:flushing")


def flusher(r, stop, stats, interval=1.0):
    """Moves buffered view counts from Redis into PostgreSQL in one batch per interval."""
    k = db()
    while not stop.is_set():
        time.sleep(interval)
        if stop.is_set():
            break
        flush_once(r, k, stats)
    flush_once(r, k, stats)  # drain whatever is left after stop


def throughput():
    reset_db()
    r = start_redis("--save", "''", "--appendonly", "no")

    def write_through(i, st):
        if "k" not in st:
            st["k"] = db()
        t = 1 + (i * 7919 + st.setdefault("n", 0)) % TICKETS; st["n"] += 1
        v = st["k"].execute("UPDATE ticket_stats SET views = views + 1 WHERE ticket_id = %s RETURNING views", (t,)).fetchone()[0]
        r.hset(f"ticket:{t}", "views", v)

    wt = run_workers(16, 4, write_through)
    wt_db = db_views()

    reset_db(); r.flushall()
    stats = {"flushes": 0, "rows_written": 0, "increments": 0}
    stop = threading.Event()
    f = threading.Thread(target=flusher, args=(r, stop, stats)); f.start()

    def write_back(i, st):
        t = 1 + (i * 7919 + st.setdefault("n", 0)) % TICKETS; st["n"] += 1
        r.hincrby("views:pending", t, 1)

    wb = run_workers(16, 4, write_back)
    time.sleep(0.2)
    lag_db = db_views()
    stop.set(); f.join()
    final_db = db_views()
    data["throughput"] = {
        "write_through": {"writes": wt[0], "per_s": round(wt[0] / 4), "median_ms": wt[1], "p99_ms": wt[2], "db_statements": wt[0], "db_views_after": wt_db, "db_matches_writes": wt_db == wt[0]},
        "write_back": {"writes": wb[0], "per_s": round(wb[0] / 4), "median_ms": wb[1], "p99_ms": wb[2],
                       "db_rows_written": stats["rows_written"], "flushes": stats["flushes"],
                       "db_views_right_after_load": lag_db, "db_views_after_final_flush": final_db}}
    report("Write-through vs write-back: 16 workers counting ticket views for 4 s",
           "write-back acknowledges faster and turns many increments into a few database writes",
           f"write-through {round(wt[0] / 4):,}/s, median {wt[1]} ms, {wt[0]:,} DB updates (db sum={wt_db:,}); "
           f"write-back {round(wb[0] / 4):,}/s, median {wb[1]} ms, {stats['rows_written']:,} DB row updates in {stats['flushes']} flushes",
           wb[0] > 2 * wt[0]
           and wb[1] < wt[1]
           and wt_db == wt[0]
           and stats["rows_written"] < wb[0] / 20
           and stats["flushes"] >= 2)
    report("The database lags behind a write-back cache", "right after the burst, the database is missing the unflushed views",
           f"acknowledged {wb[0]:,}; in the database right after: {lag_db:,}; after the final flush: {final_db:,}",
           lag_db < wb[0] * 0.95 and final_db == wb[0] and (wb[0] - lag_db) > 100)


def crash_loss():
    out = {}
    for label, args in (("no persistence", ["--save", "''", "--appendonly", "no"]),
                        ("AOF, appendfsync everysec", ["--appendonly", "yes", "--appendfsync", "everysec"]),
                        ("AOF, appendfsync always", ["--appendonly", "yes", "--appendfsync", "always"])):
        r = start_redis(*args)
        acked = 0
        stop = time.time() + 2.5
        while time.time() < stop:
            r.hincrby("views:pending", 1 + acked % TICKETS, 1); acked += 1
        r = crash_redis()
        survived = sum(int(v) for v in r.hgetall("views:pending").values())
        out[label] = {"acknowledged": acked, "survived_crash": survived, "lost": acked - survived}
        print("  ", label, out[label], flush=True)
    data["crash"] = out
    a, b_, c = out["no persistence"], out["AOF, appendfsync everysec"], out["AOF, appendfsync always"]
    # Process kill (docker KILL): AOF everysec/always both keep the buffer — host page cache still holds
    # the file. everysec can still lose ~1s on a real power cut; we don't simulate that here.
    # always fsyncs each write → far fewer acks in the same wall time.
    report(
        "Redis killed before the flush",
        "no persistence loses everything buffered; AOF survives a process crash; always is slower than everysec",
        f"lost: no persistence {a['lost']:,}/{a['acknowledged']:,}; everysec {b_['lost']:,}/{b_['acknowledged']:,}; "
        f"always {c['lost']}/{c['acknowledged']:,}",
        a["lost"] == a["acknowledged"]
        and a["acknowledged"] > 1000
        and b_["lost"] == 0
        and c["lost"] == 0
        and c["acknowledged"] < b_["acknowledged"] * 0.7,
    )


def ordering():
    reset_db()
    with db() as k:
        # Two status changes for ticket 9, buffered in order v1=pending, v2=solved; two flush workers apply them out of order
        k.execute("UPDATE ticket_stats SET status = 'solved', version = 2 WHERE ticket_id = 9")      # worker B flushes v2 first
        k.execute("UPDATE ticket_stats SET status = 'pending', version = 1 WHERE ticket_id = 9")     # worker A flushes v1 late
        naive = k.execute("SELECT status FROM ticket_stats WHERE ticket_id = 9").fetchone()[0]
        k.execute("UPDATE ticket_stats SET status = NULL, version = 0 WHERE ticket_id = 9")
        guarded = "UPDATE ticket_stats SET status = %s, version = %s WHERE ticket_id = 9 AND version < %s"
        n2 = k.execute(guarded, ("solved", 2, 2)).rowcount
        n1 = k.execute(guarded, ("pending", 1, 1)).rowcount
        fixed = k.execute("SELECT status FROM ticket_stats WHERE ticket_id = 9").fetchone()[0]
    data["ordering"] = {"naive_final": naive, "guarded_final": fixed, "late_update_rows": n1}
    report("Two flush workers apply a ticket's updates out of order",
           "without a version guard the older status wins; with WHERE version < $new it's rejected",
           f"naive: {naive}; guarded: {fixed} (late update changed {n1} rows)", naive == "pending" and fixed == "solved" and n1 == 0)


if __name__ == "__main__":
    throughput()
    crash_loss()
    ordering()
    sh("docker rm -f ul-wb")
    with open(__file__.replace("write_paths.py", "write_paths_results.json"), "w") as f:
        json.dump(data, f, indent=1, default=int)
    print(f"{sum(results)}/{len(results)} passed")
