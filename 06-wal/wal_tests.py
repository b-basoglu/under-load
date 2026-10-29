"""Crash, durability and WAL measurements for Part 6, against PostgreSQL 17 in Docker.

Uses its own container (ul-wal) with a named volume so data survives a `docker kill`.
Note: killing the container kills the database processes (and their memory) but not the host's OS page cache,
so this simulates a database crash, not a power cut.
"""
import json
import re
import subprocess
import threading
import time

import psycopg

NAME, PORT = "ul-wal", 55433
DSN = f"host=localhost port={PORT} user=postgres password=test"
results, data = [], {}


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def wait_ready():
    for _ in range(120):
        try:
            psycopg.connect(DSN, connect_timeout=1).close()
            return
        except psycopg.Error:
            time.sleep(0.5)
    raise RuntimeError("postgres did not come up")


def start_fresh():
    sh(f"docker rm -f {NAME}; docker volume rm -f ul-wal-data")
    sh(f"docker run -d --name {NAME} -e POSTGRES_PASSWORD=test -p {PORT}:5432 -v ul-wal-data:/var/lib/postgresql/data "
       f"postgres:17 -c wal_level=logical -c max_wal_size=4GB -c checkpoint_timeout=1h -c log_checkpoints=on "
       f"-c max_slot_wal_keep_size=64MB")
    wait_ready()


def crash_and_restart():
    since = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    sh(f"docker kill -s KILL {NAME}")
    t0 = time.time()
    sh(f"docker start {NAME}")
    wait_ready()
    return time.time() - t0, sh(f"docker logs --since {since} {NAME} 2>&1")


def c():
    return psycopg.connect(DSN, autocommit=True)


def main():
    start_fresh()
    with c() as k:
        k.execute("CREATE TABLE tickets (id bigserial PRIMARY KEY, subject text, body text)")

    # 1. COMMIT returned, then the process died
    with c() as k:
        k.execute("INSERT INTO tickets (subject) VALUES ('refund for order 1182')")
    secs, logs = crash_and_restart()
    with c() as k:
        n = k.execute("SELECT count(*) FROM tickets WHERE subject = 'refund for order 1182'").fetchone()[0]
    recovery_lines = [l.split("LOG:", 1)[-1].strip() for l in logs.splitlines()
                      if "not properly shut down" in l or "redo starts" in l or "redo done" in l or "ready to accept" in l]
    data["crash_recovery_log"] = recovery_lines
    report("Kill -9 right after COMMIT", "the committed ticket is there after restart; logs show automatic recovery",
           f"ticket present={n == 1}; restart+recovery took {secs:.1f} s; log: {recovery_lines[:3]}",
           n == 1 and any("redo" in l for l in recovery_lines))

    # 2. synchronous_commit = off can lose acknowledged commits
    for mode in ("on", "off"):
        with c() as k:
            k.execute("TRUNCATE tickets")
        acked = []
        stop = threading.Event()

        def writer():
            k = psycopg.connect(DSN, autocommit=True)
            k.execute(f"SET synchronous_commit = {mode}")
            i = 0
            while not stop.is_set():
                try:
                    k.execute("INSERT INTO tickets (id, subject) VALUES (%s, 'x')", (i,))
                    acked.append(i)
                    i += 1
                except psycopg.Error:
                    break
        t = threading.Thread(target=writer); t.start(); time.sleep(3)
        sh(f"docker kill -s KILL {NAME}")
        stop.set(); t.join()
        sh(f"docker start {NAME}"); wait_ready()
        with c() as k:
            present = k.execute("SELECT count(*) FROM tickets").fetchone()[0]
        lost = len(acked) - present
        data[f"sync_commit_{mode}"] = {"acknowledged": len(acked), "present_after_crash": present, "lost": lost}
        report(f"synchronous_commit = {mode}, then kill -9 during a stream of commits",
               "no acknowledged commit is lost" if mode == "on" else "the last few acknowledged commits can be lost",
               f"acknowledged={len(acked)} present after crash={present} lost={lost}",
               lost == 0 if mode == "on" else lost > 0)       # "off" must actually lose something, or the test didn't test it

    # 3. Commit latency and group commit
    def bench(mode, clients, seconds=4):
        with c() as k:
            k.execute("TRUNCATE tickets")
            k.execute("SELECT pg_stat_reset_shared('wal')")
        counts = [0] * clients
        stop = time.time() + seconds

        def w(i):
            k = psycopg.connect(DSN, autocommit=True)
            k.execute(f"SET synchronous_commit = {mode}")
            while time.time() < stop:
                k.execute("INSERT INTO tickets (subject) VALUES ('x')")
                counts[i] += 1
        ts = [threading.Thread(target=w, args=(i,)) for i in range(clients)]
        for t in ts: t.start()
        for t in ts: t.join()
        with c() as k:
            syncs = k.execute("SELECT wal_sync FROM pg_stat_wal").fetchone()[0]
        commits = sum(counts)
        return {"mode": mode, "clients": clients, "commits_per_s": round(commits / seconds),
                "avg_commit_ms": round(seconds * clients / commits * 1000, 2), "wal_syncs": syncs,
                "commits_per_sync": round(commits / max(syncs, 1), 1)}
    bench_rows = [bench(m, n) for m, n in (("on", 1), ("off", 1), ("on", 8), ("on", 32))]
    data["commit_bench"] = bench_rows
    on1, off1, on8, on32 = bench_rows
    report("Commit latency: synchronous_commit on vs off (1 client)", "off is faster because COMMIT doesn't wait for the WAL flush",
           f"on: {on1['avg_commit_ms']} ms/commit, {on1['commits_per_s']}/s; off: {off1['avg_commit_ms']} ms, {off1['commits_per_s']}/s",
           off1["commits_per_s"] > on1["commits_per_s"])
    report("Group commit: more clients share each WAL flush",
           "commits per WAL sync rise with concurrency",
           f"commits per sync: 1 client={on1['commits_per_sync']}, 8={on8['commits_per_sync']}, 32={on32['commits_per_sync']}",
           on32["commits_per_sync"] > on1["commits_per_sync"])

    # 4. Full-page writes: the first change to a page after a checkpoint logs the whole page
    with c() as k:
        k.execute("TRUNCATE tickets")
        k.execute("INSERT INTO tickets (subject) SELECT 'row ' || g FROM generate_series(1, 1000) g")
        k.execute("CHECKPOINT")
        lsn0 = k.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        k.execute("UPDATE tickets SET subject = 'changed' WHERE id = (SELECT min(id) FROM tickets)")
        lsn1 = k.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        k.execute("UPDATE tickets SET subject = 'changed again' WHERE id = (SELECT min(id) FROM tickets)")
        lsn2 = k.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        first = k.execute("SELECT pg_wal_lsn_diff(%s, %s)", (lsn1, lsn0)).fetchone()[0]
        second = k.execute("SELECT pg_wal_lsn_diff(%s, %s)", (lsn2, lsn1)).fetchone()[0]
    data["full_page_write_bytes"] = {"first_update_after_checkpoint": int(first), "second_update": int(second)}
    report("Full-page writes after a checkpoint", "the first update of a page after a checkpoint writes far more WAL",
           f"first update: {int(first)} bytes of WAL; same update again: {int(second)} bytes", first > 4 * second)

    # 5. Recovery time grows with the WAL since the last checkpoint
    rec = {}
    for label, checkpoint in (("checkpoint just before crash", True), ("~700 MB of WAL since checkpoint", False)):
        with c() as k:
            k.execute("TRUNCATE tickets"); k.execute("CHECKPOINT")
            lsn0 = k.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
            for _ in range(6):
                k.execute("INSERT INTO tickets (subject, body) SELECT 'bulk', repeat(md5(g::text), 8) FROM generate_series(1, 300000) g")
            if checkpoint:
                k.execute("CHECKPOINT")
            wal = k.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (lsn0,)).fetchone()[0]
        secs, logs = crash_and_restart()
        found = re.findall(r"redo done at .*?elapsed: ([\d.]+) s", logs)
        redo = float(found[-1]) if found else 0.0  # the last match belongs to this crash
        rec[label] = {"wal_mb_generated": round(wal / 2**20), "redo_seconds": redo, "restart_seconds": round(secs, 1)}
        print("  ", label, rec[label], flush=True)
    data["recovery"] = rec
    a, b = rec["checkpoint just before crash"], rec["~700 MB of WAL since checkpoint"]
    report("Recovery time vs distance from the last checkpoint", "more WAL to replay → longer recovery",
           f"after a checkpoint: redo {a['redo_seconds']} s, restart {a['restart_seconds']} s; "
           f"{b['wal_mb_generated']} MB since checkpoint: redo {b['redo_seconds']} s, restart {b['restart_seconds']} s",
           b["redo_seconds"] > a["redo_seconds"] and b["restart_seconds"] > a["restart_seconds"])

    # 6. An unread replication slot retains WAL until max_slot_wal_keep_size invalidates it
    with c() as k:
        k.execute("SELECT pg_create_logical_replication_slot('cdc_export', 'pgoutput')")
        k.execute("TRUNCATE tickets"); k.execute("CHECKPOINT")
        sizes = []
        for i in range(4):
            k.execute("INSERT INTO tickets (subject, body) SELECT 'x', repeat(md5(g::text), 8) FROM generate_series(1, 60000) g")
            r = k.execute("""SELECT wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
                             FROM pg_replication_slots WHERE slot_name = 'cdc_export'""").fetchone()
            sizes.append(r)
        k.execute("CHECKPOINT")
        k.execute("INSERT INTO tickets (subject) VALUES ('after')"); k.execute("CHECKPOINT")
        final = k.execute("SELECT wal_status, invalidation_reason FROM pg_replication_slots WHERE slot_name='cdc_export'").fetchone()
    data["slot"] = {"growth": sizes, "final": final}
    report("Unread replication slot with max_slot_wal_keep_size = 64MB",
           "retained WAL grows while nobody reads the slot; past the limit the slot is invalidated (wal_status=lost)",
           f"retained over time: {sizes}; after checkpoint: {final}", final[0] == "lost")

    with open(__file__.replace("wal_tests.py", "wal_results.json"), "w") as f:
        json.dump(data, f, indent=1, default=str)
    sh(f"docker rm -f {NAME}; docker volume rm -f ul-wal-data")
    print(f"{sum(results)}/{len(results)} passed")


if __name__ == "__main__":
    main()
