"""Replays the MVCC behaviors from Part 3 against real PostgreSQL and MySQL.

PostgreSQL runs with autovacuum=off so the measurements are deterministic; VACUUM is run by hand.
"""
import os
import re
import threading
import time

import psycopg
import pymysql

results = []
RESULTS = {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n")


def pg():
    return psycopg.connect(f"host=localhost port={os.environ.get('PGPORT_LAB', '55432')} user=postgres password=test", autocommit=True)


def one(c, sql, args=None):
    return c.execute(sql, args).fetchone()


def pg_reset(rows=50_000):
    with pg() as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
        c.execute("DROP TABLE IF EXISTS tickets")
        c.execute("""CREATE TABLE tickets (id int PRIMARY KEY, workspace_id int, assignee_id int,
                     state text, priority text, updated_at timestamptz, subject text)""")
        c.execute("""INSERT INTO tickets SELECT g, g %% 50, g %% 400, 'open', 'normal', now(), repeat('x', 120)
                     FROM generate_series(1, %s) g""", (rows,))
        c.execute("CREATE INDEX tickets_queue_idx ON tickets (workspace_id, assignee_id, updated_at)")
        c.execute("VACUUM (ANALYZE) tickets")


def pg_sizes(c):
    t = one(c, "SELECT pg_relation_size('tickets')")[0]
    i = one(c, "SELECT pg_relation_size('tickets_queue_idx')")[0]
    dead = one(c, "SELECT dead_tuple_count FROM pgstattuple('tickets')")[0]
    live = one(c, "SELECT tuple_count FROM pgstattuple('tickets')")[0]
    return t, i, live, dead


def mb(n):
    return f"{n / 1024 / 1024:.1f} MB"


def vacuum_report(c):
    """Runs VACUUM VERBOSE and returns (removed, not_yet_removable) from its INFO output."""
    msgs = []
    c.add_notice_handler(lambda d: msgs.append(d.message_primary + " " + (d.message_detail or "")))
    c.execute("VACUUM (VERBOSE) tickets")
    text = " ".join(msgs)
    m = re.search(r"tuples: (\d+) removed, (\d+) remain, (\d+) are dead but not yet removable", text)
    return (int(m.group(1)), int(m.group(3))) if m else (None, None)


def churn(c, rounds=3):
    for _ in range(rounds):
        c.execute("UPDATE tickets SET updated_at = now(), priority = 'urgent'")


def pg_tests():
    pg_reset()
    a, b = pg(), pg()

    # 1. Readers don't wait for writers
    a.execute("BEGIN"); a.execute("UPDATE tickets SET priority = 'urgent' WHERE id = 1")
    t0 = time.time()
    seen = one(b, "SELECT priority FROM tickets WHERE id = 1")[0]
    dt = time.time() - t0
    a.execute("ROLLBACK")
    report("PG: a reader doesn't wait for an uncommitted writer",
           "the SELECT returns the old committed value immediately", f"saw={seen} in {dt*1000:.1f} ms",
           seen == "normal" and dt < 0.2)

    # 2. Writers don't wait for readers
    a.execute("BEGIN ISOLATION LEVEL REPEATABLE READ"); one(a, "SELECT priority FROM tickets WHERE id = 1")
    t0 = time.time(); b.execute("UPDATE tickets SET priority = 'urgent' WHERE id = 1"); dt = time.time() - t0
    old = one(a, "SELECT priority FROM tickets WHERE id = 1")[0]
    a.execute("COMMIT")
    report("PG: a writer doesn't wait for a reader with an open snapshot",
           "UPDATE commits immediately; the reader still sees its snapshot version",
           f"update took {dt*1000:.1f} ms, reader saw={old}", dt < 0.2 and old == "normal")

    # 3. Writers still wait for writers
    a.execute("BEGIN"); a.execute("UPDATE tickets SET priority = 'low' WHERE id = 2")
    out = {}

    def w():
        t0 = time.time(); b.execute("UPDATE tickets SET priority = 'high' WHERE id = 2"); out["dt"] = time.time() - t0
    th = threading.Thread(target=w); th.start(); time.sleep(0.8); blocked = th.is_alive()
    a.execute("COMMIT"); th.join()
    report("PG: MVCC is not 'no locks' — two writers on one row still queue",
           "the second UPDATE waits until the first commits", f"blocked={blocked}, waited {out['dt']:.2f} s",
           blocked and out["dt"] > 0.7)

    # 4. An UPDATE writes a new row version somewhere else
    before = one(a, "SELECT ctid, xmin FROM tickets WHERE id = 3")
    a.execute("UPDATE tickets SET subject = 'changed' WHERE id = 3")
    after = one(a, "SELECT ctid, xmin FROM tickets WHERE id = 3")
    report("PG: UPDATE creates a new version (new ctid, new xmin)",
           "physical location and creating transaction id both change",
           f"ctid {before[0]} → {after[0]}, xmin {before[1]} → {after[1]}", before != after)

    # 5. Dead versions accumulate and VACUUM reclaims them
    pg_reset()
    c = pg()
    t1, i1, live1, dead1 = pg_sizes(c)
    churn(c, 3)
    t2, i2, live2, dead2 = pg_sizes(c)
    removed, pinned = vacuum_report(c)
    t3, i3, live3, dead3 = pg_sizes(c)
    c.execute("VACUUM FULL tickets")
    t4, i4, _, _ = pg_sizes(c)
    report("PG: UPDATEs grow the table; VACUUM frees space for reuse but doesn't shrink the file",
           "table grows several times over; VACUUM removes the remaining dead versions; only VACUUM FULL gives space back",
           f"table {mb(t1)} → {mb(t2)}, index {mb(i1)} → {mb(i2)}; dead versions still present before VACUUM={dead2} "
           f"(the rest were pruned in passing); VACUUM removed={removed}; after VACUUM table={mb(t3)}; "
           f"after VACUUM FULL table={mb(t4)} index={mb(i4)}",
           t2 > 3 * t1 and removed and dead3 == 0 and t3 == t2 and t4 < t2 / 2)

    # 6. Long transactions pin old versions — which kinds?
    for label, setup, blocks in (
        ("Repeatable Read, read-only, idle", ["BEGIN ISOLATION LEVEL REPEATABLE READ", "SELECT count(*) FROM tickets WHERE id < 10"], True),
        ("Read Committed, read-only, idle", ["BEGIN", "SELECT count(*) FROM tickets WHERE id < 10"], False),
        ("Read Committed, UPDATE that matched nothing, idle", ["BEGIN", "UPDATE tickets SET priority = 'x' WHERE id = 99999999"], False),
        ("Read Committed, has a transaction id (wrote something), idle", ["BEGIN", "SELECT txid_current()"], True),
    ):
        pg_reset()
        holder, c = pg(), pg()
        for s in setup:
            holder.execute(s)
        state = one(c, "SELECT state, backend_xmin IS NOT NULL, backend_xid IS NOT NULL FROM pg_stat_activity WHERE pid = %s",
                    (holder.info.backend_pid,))
        churn(c, 3)
        removed, pinned = vacuum_report(c)
        t, i, live, dead = pg_sizes(c)
        holder.execute("COMMIT")
        removed2, pinned2 = vacuum_report(c)
        report(f"PG: idle session — {label}",
               "blocks VACUUM" if blocks else "does not block VACUUM",
               f"state={state[0]} holds_xmin={state[1]} holds_xid={state[2]} → VACUUM removed={removed} "
               f"pinned={pinned}, table={mb(t)}; after COMMIT: removed={removed2}",
               (removed == 0 and removed2 > 0) if blocks else removed > 0)
        RESULTS[label] = {"blocks": blocks, "removed": removed, "pinned": pinned, "removed_after_commit": removed2, "table_mb": mb(t)}


def my():
    return pymysql.connect(host="127.0.0.1", port=53306, user="root", password="test", database="test", autocommit=True)


def my_q(c, sql, args=None):
    cur = c.cursor(); cur.execute(sql, args); return cur.fetchall()


def history_length(c):
    out = my_q(c, "SELECT count FROM information_schema.innodb_metrics WHERE name = 'trx_rseg_history_len'")
    return out[0][0]


def my_tests():
    c = my()
    my_q(c, "SET GLOBAL innodb_monitor_enable = 'trx_rseg_history_len'")
    my_q(c, "SET SESSION cte_max_recursion_depth = 100000")
    my_q(c, "DROP TABLE IF EXISTS tickets")
    my_q(c, "CREATE TABLE tickets (id int PRIMARY KEY, priority varchar(10)) ENGINE=InnoDB")
    my_q(c, "INSERT INTO tickets WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 20000) "
            "SELECT n, 'normal' FROM s")
    a, b = my(), my()

    a.cursor().execute("START TRANSACTION"); my_q(a, "UPDATE tickets SET priority='urgent' WHERE id=1")
    t0 = time.time(); seen = my_q(b, "SELECT priority FROM tickets WHERE id=1")[0][0]; dt = time.time() - t0
    my_q(a, "ROLLBACK")
    report("MySQL: a plain SELECT doesn't wait for an uncommitted writer",
           "returns the old value immediately (read from the undo log)", f"saw={seen} in {dt*1000:.1f} ms",
           seen == "normal" and dt < 0.2)

    time.sleep(2)
    base = history_length(c)
    my_q(a, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
    for i in range(3000):  # many small transactions, like real traffic (history length counts transactions)
        my_q(b, "UPDATE tickets SET priority = IF(priority='normal','urgent','normal') WHERE id = %s", (i % 20000 + 1,))
    time.sleep(3)
    held = history_length(c)
    my_q(a, "COMMIT")
    waited = 0
    for _ in range(90):  # purge runs lazily in the background
        time.sleep(1); waited += 1
        after = history_length(c)
        if after < held / 4:
            break
    report("MySQL: an open snapshot stops purge — history list grows, then drains",
           "history length jumps while the old snapshot is open, and drops after it commits",
           f"history length {base} → {held} while open → {after} {waited} s after commit", held > base + 2500 and after < held / 4)


if __name__ == "__main__":
    import json
    import sys
    pg_tests()
    if "--pg-only" not in sys.argv:
        my_tests()
    with open(__file__.replace("mvcc_tests.py", "mvcc_tests_results.json"), "w") as fh:
        json.dump({"idle_sessions": RESULTS}, fh, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
