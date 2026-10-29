"""Replays the concurrency scenarios from Part 2 against real PostgreSQL and MySQL,
interleaving two connections step by step."""
import json
import threading
import time
from pathlib import Path

import psycopg
import pymysql

results = []
observed_rows = []


def report(name, claim, observed, ok):
    results.append((name, ok))
    observed_rows.append({"name": name, "claim": claim, "observed": observed, "ok": ok})
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n")



# ---------------------------------------------------------------- PostgreSQL
def pg():
    c = psycopg.connect("host=localhost port=55432 user=postgres password=test", autocommit=True)
    return c


def pg_reset():
    with pg() as c:
        c.execute("DROP TABLE IF EXISTS tickets, agents, workspaces")
        c.execute("CREATE TABLE tickets (id int PRIMARY KEY, assignee_id text, priority text, state text, version int)")
        c.execute("INSERT INTO tickets VALUES (48213, NULL, 'normal', 'open', 7)")
        c.execute("CREATE TABLE workspaces (id int PRIMARY KEY, seats_used int, seat_limit int, CHECK (seats_used <= seat_limit))")
        c.execute("INSERT INTO workspaces VALUES (7, 49, 50)")
        c.execute("CREATE TABLE agents (id serial PRIMARY KEY, workspace_id int, email text)")
        c.execute("INSERT INTO agents (workspace_id, email) SELECT 7, 'a' || g FROM generate_series(1, 49) g")


def pg_val(sql):
    with pg() as c:
        return c.execute(sql).fetchone()[0]


def wait_until_blocked(thread, timeout=2.0):
    """True only if the other session is actually waiting on a lock (not just 'still starting')."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with pg() as c:
            n = c.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE wait_event_type = 'Lock' AND state = 'active'"
            ).fetchone()[0]
        if n >= 1:
            return True
        if not thread.is_alive():
            return False
        time.sleep(0.05)
    return thread.is_alive()  # last resort; prefer Lock wait_event above


def pg_lost_update(level):
    pg_reset()
    a, b = pg(), pg()
    a.execute(f"BEGIN ISOLATION LEVEL {level}")
    b.execute(f"BEGIN ISOLATION LEVEL {level}")
    ra = a.execute("SELECT assignee_id FROM tickets WHERE id = 48213").fetchone()[0]
    rb = b.execute("SELECT assignee_id FROM tickets WHERE id = 48213").fetchone()[0]
    a.execute("UPDATE tickets SET assignee_id = 'A' WHERE id = 48213"); a.execute("COMMIT")
    err = None
    try:
        b.execute("UPDATE tickets SET assignee_id = 'B' WHERE id = 48213"); b.execute("COMMIT")
    except psycopg.Error as e:
        err = f"{e.sqlstate}: {str(e).strip().splitlines()[0]}"
        b.execute("ROLLBACK")
    return ra, rb, err, pg_val("SELECT assignee_id FROM tickets WHERE id = 48213")


def pg_tests():
    ra, rb, err, final = pg_lost_update("READ COMMITTED")
    report("PG Read Committed: original Take ticket code",
           "both reads see NULL, no error, final assignee = B (A's write lost)",
           f"reads={ra},{rb} error={err} final={final}", ra is None and rb is None and err is None and final == "B")

    ra, rb, err, final = pg_lost_update("REPEATABLE READ")
    report("PG Repeatable Read: original code",
           "B's UPDATE fails with 40001 'could not serialize access due to concurrent update', final = A",
           f"error={err} final={final}", err and err.startswith("40001") and "concurrent update" in err and final == "A")

    ra, rb, err, final = pg_lost_update("SERIALIZABLE")
    report("PG Serializable: original code", "B fails with 40001, final = A",
           f"error={err} final={final}", err and err.startswith("40001") and final == "A")

    # Fix 1: atomic conditional UPDATE at Read Committed, truly concurrent
    pg_reset()
    a, b = pg(), pg()
    a.execute("BEGIN")
    n_a = a.execute("UPDATE tickets SET assignee_id = 'A' WHERE id = 48213 AND assignee_id IS NULL").rowcount
    out = {}

    def run_b():
        b.execute("BEGIN")
        out["n"] = b.execute("UPDATE tickets SET assignee_id = 'B' WHERE id = 48213 AND assignee_id IS NULL").rowcount
        b.execute("COMMIT")
    t = threading.Thread(target=run_b); t.start()
    blocked = wait_until_blocked(t)
    a.execute("COMMIT"); t.join()
    final = pg_val("SELECT assignee_id FROM tickets WHERE id = 48213")
    report("PG fix 1: atomic UPDATE ... AND assignee_id IS NULL (Read Committed)",
           "B waits for A's lock, then re-checks the WHERE and updates 0 rows; final = A",
           f"A rows={n_a} B blocked={blocked} B rows={out['n']} final={final}",
           n_a == 1 and blocked and out["n"] == 0 and final == "A")

    # Fix 2: SELECT ... FOR UPDATE
    pg_reset()
    a, b = pg(), pg()
    a.execute("BEGIN")
    a.execute("SELECT assignee_id FROM tickets WHERE id = 48213 FOR UPDATE")
    out = {}

    def run_b2():
        b.execute("BEGIN")
        out["seen"] = b.execute("SELECT assignee_id FROM tickets WHERE id = 48213 FOR UPDATE").fetchone()[0]
        b.execute("COMMIT")
    t = threading.Thread(target=run_b2); t.start()
    blocked = wait_until_blocked(t)
    a.execute("UPDATE tickets SET assignee_id = 'A' WHERE id = 48213"); a.execute("COMMIT"); t.join()
    report("PG fix 2: SELECT ... FOR UPDATE", "B's locking read waits, then sees the new value A",
           f"B blocked={blocked} B saw={out['seen']}", blocked and out["seen"] == "A")

    # Fix 3: version check
    pg_reset()
    a, b = pg(), pg()
    na = a.execute("UPDATE tickets SET assignee_id='A', version = version + 1 WHERE id = 48213 AND version = 7").rowcount
    nb = b.execute("UPDATE tickets SET assignee_id='B', version = version + 1 WHERE id = 48213 AND version = 7").rowcount
    report("PG fix 3: version check", "the second writer (stale version 7) updates 0 rows",
           f"A rows={na} B rows={nb} final={pg_val('SELECT assignee_id FROM tickets WHERE id=48213')}", na == 1 and nb == 0)

    # Seat limit (count then insert)
    for level, expect_51 in (("READ COMMITTED", True), ("REPEATABLE READ", True), ("SERIALIZABLE", False)):
        pg_reset()
        a, b = pg(), pg()
        a.execute(f"BEGIN ISOLATION LEVEL {level}"); b.execute(f"BEGIN ISOLATION LEVEL {level}")
        ca = a.execute("SELECT count(*) FROM agents WHERE workspace_id = 7").fetchone()[0]
        cb = b.execute("SELECT count(*) FROM agents WHERE workspace_id = 7").fetchone()[0]
        errs = []
        for conn, email in ((a, "x@h"), (b, "y@h")):
            try:
                conn.execute("INSERT INTO agents (workspace_id, email) VALUES (7, %s)", (email,))
            except psycopg.Error as e:
                errs.append(e.sqlstate); conn.execute("ROLLBACK")
        for conn in (a, b):
            try:
                conn.execute("COMMIT")
            except psycopg.Error as e:
                errs.append(e.sqlstate)
        total = pg_val("SELECT count(*) FROM agents WHERE workspace_id = 7")
        claim = "both count 49, both commit → 51 agents" if expect_51 else "one transaction fails with 40001 → 50 agents"
        ok = (total == 51 and not errs) if expect_51 else (total == 50 and "40001" in errs)
        report(f"PG seat limit at {level}", claim, f"counts={ca},{cb} errors={errs} total={total}", ok)

    # Seat fix: lock the workspace row
    pg_reset()
    a, b = pg(), pg()
    a.execute("BEGIN"); a.execute("SELECT 1 FROM workspaces WHERE id = 7 FOR UPDATE")
    ca = a.execute("SELECT count(*) FROM agents WHERE workspace_id = 7").fetchone()[0]
    out = {}

    def run_b3():
        b.execute("BEGIN"); b.execute("SELECT 1 FROM workspaces WHERE id = 7 FOR UPDATE")
        out["count"] = b.execute("SELECT count(*) FROM agents WHERE workspace_id = 7").fetchone()[0]
        b.execute("COMMIT")
    t = threading.Thread(target=run_b3); t.start()
    blocked = wait_until_blocked(t)
    a.execute("INSERT INTO agents (workspace_id, email) VALUES (7, 'x@h')"); a.execute("COMMIT"); t.join()
    report("PG seat fix: lock the workspace row", "admin 2 waits, then counts 50 and can refuse",
           f"admin1 count={ca} admin2 blocked={blocked} admin2 count={out['count']}", blocked and out["count"] == 50)

    # Seat fix: counter + CHECK
    pg_reset()
    a, b = pg(), pg()
    a.execute("BEGIN"); a.execute("UPDATE workspaces SET seats_used = seats_used + 1 WHERE id = 7")
    out = {}

    def run_b4():
        b.execute("BEGIN")
        try:
            b.execute("UPDATE workspaces SET seats_used = seats_used + 1 WHERE id = 7"); out["err"] = None
        except psycopg.Error as e:
            out["err"] = e.sqlstate
        b.execute("ROLLBACK")
    t = threading.Thread(target=run_b4); t.start()
    blocked = wait_until_blocked(t)
    a.execute("COMMIT"); t.join()
    report("PG seat fix: counter + CHECK (seats_used <= seat_limit)",
           "second increment waits, then fails the CHECK (23514)", f"blocked={blocked} error={out['err']}",
           blocked and out["err"] == "23514")

    # Read Uncommitted acts as Read Committed (no dirty reads)
    pg_reset()
    a, b = pg(), pg()
    a.execute("BEGIN"); a.execute("UPDATE tickets SET assignee_id = 'admin-bulk' WHERE id = 48213")
    b.execute("BEGIN ISOLATION LEVEL READ UNCOMMITTED")
    seen = b.execute("SELECT assignee_id FROM tickets WHERE id = 48213").fetchone()[0]
    b.execute("COMMIT"); a.execute("ROLLBACK")
    report("PG Read Uncommitted: dirty read?", "PostgreSQL never shows uncommitted data (sees NULL)",
           f"saw={seen}", seen is None)

    # Non-repeatable read and phantom: RC vs RR
    for level in ("READ COMMITTED", "REPEATABLE READ"):
        pg_reset()
        a, b = pg(), pg()
        a.execute(f"BEGIN ISOLATION LEVEL {level}")
        p1 = a.execute("SELECT priority FROM tickets WHERE id = 48213").fetchone()[0]
        n1 = a.execute("SELECT count(*) FROM tickets WHERE state = 'open'").fetchone()[0]
        b.execute("UPDATE tickets SET priority = 'urgent' WHERE id = 48213")
        b.execute("INSERT INTO tickets VALUES (48214, NULL, 'normal', 'open', 1)")
        p2 = a.execute("SELECT priority FROM tickets WHERE id = 48213").fetchone()[0]
        n2 = a.execute("SELECT count(*) FROM tickets WHERE state = 'open'").fetchone()[0]
        a.execute("COMMIT")
        rc = level == "READ COMMITTED"
        report(f"PG {level}: non-repeatable + phantom",
               "both anomalies visible" if rc else "neither visible (snapshot) — RR prevents phantoms too",
               f"priority {p1}→{p2}, open count {n1}→{n2}",
               (p1 != p2 and n1 != n2) if rc else (p1 == p2 and n1 == n2))


# ---------------------------------------------------------------- MySQL
def my():
    return pymysql.connect(host="127.0.0.1", port=53306, user="root", password="test", database="test", autocommit=True)


def my_reset():
    c = my(); cur = c.cursor()
    cur.execute("DROP TABLE IF EXISTS tickets, agents")
    cur.execute("CREATE TABLE tickets (id int PRIMARY KEY, assignee_id varchar(20), version int) ENGINE=InnoDB")
    cur.execute("INSERT INTO tickets VALUES (48213, NULL, 7)")
    cur.execute("CREATE TABLE agents (id int AUTO_INCREMENT PRIMARY KEY, workspace_id int, email varchar(50), KEY (workspace_id)) ENGINE=InnoDB")
    for i in range(49):
        cur.execute("INSERT INTO agents (workspace_id, email) VALUES (7, %s)", (f"a{i}",))
    c.close()


def my_q(conn, sql, args=None):
    cur = conn.cursor(); n = cur.execute(sql, args); return n, cur.fetchall()


def my_val(sql):
    c = my(); v = my_q(c, sql)[1][0][0]; c.close(); return v


def my_tests():
    my_reset()
    _, d = my_q(my(), "SELECT @@transaction_isolation")
    report("MySQL default isolation", "REPEATABLE-READ", d[0][0], d[0][0] == "REPEATABLE-READ")

    a, b = my(), my()
    for c in (a, b):
        my_q(c, "START TRANSACTION")
    ra = my_q(a, "SELECT assignee_id FROM tickets WHERE id = 48213")[1][0][0]
    rb = my_q(b, "SELECT assignee_id FROM tickets WHERE id = 48213")[1][0][0]
    my_q(a, "UPDATE tickets SET assignee_id = 'A' WHERE id = 48213"); my_q(a, "COMMIT")
    rb2 = my_q(b, "SELECT assignee_id FROM tickets WHERE id = 48213")[1][0][0]
    err = None
    try:
        my_q(b, "UPDATE tickets SET assignee_id = 'B' WHERE id = 48213"); my_q(b, "COMMIT")
    except pymysql.MySQLError as e:
        err = e.args
    final = my_val("SELECT assignee_id FROM tickets WHERE id = 48213")
    report("MySQL Repeatable Read (default): original code",
           "B's snapshot still says NULL, B's UPDATE succeeds with no error, final = B (lost update)",
           f"B re-read after A's commit={rb2} error={err} final={final}",
           ra is None and rb is None and rb2 is None and err is None and final == "B")

    # atomic UPDATE in MySQL
    my_reset()
    a, b = my(), my()
    my_q(a, "START TRANSACTION")
    na, _ = my_q(a, "UPDATE tickets SET assignee_id='A' WHERE id=48213 AND assignee_id IS NULL")
    out = {}

    def run_b():
        my_q(b, "START TRANSACTION")
        out["n"], _ = my_q(b, "UPDATE tickets SET assignee_id='B' WHERE id=48213 AND assignee_id IS NULL")
        my_q(b, "COMMIT")
    t = threading.Thread(target=run_b); t.start(); time.sleep(0.7)
    blocked = t.is_alive(); my_q(a, "COMMIT"); t.join()
    final = my_val("SELECT assignee_id FROM tickets WHERE id=48213")
    report("MySQL fix 1: atomic UPDATE ... AND assignee_id IS NULL",
           "B waits, then updates 0 rows; final = A",
           f"A rows={na} B blocked={blocked} B rows={out['n']} final={final}",
           na == 1 and blocked and out["n"] == 0 and final == "A")

    # Dirty read in MySQL Read Uncommitted
    my_reset()
    a, b = my(), my()
    my_q(a, "START TRANSACTION"); my_q(a, "UPDATE tickets SET assignee_id='admin-bulk' WHERE id=48213")
    my_q(b, "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    seen = my_q(b, "SELECT assignee_id FROM tickets WHERE id=48213")[1][0][0]
    my_q(a, "ROLLBACK")
    report("MySQL Read Uncommitted: dirty read", "B sees A's uncommitted value", f"saw={seen}", seen == "admin-bulk")


if __name__ == "__main__":
    pg_tests()
    my_tests()
    passed = sum(ok for _, ok in results)
    out = {"passed": passed, "total": len(results), "checks": observed_rows}
    Path(__file__).with_name("race_results.json").write_text(json.dumps(out, indent=2))
    print(f"{passed}/{len(results)} passed")

