"""Replays the write-skew scenarios from Part 4 against real PostgreSQL and MySQL."""
import threading
import time

import psycopg
import pymysql

results = []


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n")


# ------------------------------------------------------------------ PostgreSQL
def pg():
    return psycopg.connect("host=localhost port=55432 user=postgres password=test", autocommit=True)


def pg_setup():
    with pg() as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
        c.execute("DROP TABLE IF EXISTS members, workspaces, tickets, agents, shifts")
        c.execute("CREATE TABLE workspaces (id int PRIMARY KEY, admin_count int CHECK (admin_count >= 1))")
        c.execute("INSERT INTO workspaces VALUES (7, 2)")
        c.execute("CREATE TABLE members (workspace_id int, user_id text, role text, PRIMARY KEY (workspace_id, user_id))")
        c.execute("INSERT INTO members VALUES (7,'maya','admin'), (7,'omar','admin'), (7,'li','agent')")
        c.execute("CREATE TABLE tickets (id int PRIMARY KEY, assignee_id int, state text)")
        c.execute("CREATE INDEX ON tickets (assignee_id) WHERE state <> 'closed'")
        c.execute("INSERT INTO tickets SELECT g, CASE WHEN g <= 24 THEN 31 END, 'open' FROM generate_series(1, 40) g")
        c.execute("CREATE TABLE agents (id int PRIMARY KEY, open_limit int)")
        c.execute("INSERT INTO agents VALUES (31, 25)")
        c.execute("""CREATE TABLE shifts (agent_id int, during tstzrange,
                     EXCLUDE USING gist (agent_id WITH =, during WITH &&))""")


def admins_pg():
    with pg() as c:
        return c.execute("SELECT count(*) FROM members WHERE workspace_id = 7 AND role = 'admin'").fetchone()[0]


def pg_last_admin(level):
    pg_setup()
    a, b = pg(), pg()
    errs = []
    for conn in (a, b):
        conn.execute(f"BEGIN ISOLATION LEVEL {level}")
    na = a.execute("SELECT count(*) FROM members WHERE workspace_id = 7 AND role = 'admin'").fetchone()[0]
    nb = b.execute("SELECT count(*) FROM members WHERE workspace_id = 7 AND role = 'admin'").fetchone()[0]
    for conn, who in ((a, "maya"), (b, "omar")):
        try:
            conn.execute("UPDATE members SET role = 'agent' WHERE workspace_id = 7 AND user_id = %s", (who,))
        except psycopg.Error as e:
            errs.append(e.sqlstate); conn.execute("ROLLBACK")
    for conn in (a, b):
        try:
            conn.execute("COMMIT")
        except psycopg.Error as e:
            errs.append(e.sqlstate)
    return na, nb, errs, admins_pg()


def pg_tests():
    for level in ("READ COMMITTED", "REPEATABLE READ"):
        na, nb, errs, left = pg_last_admin(level)
        report(f"PG {level}: two admins step down at once",
               "both see 2 admins, each updates only their own row, both commit → 0 admins",
               f"each saw {na},{nb} admins; errors={errs}; admins left={left}", na == nb == 2 and not errs and left == 0)

    na, nb, errs, left = pg_last_admin("SERIALIZABLE")
    report("PG SERIALIZABLE: two admins step down at once",
           "one transaction is aborted with 40001 → 1 admin left",
           f"errors={errs}; admins left={left}", "40001" in errs and left == 1)

    # Fix: lock the rows the rule depends on (SELECT ... FOR UPDATE), Read Committed
    pg_setup()
    a, b = pg(), pg()
    a.execute("BEGIN")
    rows_a = a.execute("SELECT user_id FROM members WHERE workspace_id=7 AND role='admin' FOR UPDATE").fetchall()
    out = {}

    def run_b():
        b.execute("BEGIN")
        out["rows"] = b.execute("SELECT user_id FROM members WHERE workspace_id=7 AND role='admin' FOR UPDATE").fetchall()
        if len(out["rows"]) > 1:
            b.execute("UPDATE members SET role='agent' WHERE workspace_id=7 AND user_id='omar'")
        b.execute("COMMIT")
    t = threading.Thread(target=run_b); t.start(); time.sleep(0.7); blocked = t.is_alive()
    a.execute("UPDATE members SET role='agent' WHERE workspace_id=7 AND user_id='maya'"); a.execute("COMMIT"); t.join()
    report("PG fix: SELECT ... FOR UPDATE on the admin rows",
           "Omar's check waits for Maya, then sees only 1 admin and refuses",
           f"Maya saw {len(rows_a)}; Omar blocked={blocked}, then saw {[r[0] for r in out['rows']]}; admins left={admins_pg()}",
           blocked and len(out["rows"]) == 1 and admins_pg() == 1)

    # Fix: materialize the rule on one row (counter + CHECK)
    pg_setup()
    a, b = pg(), pg()
    a.execute("BEGIN")
    a.execute("UPDATE members SET role='agent' WHERE workspace_id=7 AND user_id='maya'")
    a.execute("UPDATE workspaces SET admin_count = admin_count - 1 WHERE id = 7")
    out = {}

    def run_b2():
        b.execute("BEGIN")
        b.execute("UPDATE members SET role='agent' WHERE workspace_id=7 AND user_id='omar'")
        try:
            b.execute("UPDATE workspaces SET admin_count = admin_count - 1 WHERE id = 7"); b.execute("COMMIT"); out["err"] = None
        except psycopg.Error as e:
            out["err"] = e.sqlstate; b.execute("ROLLBACK")
    t = threading.Thread(target=run_b2); t.start(); time.sleep(0.7); blocked = t.is_alive()
    a.execute("COMMIT"); t.join()
    report("PG fix: admin_count counter with CHECK (admin_count >= 1)",
           "the second decrement waits on the workspace row, then fails the CHECK (23514)",
           f"blocked={blocked} error={out['err']} admins left={admins_pg()}", blocked and out["err"] == "23514" and admins_pg() == 1)

    # Workload cap: different ticket rows, rule over the agent's open tickets
    pg_setup()
    a, b = pg(), pg()
    for conn in (a, b):
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    ca = a.execute("SELECT count(*) FROM tickets WHERE assignee_id=31 AND state<>'closed'").fetchone()[0]
    cb = b.execute("SELECT count(*) FROM tickets WHERE assignee_id=31 AND state<>'closed'").fetchone()[0]
    a.execute("UPDATE tickets SET assignee_id=31 WHERE id=30"); b.execute("UPDATE tickets SET assignee_id=31 WHERE id=31")
    a.execute("COMMIT"); b.execute("COMMIT")
    with pg() as c:
        total = c.execute("SELECT count(*) FROM tickets WHERE assignee_id=31 AND state<>'closed'").fetchone()[0]
    report("PG REPEATABLE READ: two leads assign different tickets to an agent at 24/25",
           "both see 24, both commit → 26 open tickets", f"saw {ca},{cb} → {total}", ca == cb == 24 and total == 26)

    # Locking existing rows does NOT stop a new row in PostgreSQL (phantom)
    pg_setup()
    a, b = pg(), pg()
    a.execute("BEGIN")
    ra = a.execute("SELECT id FROM tickets WHERE assignee_id=31 AND state<>'closed' FOR UPDATE").fetchall()
    out = {}

    def run_b3():
        b.execute("BEGIN")
        out["n"] = len(b.execute("SELECT id FROM tickets WHERE assignee_id=31 AND state<>'closed' FOR UPDATE").fetchall())
        b.execute("COMMIT")
    t = threading.Thread(target=run_b3); t.start(); time.sleep(0.7); blocked = t.is_alive()
    a.execute("UPDATE tickets SET assignee_id=31 WHERE id=30"); a.execute("COMMIT"); t.join()
    report("PG: FOR UPDATE on the agent's existing tickets vs a newly assigned one",
           "Lead 2 waits, but its re-check doesn't see the ticket Lead 1 just added (24, not 25)",
           f"Lead 1 locked {len(ra)} rows; Lead 2 blocked={blocked}, then counted {out['n']}", blocked and out["n"] == 24)

    # Fix for PostgreSQL: lock the agent's own row (the shared parent), then count
    pg_setup()
    a, b = pg(), pg()
    a.execute("BEGIN"); a.execute("SELECT 1 FROM agents WHERE id = 31 FOR UPDATE")
    ca = a.execute("SELECT count(*) FROM tickets WHERE assignee_id=31 AND state<>'closed'").fetchone()[0]
    out = {}

    def run_b5():
        b.execute("BEGIN"); b.execute("SELECT 1 FROM agents WHERE id = 31 FOR UPDATE")
        out["n"] = b.execute("SELECT count(*) FROM tickets WHERE assignee_id=31 AND state<>'closed'").fetchone()[0]
        b.execute("COMMIT")
    t = threading.Thread(target=run_b5); t.start(); time.sleep(0.7); blocked = t.is_alive()
    a.execute("UPDATE tickets SET assignee_id=31 WHERE id=30"); a.execute("COMMIT"); t.join()
    report("PG fix: lock the agent row first, then count",
           "Lead 2 waits on the agent row, then counts 25 and refuses",
           f"Lead 1 counted {ca}; Lead 2 blocked={blocked}, then counted {out['n']}", blocked and out["n"] == 25)

    # Exclusion constraint: overlapping shifts
    pg_setup()
    a, b = pg(), pg()
    a.execute("BEGIN"); a.execute("INSERT INTO shifts VALUES (31, '[2026-11-02 09:00, 2026-11-02 17:00)')")
    out = {}

    def run_b4():
        b.execute("BEGIN")
        try:
            b.execute("INSERT INTO shifts VALUES (31, '[2026-11-02 13:00, 2026-11-02 21:00)')"); b.execute("COMMIT"); out["err"] = None
        except psycopg.Error as e:
            out["err"] = e.sqlstate; b.execute("ROLLBACK")
    t = threading.Thread(target=run_b4); t.start(); time.sleep(0.7); blocked = t.is_alive()
    a.execute("COMMIT"); t.join()
    report("PG fix: EXCLUDE constraint on overlapping shifts",
           "the overlapping insert waits, then fails with 23P01 (exclusion_violation)",
           f"blocked={blocked} error={out['err']}", blocked and out["err"] == "23P01")


# ------------------------------------------------------------------ MySQL
def my():
    return pymysql.connect(host="127.0.0.1", port=53306, user="root", password="test", database="test", autocommit=True)


def q(c, sql, args=None):
    cur = c.cursor(); cur.execute(sql, args); return cur.fetchall()


def my_setup():
    c = my()
    q(c, "DROP TABLE IF EXISTS members, tickets")
    q(c, "CREATE TABLE members (workspace_id int, user_id varchar(20), role varchar(10), PRIMARY KEY (workspace_id, user_id)) ENGINE=InnoDB")
    q(c, "INSERT INTO members VALUES (7,'maya','admin'), (7,'omar','admin'), (7,'li','agent')")
    q(c, "CREATE TABLE tickets (id int PRIMARY KEY, assignee_id int, KEY (assignee_id)) ENGINE=InnoDB")
    for i in range(1, 41):
        q(c, "INSERT INTO tickets VALUES (%s, %s)", (i, 31 if i <= 24 else None))
    c.close()


def admins_my():
    c = my(); n = q(c, "SELECT count(*) FROM members WHERE workspace_id=7 AND role='admin'")[0][0]; c.close(); return n


def my_last_admin(level):
    my_setup()
    a, b = my(), my()
    errs = []
    for conn in (a, b):
        q(conn, f"SET SESSION TRANSACTION ISOLATION LEVEL {level}"); q(conn, "START TRANSACTION")
    na = q(a, "SELECT count(*) FROM members WHERE workspace_id=7 AND role='admin'")[0][0]
    nb = q(b, "SELECT count(*) FROM members WHERE workspace_id=7 AND role='admin'")[0][0]
    out = {}

    def upd(conn, who, key):
        try:
            q(conn, "UPDATE members SET role='agent' WHERE workspace_id=7 AND user_id=%s", (who,)); q(conn, "COMMIT")
        except pymysql.MySQLError as e:
            out[key] = e.args[0]
    ta = threading.Thread(target=upd, args=(a, "maya", "a")); tb = threading.Thread(target=upd, args=(b, "omar", "b"))
    ta.start(); tb.start(); ta.join(10); tb.join(10)
    return na, nb, out, admins_my()


def my_tests():
    na, nb, errs, left = my_last_admin("REPEATABLE READ")
    report("MySQL REPEATABLE READ (default): two admins step down at once",
           "both see 2 admins, both commit → 0 admins", f"saw {na},{nb}; errors={errs}; admins left={left}",
           na == nb == 2 and not errs and left == 0)

    na, nb, errs, left = my_last_admin("SERIALIZABLE")
    report("MySQL SERIALIZABLE: two admins step down at once",
           "plain reads take shared locks, so the two updates deadlock and one is killed (1213) → 1 admin",
           f"errors={errs}; admins left={left}", 1213 in errs.values() and left == 1)

    # MySQL's FOR UPDATE locks the index range (next-key locks), so it does block the new row
    my_setup()
    a, b = my(), my()
    q(a, "START TRANSACTION")
    ra = q(a, "SELECT id FROM tickets WHERE assignee_id=31 FOR UPDATE")
    out = {}

    def run_b():
        q(b, "START TRANSACTION")
        out["n"] = len(q(b, "SELECT id FROM tickets WHERE assignee_id=31 FOR UPDATE"))
        q(b, "COMMIT")
    t = threading.Thread(target=run_b); t.start(); time.sleep(0.7); blocked = t.is_alive()
    q(a, "UPDATE tickets SET assignee_id=31 WHERE id=30"); q(a, "COMMIT"); t.join()
    report("MySQL: FOR UPDATE on the agent's existing tickets vs a newly assigned one",
           "Lead 2 waits, and its locking read sees the new ticket (25) — InnoDB locks the index range",
           f"Lead 1 locked {len(ra)} rows; Lead 2 blocked={blocked}, then counted {out['n']}", blocked and out["n"] == 25)
    # The 25 above would appear without gap locks too: a locking read in InnoDB reads the latest committed rows.
    # The real test of next-key locks: can another transaction put a NEW row into the locked range while Lead 1 holds it?
    my_setup()
    a, b = my(), my()
    plan = q(a, "EXPLAIN SELECT id FROM tickets WHERE assignee_id=31 FOR UPDATE")
    q(a, "START TRANSACTION")
    q(a, "SELECT id FROM tickets WHERE assignee_id=31 FOR UPDATE")
    res = {}

    def assign():
        q(b, "SET innodb_lock_wait_timeout = 3")
        q(b, "START TRANSACTION")
        t0 = time.time()
        try:
            q(b, "UPDATE tickets SET assignee_id=31 WHERE id=31")
            res["outcome"] = "assigned"
        except pymysql.err.OperationalError as e:
            res["outcome"] = f"error {e.args[0]}"
        res["waited_s"] = round(time.time() - t0, 2)
        q(b, "ROLLBACK")
    t = threading.Thread(target=assign); t.start(); t.join()
    q(a, "ROLLBACK")
    pg_setup()                            # the same on PostgreSQL: FOR UPDATE locks rows, not ranges
    pa, pb = psycopg.connect("host=localhost port=55432 user=postgres password=test"), psycopg.connect("host=localhost port=55432 user=postgres password=test")
    pa.execute("SELECT id FROM tickets WHERE assignee_id=31 FOR UPDATE")
    pb.execute("SET lock_timeout = '3s'")
    t0 = time.time()
    try:
        pb.execute("UPDATE tickets SET assignee_id=31 WHERE id=31"); pg_outcome = "assigned"
    except psycopg.errors.LockNotAvailable:
        pg_outcome = "lock timeout"
    pg_wait = round(time.time() - t0, 2)
    pb.rollback(); pa.rollback(); pa.close(); pb.close()
    report("Next-key locks: a new row enters the range Lead 1 locked with FOR UPDATE",
           "MySQL blocks the insert into the locked index range (gap lock); PostgreSQL doesn't lock ranges",
           f"MySQL: {res['outcome']} after {res['waited_s']} s (plan uses key {plan[0][6] if plan else '?'}) · PostgreSQL: {pg_outcome} after {pg_wait} s",
           res["outcome"].startswith("error 1205") and pg_outcome == "assigned")


if __name__ == "__main__":
    pg_tests()
    my_tests()
    print(f"{sum(results)}/{len(results)} passed")
