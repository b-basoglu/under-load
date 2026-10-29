"""Checks the locking behaviors described in Part 5 against PostgreSQL 17 (and SKIP LOCKED on MySQL 8.4)."""
import threading
import time

import psycopg
import pymysql

results = []


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n")


def pg():
    return psycopg.connect("host=localhost port=55432 user=postgres password=test", autocommit=True)


def setup():
    with pg() as c:
        c.execute("DROP TABLE IF EXISTS comments, tickets")
        c.execute("CREATE TABLE tickets (id int PRIMARY KEY, subject text, status text, priority text, version int NOT NULL DEFAULT 1)")
        c.execute("INSERT INTO tickets SELECT g, 'subject ' || g, 'open', 'normal', 1 FROM generate_series(1, 10) g")
        c.execute("CREATE TABLE comments (id serial PRIMARY KEY, ticket_id int REFERENCES tickets(id), body text)")


def wait_blocked(fn):
    out = {}

    def run():
        t0 = time.time()
        try:
            out["result"] = fn()
        except psycopg.Error as e:
            out["error"] = e.sqlstate
        out["dt"] = time.time() - t0
    t = threading.Thread(target=run); t.start(); time.sleep(0.6)
    return t, t.is_alive(), out


def main():
    setup()
    a, b = pg(), pg()

    # Whole-object save overwrites a concurrent field change (the story)
    ra = a.execute("SELECT subject, status, priority FROM tickets WHERE id = 1").fetchone()
    rb = b.execute("SELECT subject, status, priority FROM tickets WHERE id = 1").fetchone()
    b.execute("UPDATE tickets SET subject=%s, status=%s, priority=%s WHERE id=1", (rb[0], "pending", rb[2]))
    a.execute("UPDATE tickets SET subject=%s, status=%s, priority=%s WHERE id=1", ("Refund for order 1182", ra[1], "high"))
    final = a.execute("SELECT subject, status, priority FROM tickets WHERE id = 1").fetchone()
    report("Full-row save from a stale read overwrites a field someone else changed",
           "B set status=pending; A's later save of the whole form writes status back to open",
           f"final={final}", final[1] == "open")

    # Optimistic: version check
    setup()
    va = a.execute("SELECT version FROM tickets WHERE id = 1").fetchone()[0]
    vb = b.execute("SELECT version FROM tickets WHERE id = 1").fetchone()[0]
    nb = b.execute("UPDATE tickets SET status='pending', version=version+1 WHERE id=1 AND version=%s", (vb,)).rowcount
    na = a.execute("UPDATE tickets SET priority='high', version=version+1 WHERE id=1 AND version=%s", (va,)).rowcount
    final = a.execute("SELECT status, priority, version FROM tickets WHERE id = 1").fetchone()
    report("Optimistic: version in the WHERE clause", "the stale save updates 0 rows; B's status survives",
           f"B rows={nb}, A rows={na}, final={final}", nb == 1 and na == 0 and final[0] == "pending")

    # NOWAIT
    setup()
    a.execute("BEGIN"); a.execute("SELECT 1 FROM tickets WHERE id=2 FOR UPDATE")
    b.execute("BEGIN")
    try:
        b.execute("SELECT 1 FROM tickets WHERE id=2 FOR UPDATE NOWAIT"); err = None
    except psycopg.Error as e:
        err = e.sqlstate
    b.execute("ROLLBACK"); a.execute("ROLLBACK")
    report("FOR UPDATE NOWAIT", "fails immediately with 55P03 (lock_not_available) instead of waiting", f"error={err}", err == "55P03")

    # lock_timeout
    a.execute("BEGIN"); a.execute("SELECT 1 FROM tickets WHERE id=2 FOR UPDATE")
    b.execute("BEGIN"); b.execute("SET LOCAL lock_timeout = '200ms'")
    t0 = time.time()
    try:
        b.execute("SELECT 1 FROM tickets WHERE id=2 FOR UPDATE"); err = None
    except psycopg.Error as e:
        err = e.sqlstate
    dt = time.time() - t0
    b.execute("ROLLBACK"); a.execute("ROLLBACK")
    report("lock_timeout", "gives up after ~200 ms with 55P03", f"error={err} after {dt*1000:.0f} ms", err == "55P03" and 0.15 < dt < 0.6)

    # SKIP LOCKED
    a.execute("BEGIN")
    got_a = [r[0] for r in a.execute("SELECT id FROM tickets ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED").fetchall()]
    b.execute("BEGIN")
    got_b = [r[0] for r in b.execute("SELECT id FROM tickets ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED").fetchall()]
    b.execute("ROLLBACK"); a.execute("ROLLBACK")
    report("FOR UPDATE SKIP LOCKED", "a second worker gets the next unlocked rows instead of waiting",
           f"worker A={got_a}, worker B={got_b}", got_a == [1, 2, 3] and got_b == [4, 5, 6])

    # FOR UPDATE blocks inserts of child rows referencing the locked row; FOR NO KEY UPDATE does not
    for mode, expect_block in (("FOR UPDATE", True), ("FOR NO KEY UPDATE", False)):
        setup()
        a.execute("BEGIN"); a.execute(f"SELECT 1 FROM tickets WHERE id=3 {mode}")
        t, blocked, out = wait_blocked(lambda: b.execute("INSERT INTO comments (ticket_id, body) VALUES (3, 'hi')"))
        a.execute("ROLLBACK"); t.join()
        report(f"{mode} vs adding a comment to the same ticket",
               "blocks the foreign-key check of the new comment" if expect_block else "lets the comment insert go through",
               f"insert blocked={blocked}", blocked == expect_block)

    # Deadlock: two transactions lock tickets 4 and 5 in opposite order
    setup()
    a.execute("BEGIN"); b.execute("BEGIN")
    a.execute("SELECT 1 FROM tickets WHERE id=4 FOR UPDATE")
    b.execute("SELECT 1 FROM tickets WHERE id=5 FOR UPDATE")
    t, _, out = wait_blocked(lambda: a.execute("SELECT 1 FROM tickets WHERE id=5 FOR UPDATE"))
    t0 = time.time()
    try:
        b.execute("SELECT 1 FROM tickets WHERE id=4 FOR UPDATE"); errb = None
    except psycopg.Error as e:
        errb = e.sqlstate
    t.join()
    detect = out["dt"]  # how long the first waiter waited before the deadlock was resolved
    erra = out.get("error")
    for c in (a, b):
        try:
            c.execute("ROLLBACK")
        except psycopg.Error:
            pass
    report("Deadlock from opposite lock order",
           "PostgreSQL detects it after deadlock_timeout (1 s by default) and kills one with 40P01",
           f"errors: A={erra} B={errb}; first waiter released after {detect:.2f} s", "40P01" in (erra, errb) and 0.8 < detect < 2.5)

    # Same work, consistent order: no deadlock
    setup()
    a.execute("BEGIN"); b.execute("BEGIN")
    a.execute("SELECT id FROM tickets WHERE id IN (4, 5) ORDER BY id FOR UPDATE")
    t, blocked, out = wait_blocked(lambda: b.execute("SELECT id FROM tickets WHERE id IN (5, 4) ORDER BY id FOR UPDATE").fetchall())
    a.execute("COMMIT"); t.join(); b.execute("COMMIT")
    report("Same two tickets, locked in id order by both", "the second simply waits, then proceeds — no deadlock",
           f"blocked={blocked} error={out.get('error')}", blocked and "error" not in out)

    # Advisory lock for something that isn't a row
    a.execute("BEGIN"); a.execute("SELECT pg_advisory_xact_lock(hashtext('export:workspace:7'))")
    b.execute("BEGIN")
    got = b.execute("SELECT pg_try_advisory_xact_lock(hashtext('export:workspace:7'))").fetchone()[0]
    a.execute("COMMIT")
    got_after = b.execute("SELECT pg_try_advisory_xact_lock(hashtext('export:workspace:7'))").fetchone()[0]
    b.execute("COMMIT")
    report("Advisory lock on a name", "a second session can't take it until the first commits",
           f"while held={got}, after commit={got_after}", got is False and got_after is True)

    # MySQL 8 supports SKIP LOCKED too
    m1 = pymysql.connect(host="127.0.0.1", port=53306, user="root", password="test", database="test", autocommit=True)
    m2 = pymysql.connect(host="127.0.0.1", port=53306, user="root", password="test", database="test", autocommit=True)
    c1, c2 = m1.cursor(), m2.cursor()
    c1.execute("DROP TABLE IF EXISTS jobs"); c1.execute("CREATE TABLE jobs (id int PRIMARY KEY) ENGINE=InnoDB")
    c1.execute("INSERT INTO jobs VALUES (1),(2),(3),(4),(5),(6)")
    c1.execute("START TRANSACTION"); c1.execute("SELECT id FROM jobs ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED")
    ja = [r[0] for r in c1.fetchall()]
    c2.execute("START TRANSACTION"); c2.execute("SELECT id FROM jobs ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED")
    jb = [r[0] for r in c2.fetchall()]
    c1.execute("ROLLBACK"); c2.execute("ROLLBACK")
    report("MySQL FOR UPDATE SKIP LOCKED", "same behavior as PostgreSQL", f"A={ja} B={jb}", ja == [1, 2, 3] and jb == [4, 5, 6])

    print(f"{sum(results)}/{len(results)} passed")


if __name__ == "__main__":
    main()
