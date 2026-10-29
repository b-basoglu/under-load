"""CQRS and materialized projections, measured for Part 32. PostgreSQL 17.

Write model: normalized tickets, users, tenants, tags, comments. Read model: ticket_list_view, one denormalized row per
ticket, maintained by a projector from events written in the same transaction as each command (outbox, Part 29).
"""
import json
import random
import statistics
import threading
import time

import psycopg

PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
results, data = [], {}
HALCYON = 1


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def setup(tickets=200_000, comments=1_000_000):
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS events, ticket_list_view, ticket_list_view_new, tenant_status_counts, comments, ticket_tags, tickets, users, tenants CASCADE")
        c.execute("CREATE TABLE tenants (id int PRIMARY KEY, name text)")
        c.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")
        c.execute("""CREATE TABLE tickets (id bigint PRIMARY KEY, tenant_id int, requester_id int, assignee_id int, subject text,
                     status text, version int DEFAULT 1)""")
        c.execute("CREATE TABLE ticket_tags (ticket_id bigint, tag text)")
        c.execute("CREATE TABLE comments (id bigserial PRIMARY KEY, ticket_id bigint, author_id int, body text, created_at timestamptz)")
        c.execute("INSERT INTO tenants SELECT g, 'tenant ' || g FROM generate_series(1, 50) g")
        c.execute("UPDATE tenants SET name = 'Halcyon' WHERE id = 1")
        c.execute("INSERT INTO users SELECT g, 'user ' || g FROM generate_series(1, 5000) g")
        c.execute(f"""INSERT INTO tickets SELECT g, CASE WHEN g % 5 = 0 THEN 1 ELSE 2 + g % 49 END, 1 + g % 5000, 1 + (g * 7) % 300,
                      'ticket ' || g, (ARRAY['open','open','pending','solved','closed'])[1 + (g / 5) % 5], 1 FROM generate_series(1, {tickets}) g""")
        # status uses g / 5 so it is independent of the tenant (g % 5): every tenant gets the same 60% open/pending mix
        c.execute(f"INSERT INTO ticket_tags SELECT 1 + (g % {tickets}), (ARRAY['billing','login','bug','vip','api','sso'])[1 + g % 6] FROM generate_series(1, {tickets * 3}) g")
        c.execute(f"""INSERT INTO comments (ticket_id, author_id, body, created_at) SELECT 1 + (g::bigint * 7919) % {tickets}, 1 + g % 5000, 'comment',
                      now() - (g % 1000000) * interval '1 second' FROM generate_series(1, {comments}) g""")
        c.execute("CREATE INDEX ON tickets (tenant_id, status)")
        c.execute("CREATE INDEX ON ticket_tags (ticket_id)")
        c.execute("CREATE INDEX ON comments (ticket_id, created_at)")
        c.execute("""CREATE TABLE events (seq bigserial PRIMARY KEY, ticket_id bigint, type text, payload jsonb, version int,
                     created_at timestamptz DEFAULT clock_timestamp(), projected boolean DEFAULT false)""")
        c.execute("CREATE INDEX ON events (seq) WHERE NOT projected")
        c.execute("ANALYZE")


JOIN_QUERY = """
SELECT t.id, t.subject, t.status, r.name, a.name, tn.name,
       (SELECT array_agg(tag) FROM ticket_tags tt WHERE tt.ticket_id = t.id),
       (SELECT count(*) FROM comments c WHERE c.ticket_id = t.id),
       (SELECT max(created_at) FROM comments c WHERE c.ticket_id = t.id) AS last_activity
  FROM tickets t JOIN users r ON r.id = t.requester_id LEFT JOIN users a ON a.id = t.assignee_id JOIN tenants tn ON tn.id = t.tenant_id
 WHERE t.tenant_id = %s AND t.status IN ('open', 'pending')
 ORDER BY last_activity DESC NULLS LAST LIMIT 50"""

VIEW_QUERY = """SELECT ticket_id, subject, status, requester, assignee, tenant_name, tags, comment_count, last_activity
  FROM ticket_list_view WHERE tenant_id = %s AND status IN ('open', 'pending') ORDER BY last_activity DESC NULLS LAST LIMIT 50"""

BACKFILL = """
INSERT INTO {t} SELECT t.id, t.tenant_id, t.subject, t.status, r.name, a.name, tn.name,
       (SELECT array_agg(tag) FROM ticket_tags tt WHERE tt.ticket_id = t.id),
       (SELECT count(*) FROM comments c WHERE c.ticket_id = t.id),
       (SELECT max(created_at) FROM comments c WHERE c.ticket_id = t.id), t.version
  FROM tickets t JOIN users r ON r.id = t.requester_id LEFT JOIN users a ON a.id = t.assignee_id JOIN tenants tn ON tn.id = t.tenant_id"""


def create_view(c, name="ticket_list_view"):
    c.execute(f"""CREATE TABLE {name} (ticket_id bigint PRIMARY KEY, tenant_id int, subject text, status text, requester text, assignee text,
                  tenant_name text, tags text[], comment_count int, last_activity timestamptz, version int)""")


def index_view(c, name="ticket_list_view"):
    c.execute(f"CREATE INDEX {name}_q ON {name} (tenant_id, last_activity DESC NULLS LAST) WHERE status IN ('open', 'pending')")


def timed(c, q, args, n):
    lat = []
    for _ in range(n):
        t0 = time.perf_counter()
        c.execute(q, args).fetchall()
        lat.append((time.perf_counter() - t0) * 1000)
    return {"p50_ms": round(pct(lat, .5), 2), "p99_ms": round(pct(lat, .99), 2)}


# ---- commands (write side) --------------------------------------------------------------------------
def add_comment(c, tid, author):
    v = c.execute("UPDATE tickets SET version = version + 1 WHERE id = %s RETURNING version", (tid,)).fetchone()[0]
    at = c.execute("INSERT INTO comments (ticket_id, author_id, body, created_at) VALUES (%s, %s, 'reply', clock_timestamp()) RETURNING created_at", (tid, author)).fetchone()[0]
    c.execute("INSERT INTO events (ticket_id, type, payload, version) VALUES (%s, 'comment.added', %s, %s)", (tid, json.dumps({"at": at.isoformat()}), v))
    return v


def change_status(c, tid, status, sync_counts=False, ordered=False):
    old = c.execute("SELECT status, tenant_id FROM tickets WHERE id = %s", (tid,)).fetchone()
    v = c.execute("UPDATE tickets SET status = %s, version = version + 1 WHERE id = %s RETURNING version", (status, tid)).fetchone()[0]
    c.execute("INSERT INTO events (ticket_id, type, payload, version) VALUES (%s, 'status.changed', %s, %s)",
              (tid, json.dumps({"status": status, "old": old[0], "tenant": old[1]}), v))
    if sync_counts:                                          # update the dashboard read model inside the command's transaction
        steps = [(old[0], -1), (status, +1)]
        if ordered:                                          # always lock the two counter rows in the same order: no deadlock cycle
            steps.sort()
        for st, d in steps:
            c.execute("UPDATE tenant_status_counts SET n = n + %s WHERE tenant_id = %s AND status = %s", (d, old[1], st))
    return v


# ---- projector (read side) -----------------------------------------------------------------------------
def apply(c, ev, idempotent=True, counts=True):
    seq, tid, typ, payload, version = ev
    guard = " AND version < %s" if idempotent else ""
    if typ == "comment.added":
        c.execute(f"UPDATE ticket_list_view SET comment_count = comment_count + 1, last_activity = %s, version = %s WHERE ticket_id = %s{guard}",
                  (payload["at"], version, tid) + ((version,) if idempotent else ()))
    elif typ == "status.changed":
        r = c.execute(f"UPDATE ticket_list_view SET status = %s, version = %s WHERE ticket_id = %s{guard} RETURNING 1",
                      (payload["status"], version, tid) + ((version,) if idempotent else ())).fetchone()
        if counts and r:
            c.execute("UPDATE tenant_status_counts SET n = n - 1 WHERE tenant_id = %s AND status = %s", (payload["tenant"], payload["old"]))
            c.execute("UPDATE tenant_status_counts SET n = n + 1 WHERE tenant_id = %s AND status = %s", (payload["tenant"], payload["status"]))


def projector(stop, applied_at, interval=0.05, idempotent=True, dup_p=0.0, seed=3):
    rnd = random.Random(seed)
    with psycopg.connect(PG) as c:
        while not stop.is_set():
            rows = c.execute("SELECT seq, ticket_id, type, payload, version FROM events WHERE NOT projected ORDER BY seq LIMIT 500 FOR UPDATE SKIP LOCKED").fetchall()
            for ev in rows:
                apply(c, ev, idempotent)
                if rnd.random() < dup_p:                 # at-least-once: the same event delivered again
                    apply(c, ev, idempotent)
            if rows:
                c.execute("UPDATE events SET projected = true WHERE seq = ANY(%s)", ([r[0] for r in rows],))
            c.commit()
            now = time.time()
            for r in rows:
                applied_at[r[0]] = now
            if len(rows) < 500:
                time.sleep(interval)


if __name__ == "__main__":
    t0 = time.time()
    setup()
    print(f"setup {time.time() - t0:.0f} s", flush=True)
    with psycopg.connect(PG, autocommit=True) as c:
        t1 = time.time()
        create_view(c)
        c.execute(BACKFILL.format(t="ticket_list_view"))
        index_view(c)
        c.execute("CREATE TABLE tenant_status_counts AS SELECT tenant_id, status, count(*)::int AS n FROM tickets GROUP BY 1, 2")
        c.execute("ALTER TABLE tenant_status_counts ADD PRIMARY KEY (tenant_id, status)")
        c.execute("ANALYZE")
        backfill_s = round(time.time() - t1, 1)
        rows = c.execute("SELECT count(*) FROM ticket_list_view").fetchone()[0]
        J = timed(c, JOIN_QUERY, (HALCYON,), 20)
        V = timed(c, VIEW_QUERY, (HALCYON,), 200)
        dash_join = timed(c, "SELECT status, count(*) FROM tickets WHERE tenant_id = %s GROUP BY 1", (HALCYON,), 50)
        dash_view = timed(c, "SELECT status, n FROM tenant_status_counts WHERE tenant_id = %s", (HALCYON,), 200)
        same = c.execute(JOIN_QUERY, (HALCYON,)).fetchall()
        same2 = c.execute(VIEW_QUERY, (HALCYON,)).fetchall()
        identical = [r[0] for r in same] == [r[0] for r in same2]
    data["read"] = {"join": J, "view": V, "dash_join": dash_join, "dash_view": dash_view, "backfill_s": backfill_s, "rows": rows, "identical_results": identical}
    print(data["read"], flush=True)

    # ---- lag and read-your-writes ----
    stop, applied = threading.Event(), {}
    pt = threading.Thread(target=projector, args=(stop, applied))
    pt.start()
    rnd = random.Random(7)
    halcyon_ids = list(range(5, 200_001, 5))
    stale, checks, created = 0, 0, {}
    with psycopg.connect(PG) as c:
        t2 = time.time()
        for i in range(3000):
            tid = rnd.choice(halcyon_ids)
            v = add_comment(c, tid, rnd.randint(1, 5000))
            c.commit()
            seq = c.execute("SELECT max(seq) FROM events WHERE ticket_id = %s", (tid,)).fetchone()[0]
            created[seq] = time.time()
            got = c.execute("SELECT version FROM ticket_list_view WHERE ticket_id = %s", (tid,)).fetchone()[0]
            c.commit()
            checks += 1
            stale += got < v                          # the agent's own reply isn't on the list yet
            d = t2 + (i + 1) / 300 - time.time()
            if d > 0:
                time.sleep(d)
    time.sleep(1)
    stop.set()
    pt.join()
    lag = [(applied[s] - t) * 1000 for s, t in created.items() if s in applied]
    data["lag"] = {"commands": checks, "stale_reads_pct": round(100 * stale / checks, 1), "lag_p50_ms": round(pct(lag, .5)), "lag_p99_ms": round(pct(lag, .99))}
    print(data["lag"], flush=True)

    # ---- sync vs async dashboard counters under concurrency ----
    def writers(sync, n_threads=16, per=150, ordered=False):
        lat, lock = [], threading.Lock()
        dead = [0]

        def w(k):
            r = random.Random(k)
            with psycopg.connect(PG) as c:
                c.execute("SET deadlock_timeout = '100ms'")   # detect cycles quickly (default 1 s) so the run stays short
                c.commit()
                for _ in range(per):
                    t = time.perf_counter()
                    tid, st = r.choice(halcyon_ids), r.choice(["open", "pending", "solved"])
                    while True:
                        try:
                            change_status(c, tid, st, sync_counts=sync, ordered=ordered)
                            c.commit()
                            break
                        except psycopg.errors.DeadlockDetected:
                            c.rollback()                     # PostgreSQL killed one side of the cycle; retry the command
                            with lock:
                                dead[0] += 1
                    with lock:
                        lat.append((time.perf_counter() - t) * 1000)
        ts = [threading.Thread(target=w, args=(k,)) for k in range(n_threads)]
        t0 = time.time()
        for t in ts: t.start()
        for t in ts: t.join()
        return {"sync": sync, "ordered": ordered, "p50_ms": round(pct(lat, .5), 1), "p99_ms": round(pct(lat, .99), 1),
                "per_s": round(len(lat) / (time.time() - t0)), "deadlocks": dead[0]}
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("UPDATE events SET projected = true")
    S = writers(True)
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("UPDATE events SET projected = true")
    SO = writers(True, ordered=True)
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("UPDATE events SET projected = true")
    A = writers(False)
    data["counters"] = {"sync": S, "sync_ordered": SO, "async": A}
    print(data["counters"], flush=True)

    # ---- duplicates: non-idempotent vs version-aware projector; divergence check ----
    def dup_run(idempotent):
        with psycopg.connect(PG, autocommit=True) as c:
            c.execute("UPDATE events SET projected = true")
            c.execute("TRUNCATE ticket_list_view")
            c.execute(BACKFILL.format(t="ticket_list_view"))
        stop, applied = threading.Event(), {}
        pt = threading.Thread(target=projector, args=(stop, applied), kwargs={"idempotent": idempotent, "dup_p": 0.15})
        pt.start()
        r = random.Random(11)
        touched = set()
        with psycopg.connect(PG) as c:
            for _ in range(2000):
                tid = r.choice(halcyon_ids)
                add_comment(c, tid, 1)
                c.commit()
                touched.add(tid)
        time.sleep(2)
        stop.set()
        pt.join()
        with psycopg.connect(PG) as c:                 # divergence check: recompute from the write model and compare
            diverged = c.execute("""SELECT count(*) FROM ticket_list_view v
                                     WHERE v.ticket_id = ANY(%s) AND v.comment_count <> (SELECT count(*) FROM comments c WHERE c.ticket_id = v.ticket_id)""",
                                 (list(touched),)).fetchone()[0]
        return {"idempotent": idempotent, "tickets_touched": len(touched), "diverged": diverged}
    D = {"plain": dup_run(False), "versioned": dup_run(True)}
    data["duplicates"] = D
    print(D, flush=True)

    # ---- rebuild a projection without downtime: build a new table, swap ----
    with psycopg.connect(PG, autocommit=True) as c:
        t3 = time.time()
        create_view(c, "ticket_list_view_new")
        c.execute(BACKFILL.format(t="ticket_list_view_new"))
        index_view(c, "ticket_list_view_new")
        build_s = round(time.time() - t3, 1)
        t4 = time.perf_counter()
        with c.transaction():
            c.execute("ALTER TABLE ticket_list_view RENAME TO ticket_list_view_old")
            c.execute("ALTER TABLE ticket_list_view_new RENAME TO ticket_list_view")
        swap_ms = round((time.perf_counter() - t4) * 1000, 1)
        c.execute("DROP TABLE ticket_list_view_old")
    data["rebuild"] = {"rows": rows, "build_s": build_s, "swap_ms": swap_ms}
    print(data["rebuild"], flush=True)

    r, l = data["read"], data["lag"]
    report("Ticket list from the normalized write model", "joins and per-ticket aggregates on every page load",
           f"Halcyon open+pending queue: p50 {r['join']['p50_ms']} ms, p99 {r['join']['p99_ms']} ms",
           r["join"]["p50_ms"] > 10 * r["view"]["p50_ms"])
    report("Same list from a projection", "one indexed read of a precomputed row per ticket, same result",
           f"p50 {r['view']['p50_ms']} ms, p99 {r['view']['p99_ms']} ms; identical result: {r['identical_results']}",
           r["view"]["p50_ms"] * 20 < r["join"]["p50_ms"] and r["identical_results"])
    report("A second projection for a different query (dashboard counts)", "each query gets a model shaped for it",
           f"GROUP BY on tickets p50 {r['dash_join']['p50_ms']} ms vs counts table {r['dash_view']['p50_ms']} ms",
           r["dash_view"]["p50_ms"] < r["dash_join"]["p50_ms"] and r["dash_view"]["p50_ms"] < 2)
    report("Projection is eventually consistent", "an agent reading right after writing may not see their change",
           f"lag p50 {l['lag_p50_ms']} ms, p99 {l['lag_p99_ms']} ms; {l['stale_reads_pct']}% of immediate reads were stale",
           l["stale_reads_pct"] > 50 and l["lag_p50_ms"] > 5)
    s, so, a = data["counters"]["sync"], data["counters"]["sync_ordered"], data["counters"]["async"]
    report("Updating a hot read model inside the command", "no lag, but every writer queues on the same counter rows — and can deadlock",
           f"command p99: synchronous {s['p99_ms']} ms ({s['deadlocks']} deadlocks retried), counter rows locked in a fixed order {so['p99_ms']} ms "
           f"({so['deadlocks']} deadlocks), asynchronous {a['p99_ms']} ms (16 writers, one tenant)",
           s["p99_ms"] > a["p99_ms"] * 1.5 and so["deadlocks"] == 0 and s["deadlocks"] > 0 and a["deadlocks"] == 0)
    report("Duplicate events (15%) with a counting handler", "counts drift; a divergence check catches it",
           f"diverged tickets: plain {D['plain']['diverged']} of {D['plain']['tickets_touched']}, version-aware {D['versioned']['diverged']}",
           D["plain"]["diverged"] > 50 and D["versioned"]["diverged"] == 0)
    rb = data["rebuild"]
    report("Rebuild into a new table, then swap", "readers never see a half-built projection",
           f"{rb['rows']:,} rows built in {rb['build_s']} s; swap took {rb['swap_ms']} ms",
           rb["swap_ms"] < 100 and rb["rows"] == 200_000)
    with open(__file__.replace("cqrs.py", "cqrs_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
