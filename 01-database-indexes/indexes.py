"""Database indexes, measured for Part 1. PostgreSQL 17 (container ul-pg, port 55432), database `ul_indexes`.

Data: 5,000,000 tickets. 1,000 ordinary workspaces with 2,000 tickets each (30% not closed, 10 agents each), and one big
customer, Halcyon (workspace 7), whose import added 3,000,000 tickets (99% closed, 200 agents). 2,000,000 comments.
Every query timing is the median of 5 warm runs of EXPLAIN (ANALYZE, BUFFERS); buffers are pages touched
(hit = found in PostgreSQL's memory, read = fetched from the OS).

Run:  PYTHONPATH=<dir with psycopg> python3 indexes.py      (about 10 minutes)
"""
import json
import statistics
import subprocess
import threading
import time
from pathlib import Path

import psycopg

DSN = "host=localhost port=55432 user=postgres password=test"
HERE = Path(__file__).resolve().parent
results, data, plans = [], {}, []
HALCYON, AGENT, SMALL_WS, SMALL_AGENT = 7, 100005, 1003, 10035


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


with psycopg.connect(DSN, autocommit=True) as c:
    c.execute("DROP DATABASE IF EXISTS ul_indexes")
    c.execute("CREATE DATABASE ul_indexes")
db = psycopg.connect(DSN + " dbname=ul_indexes", autocommit=True, cursor_factory=psycopg.ClientCursor)
q = db.execute


def nodes(p, out):
    out.append((p["Node Type"], p.get("Index Name")))
    for ch in p.get("Plans", []):
        nodes(ch, out)
    return out


def explain(sql, label=None, runs=5, settings=()):
    for s in settings:
        q(s)
    ms = []
    for _ in range(runs):
        j = q("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql).fetchone()[0][0]
        ms.append(j["Execution Time"])
    root = j["Plan"]
    if label:
        txt = "\n".join(r[0] for r in q("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) " + sql).fetchall())
        plans.append(f"-- {label}\n{sql}\n{txt}\n")
    for s in settings:
        q("RESET " + s.split()[1])
    ns = nodes(root, [])

    def removed(p):
        return p.get("Rows Removed by Filter", 0) + sum(removed(ch) for ch in p.get("Plans", []))

    def heap(p):
        return p.get("Heap Fetches", 0) + sum(heap(ch) for ch in p.get("Plans", []))

    return {"ms": round(statistics.median(ms), 3), "heap_fetches": heap(root), "hit": root.get("Shared Hit Blocks", 0), "read": root.get("Shared Read Blocks", 0),
            "pages": root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0), "nodes": [n for n, _ in ns],
            "indexes": sorted({i for _, i in ns if i}), "rows": root["Actual Rows"], "est_rows": root["Plan Rows"], "removed": removed(root)}


def size(rel):
    return q("SELECT pg_relation_size(%s)", (rel,)).fetchone()[0]


def mb(b):
    return round(b / 2**20, 1)


MYQ = ("SELECT id, subject, priority, updated_at FROM tickets WHERE workspace_id = {ws} AND assignee_id = {a} "
       "AND state <> 'closed' ORDER BY updated_at DESC LIMIT 50")


def myq(ws=HALCYON, a=AGENT):
    return MYQ.format(ws=ws, a=a)


def pgbench(script, clients=32, secs=15, mode="simple"):
    subprocess.run(["docker", "exec", "-i", "ul-pg", "sh", "-c", "cat > /tmp/myq.sql"], input=script.encode(), check=True)
    out = subprocess.run(["docker", "exec", "ul-pg", "pgbench", "-U", "postgres", "-n", "-M", mode, "-c", str(clients), "-j", "8",
                          "-T", str(secs), "-f", "/tmp/myq.sql", "ul_indexes"], capture_output=True, text=True).stdout
    tps = float(next(l for l in out.splitlines() if l.startswith("tps")).split("=")[1].split()[0])
    lat = float(next(l for l in out.splitlines() if l.startswith("latency average")).split("=")[1].split()[0])
    return {"tps": round(tps), "latency_ms": round(lat, 2)}


if __name__ == "__main__":
    t0 = time.time()
    # ------------------------------------------------------------------ data
    q("""CREATE TABLE tickets (
           id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, workspace_id int NOT NULL, assignee_id int, requester_id int NOT NULL,
           state text NOT NULL, priority smallint NOT NULL, subject text NOT NULL, external_ref text NOT NULL, metadata jsonb NOT NULL,
           created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)""")
    q("SELECT setseed(0.42)")
    subjects = "ARRAY['Login loops after SSO change','Invoice is missing the VAT number','Webhook deliveries failing','Cannot attach a PDF'," \
               "'Agent seat not showing','Export to CSV times out','Two-factor code rejected','Email replies not threaded'," \
               "'Billing address update','Slow search results','API returns 429','Macro not applied']"
    common = f"""requester_id, state, priority, subject, 'EXT-' || g, jsonb_build_object('channel', ch, 'locale', 'en'), ts, ts + random() * interval '20 days'
        FROM (SELECT g, 1 + floor(random() * 1000000)::int AS requester_id,
                     CASE WHEN random() < {{open}} THEN (CASE WHEN random() < 0.7 THEN 'open' ELSE 'pending' END) ELSE 'closed' END AS state,
                     1 + floor(random() * 4)::smallint AS priority,
                     CASE WHEN random() < 0.001 THEN 'Chargeback dispute on order ' || g ELSE ({subjects})[1 + floor(random() * 12)::int] || ' #' || g END AS subject,
                     CASE WHEN random() < 0.01 THEN 'api' ELSE (ARRAY['email','chat','web'])[1 + floor(random() * 3)::int] END AS ch,
                     timestamptz '2024-10-01' + random() * interval '{{span}} days' AS ts
              FROM generate_series({{lo}}, {{hi}}) g) s"""
    q(f"""INSERT INTO tickets (workspace_id, assignee_id, requester_id, state, priority, subject, external_ref, metadata, created_at, updated_at)
          SELECT 1000 + g % 1000, CASE WHEN random() < 0.05 THEN NULL ELSE (1000 + g % 1000) * 10 + floor(random() * 10)::int END,
          {common.format(open=0.30, span=730, lo=1, hi=2000000)}""")
    q(f"""INSERT INTO tickets (workspace_id, assignee_id, requester_id, state, priority, subject, external_ref, metadata, created_at, updated_at)
          SELECT {HALCYON}, CASE WHEN random() < 0.05 THEN NULL ELSE 100000 + floor(random() * 200)::int END,
          {common.format(open=0.01, span=700, lo=2000001, hi=5000000)}""")
    q("CREATE TABLE comments (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ticket_id bigint NOT NULL REFERENCES tickets(id), body text NOT NULL, created_at timestamptz NOT NULL)")
    q("INSERT INTO comments (ticket_id, body, created_at) SELECT 1 + floor(random() * 4000000)::bigint, 'Thanks, looking into it now. ' || g, "
      "timestamptz '2024-10-01' + random() * interval '730 days' FROM generate_series(1, 2000000) g")
    q("VACUUM ANALYZE tickets")
    q("VACUUM ANALYZE comments")
    counts = q(f"SELECT count(*) FILTER (WHERE workspace_id = {HALCYON}), count(*) FILTER (WHERE workspace_id = {HALCYON} AND state <> 'closed'), "
               f"count(*) FILTER (WHERE state <> 'closed'), count(*) FILTER (WHERE workspace_id = {HALCYON} AND assignee_id = {AGENT}), "
               f"count(*) FILTER (WHERE workspace_id = {HALCYON} AND assignee_id = {AGENT} AND state <> 'closed'), "
               f"count(*) FILTER (WHERE workspace_id = {SMALL_WS}), count(*) FILTER (WHERE assignee_id = {SMALL_AGENT} AND state <> 'closed') FROM tickets").fetchone()
    data["data"] = {"tickets": 5000000, "halcyon": counts[0], "halcyon_open": counts[1], "open_all": counts[2], "agent_tickets": counts[3],
                    "agent_open": counts[4], "small_ws": counts[5], "small_agent_open": counts[6], "table_mb": mb(size("tickets")),
                    "table_pages": size("tickets") // 8192, "load_s": round(time.time() - t0)}
    print("data", data["data"], flush=True)

    # ------------------------------------------------------------------ 1. B-tree
    q("CREATE EXTENSION pageinspect")
    meta = q("SELECT level FROM bt_metap('tickets_pkey')").fetchone()[0]
    leaf = q("SELECT live_items FROM bt_page_stats('tickets_pkey', 100)").fetchone()[0]
    root_items = q("SELECT live_items FROM bt_page_stats('tickets_pkey', (SELECT root FROM bt_metap('tickets_pkey')))").fetchone()[0]
    look = explain("SELECT * FROM tickets WHERE id = 3481213", "lookup one ticket by primary key")
    scan = explain("SELECT * FROM tickets WHERE external_ref = 'EXT-3481213'", "same ticket, no index on external_ref")
    data["btree"] = {"levels_below_root": meta, "levels_total": meta + 1, "entries_per_leaf": leaf, "root_entries": root_items, "lookup": look, "no_index": scan,
                     "pkey_mb": mb(size("tickets_pkey"))}
    report("A B-tree finds one row in a handful of page reads", "5M rows fit in 3–4 levels; a lookup reads one page per level plus the row",
           f"{meta + 1} levels, {leaf} entries per leaf page; lookup {look['pages']} pages in {look['ms']} ms vs full scan {scan['pages']} pages in {scan['ms']} ms",
           meta + 1 <= 4 and look["pages"] <= 5 and scan["pages"] > 1000 * look["pages"])

    # ------------------------------------------------------------------ 2. selectivity: random vs sequential
    q("CREATE INDEX tickets_requester_idx ON tickets (requester_id)")
    q("ANALYZE tickets")
    sweep = {}
    for pct in (0.01, 0.1, 1, 5, 20):
        cut = int(1 + 1000000 * pct / 100)
        sql = f"SELECT sum(priority) FROM tickets WHERE requester_id < {cut}"
        idx = explain(sql, runs=3, settings=("SET enable_seqscan = off", "SET enable_bitmapscan = off"))
        seq = explain(sql, runs=3, settings=("SET enable_indexscan = off", "SET enable_bitmapscan = off"))
        bmp = explain(sql, runs=3, settings=("SET enable_seqscan = off", "SET enable_indexscan = off"))
        chosen = explain(sql, runs=3)
        sweep[str(pct)] = {"index_ms": idx["ms"], "index_pages": idx["pages"], "bitmap_ms": bmp["ms"], "seq_ms": seq["ms"], "seq_pages": seq["pages"],
                           "planner": chosen["nodes"], "planner_ms": chosen["ms"]}
        print("selectivity", pct, sweep[str(pct)], flush=True)
    closed = explain("SELECT count(*) FROM tickets WHERE state = 'closed'", "poorly selective filter")
    data["selectivity"] = {"sweep": sweep, "closed_plan": closed["nodes"]}
    report("Selectivity decides whether an index helps", "an index wins for a few rows and loses to a sequential scan when a filter matches many",
           " · ".join(f"{k}%: index {v['index_ms']:.0f} ms vs seq {v['seq_ms']:.0f} ms" for k, v in sweep.items()),
           sweep["0.01"]["index_ms"] < sweep["0.01"]["seq_ms"] and sweep["20"]["index_ms"] > sweep["20"]["seq_ms"])

    # ------------------------------------------------------------------ 3. three single-column indexes
    q("CREATE INDEX tickets_workspace_idx ON tickets (workspace_id)")
    q("CREATE INDEX tickets_assignee_idx ON tickets (assignee_id)")
    q("CREATE INDEX tickets_state_idx ON tickets (state)")
    q("ANALYZE tickets")
    before_h = explain(myq(), "My Queue, Halcyon agent, three single-column indexes")
    before_s = explain(myq(SMALL_WS, SMALL_AGENT), "My Queue, ordinary agent, three single-column indexes")
    ws_only = explain(f"SELECT count(*) FROM tickets WHERE workspace_id = {HALCYON} AND priority = 1", "Halcyon workspace filter alone")
    data["before"] = {"halcyon": before_h, "small": before_s, "workspace_only": ws_only["nodes"]}
    report("Three single-column indexes don't serve My Queue", "the database reads, filters and sorts far more rows than the 50 it returns",
           f"Halcyon agent: {before_h['ms']} ms, {before_h['pages']} pages, {before_h['removed']} rows removed by filter, plan {before_h['nodes']} · "
           f"ordinary agent: {before_s['ms']} ms, {before_s['pages']} pages",
           before_h["pages"] > 20 * before_s["pages"] and "Sort" in before_h["nodes"])
    hal = f"\\set a random(100000, 100199)\n{MYQ.format(ws=HALCYON, a=':a')};\n"
    data["bench_before"] = {"halcyon": pgbench(hal), "halcyon_prepared": pgbench(hal, mode="prepared"),
                            "small": pgbench(f"\\set w random(1000, 1999)\n\\set k random(0, 9)\n{MYQ.format(ws=':w', a=':w * 10 + :k')};\n")}
    print("bench before", data["bench_before"], flush=True)

    # ------------------------------------------------------------------ 4. estimates and tenant skew
    est = explain(f"SELECT id FROM tickets WHERE workspace_id = {HALCYON} AND state <> 'closed'", runs=1)
    est_agent = explain(f"SELECT id FROM tickets WHERE workspace_id = {HALCYON} AND assignee_id = {AGENT} AND state <> 'closed'", runs=1)
    q("CREATE STATISTICS tickets_ws_state (mcv) ON workspace_id, state FROM tickets")
    q("ANALYZE tickets")
    est2 = explain(f"SELECT id FROM tickets WHERE workspace_id = {HALCYON} AND state <> 'closed'", runs=1)
    est3 = explain(f"SELECT id FROM tickets WHERE workspace_id = {HALCYON} AND state IN ('open', 'pending')", runs=1)
    q("DROP STATISTICS tickets_ws_state")
    q("ANALYZE tickets")
    data["estimates"] = {"halcyon_open": {"est": est["est_rows"], "actual": est["rows"]}, "agent_open": {"est": est_agent["est_rows"], "actual": est_agent["rows"]},
                         "with_stats_neq": {"est": est2["est_rows"], "actual": est2["rows"]}, "with_stats_in": {"est": est3["est_rows"], "actual": est3["rows"]}}
    report("Per-column statistics misjudge the big tenant", "the planner multiplies selectivities as if columns were unrelated; CREATE STATISTICS studies them together",
           f"Halcyon open tickets: estimated {est['est_rows']:,}, actual {est['rows']:,} · with CREATE STATISTICS: <> estimated {est2['est_rows']:,}, "
           f"IN ('open','pending') estimated {est3['est_rows']:,}", est["est_rows"] > 2 * est["rows"])
    q(f"PREPARE gq(int, int) AS {MYQ.format(ws='$1', a='$2')}")
    gen = explain("EXECUTE gq(7, 100005)", "My Queue as a generic prepared plan", settings=("SET plan_cache_mode = force_generic_plan",))
    cus = explain("EXECUTE gq(7, 100005)", settings=("SET plan_cache_mode = force_custom_plan",))
    q("DEALLOCATE gq")
    data["generic_old"] = {"generic": gen, "custom": cus}
    print("generic vs custom (old indexes)", gen["ms"], gen["nodes"], cus["ms"], cus["nodes"], flush=True)

    # ------------------------------------------------------------------ 5. the right index
    t = time.time()
    q("CREATE INDEX CONCURRENTLY tickets_my_queue_idx ON tickets (workspace_id, assignee_id, updated_at DESC) WHERE state <> 'closed'")
    build_s = round(time.time() - t, 1)
    q("ANALYZE tickets")
    after_h = explain(myq(), "My Queue, Halcyon agent, composite partial index")
    after_s = explain(myq(SMALL_WS, SMALL_AGENT), "My Queue, ordinary agent, composite partial index")
    q("CREATE INDEX tickets_full_composite_idx ON tickets (workspace_id, assignee_id, updated_at DESC)")
    sizes = {k: mb(size(k)) for k in ("tickets_my_queue_idx", "tickets_full_composite_idx", "tickets_workspace_idx", "tickets_assignee_idx", "tickets_state_idx", "tickets_pkey")}
    q("DROP INDEX tickets_full_composite_idx")
    data["after"] = {"halcyon": after_h, "small": after_s, "build_concurrently_s": build_s, "sizes_mb": sizes, "table_mb": mb(size("tickets"))}
    report("A composite partial index serves the page directly", "seek to (workspace, agent), read 50 entries in order, stop — the same for every tenant",
           f"Halcyon agent: {before_h['ms']} ms → {after_h['ms']} ms, {before_h['pages']} → {after_h['pages']} pages, plan {after_h['nodes']} · "
           f"ordinary agent {after_s['ms']} ms, {after_s['pages']} pages",
           "Sort" not in after_h["nodes"] and after_h["pages"] < 100 and after_h["ms"] < before_h["ms"] / 10)
    report("Partial index is a fraction of the full one", "leaving closed tickets out keeps the index small",
           f"partial {sizes['tickets_my_queue_idx']} MB vs full composite {sizes['tickets_full_composite_idx']} MB (table {data['after']['table_mb']} MB)",
           sizes["tickets_my_queue_idx"] < sizes["tickets_full_composite_idx"] / 3)
    data["bench_after"] = {"halcyon": pgbench(hal), "halcyon_prepared": pgbench(hal, mode="prepared")}
    print("bench after", data["bench_after"], flush=True)

    # column order: only one candidate index at a time
    q("DROP INDEX tickets_workspace_idx, tickets_assignee_idx, tickets_state_idx")
    lead = f"SELECT id, subject, priority, updated_at FROM tickets WHERE workspace_id = {HALCYON} AND state <> 'closed' ORDER BY updated_at DESC LIMIT 50"
    order = {"workspace, assignee, updated_at": {"myq": after_h, "lead": explain(lead, "team lead view with (workspace, assignee, updated_at)")}}
    q("DROP INDEX tickets_my_queue_idx")
    for cols, name in (("updated_at DESC, workspace_id, assignee_id", "updated_at, workspace, assignee"), ("assignee_id, workspace_id, updated_at DESC", "assignee, workspace, updated_at")):
        q(f"CREATE INDEX cand_idx ON tickets ({cols}) WHERE state <> 'closed'")
        q("ANALYZE tickets")
        order[name] = {"myq": explain(myq(), runs=3), "lead": explain(lead, runs=3)}
        q("DROP INDEX cand_idx")
    q("CREATE INDEX tickets_my_queue_idx ON tickets (workspace_id, assignee_id, updated_at DESC) WHERE state <> 'closed'")
    data["column_order"] = {k: {"myq_ms": v["myq"]["ms"], "myq_pages": v["myq"]["pages"], "myq_plan": v["myq"]["nodes"], "lead_ms": v["lead"]["ms"],
                                "lead_pages": v["lead"]["pages"], "lead_plan": v["lead"]["nodes"]} for k, v in order.items()}
    print("column order", json.dumps(data["column_order"]), flush=True)
    co = data["column_order"]
    report("Column order decides what an index can do", "equality columns first, the sort column last; a different order serves other queries or none",
           " · ".join(f"({k}): My Queue {v['myq_ms']} ms / {v['myq_pages']} pages, lead view {v['lead_ms']} ms" for k, v in co.items()),
           co["updated_at, workspace, assignee"]["myq_pages"] > 10 * co["workspace, assignee, updated_at"]["myq_pages"])

    # leftmost prefix
    q("CREATE INDEX tickets_ws_assignee_idx ON tickets (workspace_id, assignee_id)")
    q("ANALYZE tickets")
    skip = explain(f"SELECT count(*) FROM tickets WHERE assignee_id = {AGENT}", "assignee only, index on (workspace_id, assignee_id)")
    both = explain(f"SELECT count(*) FROM tickets WHERE workspace_id = {HALCYON} AND assignee_id = {AGENT}")
    q("DROP INDEX tickets_ws_assignee_idx")
    data["leftmost"] = {"skip_first": skip, "with_first": both}
    report("Leftmost prefix rule", "an index on (workspace_id, assignee_id) can't seek on assignee_id alone",
           f"assignee only: {skip['ms']} ms, {skip['pages']} pages, {skip['nodes']} · with workspace: {both['ms']} ms, {both['pages']} pages",
           skip["pages"] > 10 * both["pages"])

    # partial index and a parameterised state
    q(f"PREPARE pq(int, int, text) AS SELECT id, subject, priority, updated_at FROM tickets WHERE workspace_id = $1 AND assignee_id = $2 "
      "AND state <> $3 ORDER BY updated_at DESC LIMIT 50")
    pgen = explain("EXECUTE pq(7, 100005, 'closed')", "state passed as a parameter, generic plan", settings=("SET plan_cache_mode = force_generic_plan",))
    pcus = explain("EXECUTE pq(7, 100005, 'closed')", settings=("SET plan_cache_mode = force_custom_plan",))
    q("DEALLOCATE pq")
    data["partial_param"] = {"generic": pgen, "custom": pcus}
    report("A partial index needs a matching condition", "with state <> $3 in a generic plan, the planner can't prove the partial index applies",
           f"generic: {pgen['ms']} ms, {pgen['pages']} pages, indexes {pgen['indexes']} · custom (value known): {pcus['ms']} ms, indexes {pcus['indexes']}",
           "tickets_my_queue_idx" not in pgen["indexes"] and "tickets_my_queue_idx" in pcus["indexes"])

    # covering index
    many = f"SELECT subject, priority FROM tickets WHERE workspace_id = {HALCYON} AND state <> 'closed'"
    q("VACUUM tickets")
    nocov_50, nocov_many = explain(myq(), runs=5), explain(many, "lead export without INCLUDE", runs=5)
    q("CREATE INDEX tickets_my_queue_cov ON tickets (workspace_id, assignee_id, updated_at DESC) INCLUDE (subject, priority) WHERE state <> 'closed'")
    q("DROP INDEX tickets_my_queue_idx")
    q("VACUUM ANALYZE tickets")
    cov_50, cov_many = explain(myq(), "My Queue with a covering index"), explain(many, "lead export with INCLUDE")
    data["covering"] = {"no_include": {"myq": nocov_50, "many": nocov_many}, "include": {"myq": cov_50, "many": cov_many}, "cov_mb": mb(size("tickets_my_queue_cov"))}
    q("DROP INDEX tickets_my_queue_cov")
    q("CREATE INDEX tickets_my_queue_idx ON tickets (workspace_id, assignee_id, updated_at DESC) WHERE state <> 'closed'")
    q("ANALYZE tickets")
    report("Covering index: index-only scan", "INCLUDE lets the query skip the table; it matters for queries that read many rows",
           f"50 rows: {nocov_50['ms']} → {cov_50['ms']} ms ({nocov_50['pages']} → {cov_50['pages']} pages) · {nocov_many['rows']:,} rows: "
           f"{nocov_many['ms']} → {cov_many['ms']} ms ({nocov_many['pages']} → {cov_many['pages']} pages, plan {cov_many['nodes']}, heap fetches {cov_many['heap_fetches']}) · index {sizes['tickets_my_queue_idx']} → {data['covering']['cov_mb']} MB",
           "Index Only Scan" in cov_many["nodes"] and cov_many["ms"] < nocov_many["ms"] / 5)

    # ------------------------------------------------------------------ 6. joins
    page = "SELECT c.body, c.created_at FROM tickets t JOIN comments c ON c.ticket_id = t.id WHERE t.id = 3200017 ORDER BY c.created_at"

    def delete_ms():
        with db.transaction(force_rollback=True):
            t = time.perf_counter()
            q("DELETE FROM tickets WHERE id = (SELECT max(id) FROM tickets)")
            return round(1000 * (time.perf_counter() - t), 1)

    nofk = explain(page, "ticket page, no index on comments.ticket_id")
    del_no = statistics.median(delete_ms() for _ in range(3))
    q("CREATE INDEX comments_ticket_idx ON comments (ticket_id, created_at)")
    q("ANALYZE comments")
    fk = explain(page, "ticket page, index on comments (ticket_id, created_at)")
    del_yes = statistics.median(delete_ms() for _ in range(3))
    data["joins"] = {"no_index": nofk, "index": fk, "delete_ms_no_index": del_no, "delete_ms_index": del_yes}
    report("Foreign keys need their own index", "PostgreSQL doesn't index comments.ticket_id; without it every ticket page and every delete scans comments",
           f"ticket page {nofk['ms']} → {fk['ms']} ms ({nofk['pages']} → {fk['pages']} pages) · deleting a ticket {del_no} → {del_yes} ms",
           fk["ms"] < nofk["ms"] / 20 and del_yes < del_no / 5)

    # ------------------------------------------------------------------ 7. queries that can't use an index
    q("CREATE INDEX tickets_created_idx ON tickets (created_at)")
    q("CREATE INDEX tickets_unassigned_idx ON tickets (workspace_id, created_at) WHERE assignee_id IS NULL")
    q("CREATE INDEX tickets_assignee_idx ON tickets (assignee_id)")
    q("CREATE INDEX tickets_external_ref_idx ON tickets (external_ref)")
    q("ANALYZE tickets")
    cases = {}

    def pair(name, bad, good, setup=None, teardown=None):
        b = explain(bad, f"{name}: can't seek")
        if setup:
            for s in setup:
                q(s)
            q("ANALYZE tickets")
        g = explain(good, f"{name}: can seek")
        for s in teardown or ():
            q(s)
        cases[name] = {"bad_ms": b["ms"], "bad_pages": b["pages"], "bad_plan": b["nodes"], "good_ms": g["ms"], "good_pages": g["pages"], "good_plan": g["nodes"],
                       "rows": [b["rows"], g["rows"]]}
        print(name, cases[name], flush=True)

    pair("date_trunc", "SELECT count(*) FROM tickets WHERE date_trunc('day', created_at) = '2025-06-01'",
         "SELECT count(*) FROM tickets WHERE created_at >= '2025-06-01' AND created_at < '2025-06-02'")
    pair("coalesce", f"SELECT id FROM tickets WHERE workspace_id = {SMALL_WS} AND COALESCE(assignee_id, 0) = 0 ORDER BY created_at LIMIT 50",
         f"SELECT id FROM tickets WHERE workspace_id = {SMALL_WS} AND assignee_id IS NULL ORDER BY created_at LIMIT 50")
    pair("type mismatch", "SELECT * FROM tickets WHERE id = 3481213.0", "SELECT * FROM tickets WHERE id = 3481213")
    pair("json", f"SELECT id FROM tickets WHERE workspace_id = {SMALL_WS} AND metadata->>'channel' = 'api'",
         f"SELECT id FROM tickets WHERE workspace_id = {SMALL_WS} AND metadata->>'channel' = 'api'",
         setup=["CREATE INDEX tickets_channel_idx ON tickets (workspace_id, (metadata->>'channel'))"], teardown=["DROP INDEX tickets_channel_idx"])
    q("CREATE EXTENSION pg_trgm")
    t = time.time()
    pair("contains", "SELECT count(*) FROM tickets WHERE subject ILIKE '%chargeback%'", "SELECT count(*) FROM tickets WHERE subject ILIKE '%chargeback%'",
         setup=["CREATE INDEX tickets_subject_trgm ON tickets USING gin (subject gin_trgm_ops)"])
    cases["contains"]["build_s"] = round(time.time() - t, 1)
    cases["contains"]["trgm_mb"] = mb(size("tickets_subject_trgm"))
    q("DROP INDEX tickets_subject_trgm")
    # OR across two columns vs UNION ALL, newest first
    orq = (f"SELECT id, updated_at FROM tickets WHERE assignee_id = {AGENT} OR requester_id = 4242 ORDER BY updated_at DESC LIMIT 50")
    unq = (f"(SELECT id, updated_at FROM tickets WHERE assignee_id = {AGENT} UNION ALL "
           f"SELECT id, updated_at FROM tickets WHERE requester_id = 4242 AND assignee_id IS DISTINCT FROM {AGENT}) ORDER BY updated_at DESC LIMIT 50")
    pair("or_vs_union", orq, unq)
    data["sargable"] = cases
    report("Calculations on a column hide it from the index", "rewrite to compare the stored value, or index the expression",
           " · ".join(f"{k}: {v['bad_ms']} → {v['good_ms']} ms" for k, v in cases.items()),
           all(cases[k]["good_ms"] < cases[k]["bad_ms"] / 5 for k in ("date_trunc", "coalesce", "type mismatch", "json", "contains")))

    # ------------------------------------------------------------------ 8. write cost and HOT updates
    def wal():
        return q("SELECT pg_current_wal_lsn()").fetchone()[0]

    def hot():
        q("SELECT pg_stat_force_next_flush()")
        q("SELECT 1")
        time.sleep(0.3)
        return q("SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relname = 'wt'").fetchone()

    def wal_diff(a, b):
        return float(q("SELECT pg_wal_lsn_diff(%s, %s)", (b, a)).fetchone()[0])

    idx_sets = {"primary key only": [], "old: pk + 3 single-column": ["(workspace_id)", "(assignee_id)", "(state)"],
                "new: pk + my-queue partial": ["(workspace_id, assignee_id, updated_at DESC) WHERE state <> 'closed'"],
                "pk + 7 indexes": ["(workspace_id)", "(assignee_id)", "(state)", "(created_at)", "(requester_id)", "(external_ref)",
                                   "(workspace_id, assignee_id, updated_at DESC) WHERE state <> 'closed'"]}
    writes = {}
    for name, idxs in idx_sets.items():
        q("DROP TABLE IF EXISTS wt")
        q("CREATE TABLE wt (LIKE tickets INCLUDING DEFAULTS) WITH (fillfactor = 90)")
        q("ALTER TABLE wt ADD PRIMARY KEY (id)")
        for i, cols in enumerate(idxs):
            q(f"CREATE INDEX wt_i{i} ON wt {cols}")
        q("CHECKPOINT")
        a, t = wal(), time.perf_counter()
        q("INSERT INTO wt SELECT * FROM tickets WHERE id <= 300000")
        ins_s, ins_wal = time.perf_counter() - t, wal_diff(a, wal())
        q("VACUUM ANALYZE wt")
        ids = [r[0] for r in q("SELECT id FROM wt WHERE state <> 'closed' ORDER BY random() LIMIT 5000").fetchall()]
        q("CHECKPOINT")
        for i in ids:                                   # warm-up pass: the first change to each page after a checkpoint
            q(f"UPDATE wt SET updated_at = now() WHERE id = {i}")   # logs a full copy of the page; measure the steady state
        q("VACUUM wt")
        h0 = hot()
        fpi0 = q("SELECT wal_fpi FROM pg_stat_wal").fetchone()[0]
        a, t = wal(), time.perf_counter()
        for i in ids:
            q(f"UPDATE wt SET updated_at = now() WHERE id = {i}")
        upd_s, upd_wal = time.perf_counter() - t, wal_diff(a, wal())
        fpi = q("SELECT wal_fpi FROM pg_stat_wal").fetchone()[0] - fpi0
        h1 = hot()
        a2, t2 = wal(), time.perf_counter()
        for i in ids[:2000]:
            q(f"UPDATE wt SET priority = 1 + priority % 4 WHERE id = {i}")
        pr_s, pr_wal = time.perf_counter() - t2, wal_diff(a2, wal())
        h2 = hot()
        writes[name] = {"insert_300k_s": round(ins_s, 2), "insert_wal_mb": mb(ins_wal), "update_updated_at_us": round(1e6 * upd_s / len(ids)),
                        "update_updated_at_wal_bytes": round(upd_wal / len(ids)), "update_full_page_images": fpi, "update_priority_wal_bytes": round(pr_wal / 2000),
                        "hot_pct": round(100 * (h1[1] - h0[1]) / max(1, h1[0] - h0[0]), 1), "priority_hot_pct": round(100 * (h2[1] - h1[1]) / max(1, h2[0] - h1[0]), 1),
                        "update_priority_us": round(1e6 * pr_s / 2000), "index_mb": mb(sum(size(f"wt_i{i}") for i in range(len(idxs))) + size("wt_pkey"))}
        print("writes", name, writes[name], flush=True)
    q("DROP TABLE wt")
    data["writes"] = writes
    w = writes
    report("Every index makes writes more expensive", "inserts slow down and write more WAL as indexes are added",
           " · ".join(f"{k}: insert {v['insert_300k_s']} s, {v['insert_wal_mb']} MB WAL" for k, v in w.items()),
           w["pk + 7 indexes"]["insert_300k_s"] > 1.5 * w["primary key only"]["insert_300k_s"] and w["pk + 7 indexes"]["insert_wal_mb"] > 1.5 * w["primary key only"]["insert_wal_mb"])
    report("Indexing updated_at gives up HOT updates", "an update that changes an indexed column writes a new entry in every index",
           f"old set: {w['old: pk + 3 single-column']['hot_pct']}% HOT, {w['old: pk + 3 single-column']['update_updated_at_wal_bytes']} B WAL per update · "
           f"new set: {w['new: pk + my-queue partial']['hot_pct']}% HOT, {w['new: pk + my-queue partial']['update_updated_at_wal_bytes']} B · "
           f"7 indexes: {w['pk + 7 indexes']['hot_pct']}% HOT, {w['pk + 7 indexes']['update_updated_at_wal_bytes']} B",
           w["old: pk + 3 single-column"]["hot_pct"] > 50 and w["pk + 7 indexes"]["hot_pct"] < 50)

    # ------------------------------------------------------------------ 9. unused indexes
    q("SELECT pg_stat_force_next_flush()")
    time.sleep(0.5)
    unused = q("SELECT indexrelname, idx_scan, pg_relation_size(indexrelid) FROM pg_stat_user_indexes WHERE relname = 'tickets' ORDER BY idx_scan, 3 DESC").fetchall()
    data["unused"] = [{"index": n, "scans": s, "mb": mb(b)} for n, s, b in unused]

    # ------------------------------------------------------------------ 10. building on a live table
    def update_during(stmt):
        done = threading.Event()

        def build():
            with psycopg.connect(DSN + " dbname=ul_indexes", autocommit=True) as c2:
                c2.execute("SET max_parallel_maintenance_workers = 0")
                c2.execute(stmt)
            done.set()
        th = threading.Thread(target=build)
        tb = time.perf_counter()
        th.start()
        time.sleep(1.0)
        t = time.perf_counter()
        q("UPDATE tickets SET priority = priority WHERE id = 42")
        wait = time.perf_counter() - t
        th.join()
        return round(1000 * wait), round(time.perf_counter() - tb, 1)

    plain_wait, plain_build = update_during("CREATE INDEX tmp_build ON tickets (subject)")
    q("DROP INDEX tmp_build")
    conc_wait, conc_build = update_during("CREATE INDEX CONCURRENTLY tmp_build ON tickets (subject)")
    q("DROP INDEX tmp_build")
    try:
        q("CREATE UNIQUE INDEX CONCURRENTLY tickets_requester_uniq ON tickets (requester_id)")
        failed = False
    except psycopg.Error as e:
        failed = type(e).__name__
    valid = q("SELECT indisvalid FROM pg_index WHERE indexrelid = 'tickets_requester_uniq'::regclass").fetchone()[0]
    inv_plan = explain("SELECT id FROM tickets WHERE requester_id = 4242", runs=1)
    q("DROP INDEX tickets_requester_uniq")
    data["online"] = {"plain_update_wait_ms": plain_wait, "plain_build_s": plain_build, "concurrent_update_wait_ms": conc_wait, "concurrent_build_s": conc_build,
                      "failed_build_error": failed, "left_valid": valid, "invalid_used": "tickets_requester_uniq" in inv_plan["indexes"]}
    report("CREATE INDEX blocks writes; CONCURRENTLY doesn't", "a plain build locks the table against writes until it finishes",
           f"UPDATE during plain build waited {plain_wait} ms (build {plain_build} s) · during CONCURRENTLY {conc_wait} ms (build {conc_build} s)",
           plain_wait > 1000 and conc_wait < 200)
    report("A failed concurrent build leaves an INVALID index", "check pg_index.indisvalid after every build",
           f"error {failed}; index left behind with indisvalid = {valid}; used by planner: {data['online']['invalid_used']}",
           failed and valid is False and not data["online"]["invalid_used"])

    (HERE / "plans.txt").write_text("\n".join(plans))
    data["total_s"] = round(time.time() - t0)
    (HERE / "indexes_results.json").write_text(json.dumps(data, indent=1, default=str))
    print(f"{sum(results)}/{len(results)} passed in {data['total_s']} s")
