"""Offset vs keyset pagination on PostgreSQL 17, for Part 8."""
import json
import time

import psycopg

DSN = "host=localhost port=55432 user=postgres password=test"
results, data = [], {}
PAGE = 50


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def c():
    return psycopg.connect(DSN, autocommit=True)


def setup(k):
    k.execute("DROP TABLE IF EXISTS tickets CASCADE")
    k.execute("CREATE TABLE tickets (id bigint PRIMARY KEY, workspace_id int, created_at timestamptz, subject text)")
    # 2M tickets for workspace 7; created_at has many ties (one value per 10 tickets)
    k.execute("""INSERT INTO tickets
                 SELECT g, 7, timestamptz '2024-01-01' + (g / 10) * interval '1 second', 'ticket ' || g
                 FROM generate_series(1, 2000000) g""")
    k.execute("CREATE INDEX tickets_ws_created_id ON tickets (workspace_id, created_at DESC, id DESC)")
    k.execute("VACUUM ANALYZE tickets")


def timed(k, sql, args, runs=5):
    best, buffers = None, None
    for _ in range(runs):
        plan = k.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, args).fetchone()[0][0]
        t = plan["Execution Time"]
        if best is None or t < best:
            best = t
            p = plan["Plan"]
            buffers = p.get("Shared Hit Blocks", 0) + p.get("Shared Read Blocks", 0)
    return round(best, 2), buffers


OFFSET_SQL = """SELECT id, created_at, subject FROM tickets WHERE workspace_id = 7
                ORDER BY created_at DESC, id DESC LIMIT %s OFFSET %s"""
KEYSET_SQL = """SELECT id, created_at, subject FROM tickets WHERE workspace_id = 7
                AND (created_at, id) < (%s, %s) ORDER BY created_at DESC, id DESC LIMIT %s"""


def depth(k):
    rows = []
    for page in (1, 10, 100, 1000, 10000, 39999):
        off = (page - 1) * PAGE
        t_off, b_off = timed(k, OFFSET_SQL, (PAGE, off))
        if page == 1:
            t_key, b_key = timed(k, """SELECT id, created_at, subject FROM tickets WHERE workspace_id = 7
                                       ORDER BY created_at DESC, id DESC LIMIT %s""", (PAGE,))
        else:
            last = k.execute("SELECT created_at, id FROM tickets WHERE workspace_id = 7 ORDER BY created_at DESC, id DESC "
                             "LIMIT 1 OFFSET %s", (off - 1,)).fetchone()
            t_key, b_key = timed(k, KEYSET_SQL, (last[0], last[1], PAGE))
        row = {"page": page, "offset_ms": t_off, "offset_buffers": b_off, "keyset_ms": t_key, "keyset_buffers": b_key}
        rows.append(row); print("  ", row, flush=True)
    data["depth"] = rows
    last = rows[-1]
    report("Deep pages: OFFSET vs keyset", "OFFSET cost grows with page number; keyset stays flat",
           f"page 1: offset {rows[0]['offset_ms']} ms / keyset {rows[0]['keyset_ms']} ms; page {last['page']}: "
           f"offset {last['offset_ms']} ms ({last['offset_buffers']} pages read) / keyset {last['keyset_ms']} ms ({last['keyset_buffers']} pages)",
           last["offset_ms"] > 50 * last["keyset_ms"])


def shifting(k):
    # Agent reads page 1, then 5 new tickets arrive, then the agent reads page 2.
    p1 = k.execute(OFFSET_SQL, (PAGE, 0)).fetchall()
    last = (p1[-1][1], p1[-1][0])
    k.execute("""INSERT INTO tickets SELECT 3000000 + g, 7, now() + g * interval '1 second', 'new ' || g
                 FROM generate_series(1, 5) g""")
    p2_off = k.execute(OFFSET_SQL, (PAGE, PAGE)).fetchall()
    p2_key = k.execute(KEYSET_SQL, (last[0], last[1], PAGE)).fetchall()
    ids1 = {r[0] for r in p1}
    dup_off = len(ids1 & {r[0] for r in p2_off})
    dup_key = len(ids1 & {r[0] for r in p2_key})
    k.execute("DELETE FROM tickets WHERE id > 3000000")
    # Deletion case: 5 tickets on page 1 are closed/removed between requests
    p1 = k.execute(OFFSET_SQL, (PAGE, 0)).fetchall()
    last = (p1[-1][1], p1[-1][0])
    truth_p2 = [r[0] for r in k.execute(OFFSET_SQL, (PAGE, PAGE)).fetchall()]
    removed = [r[0] for r in p1[:5]]
    k.execute("CREATE TEMP TABLE IF NOT EXISTS removed AS SELECT * FROM tickets WHERE false")
    k.execute("INSERT INTO removed SELECT * FROM tickets WHERE id = ANY(%s)", (removed,))
    k.execute("DELETE FROM tickets WHERE id = ANY(%s)", (removed,))
    p2_off = [r[0] for r in k.execute(OFFSET_SQL, (PAGE, PAGE)).fetchall()]
    p2_key = [r[0] for r in k.execute(KEYSET_SQL, (last[0], last[1], PAGE)).fetchall()]
    skipped_off = len(set(truth_p2[:5]) - set(p2_off))
    skipped_key = len(set(truth_p2) - set(p2_key))
    k.execute("INSERT INTO tickets SELECT * FROM removed"); k.execute("DROP TABLE removed")
    data["shifting"] = {"insert_duplicates_offset": dup_off, "insert_duplicates_keyset": dup_key,
                        "delete_skipped_offset": skipped_off, "delete_skipped_keyset": skipped_key}
    report("5 new tickets arrive between page 1 and page 2", "OFFSET repeats 5 tickets on page 2; keyset repeats none",
           f"duplicates: offset={dup_off}, keyset={dup_key}", dup_off == 5 and dup_key == 0)
    report("5 tickets on page 1 are deleted before page 2", "OFFSET silently skips 5 tickets; keyset skips none",
           f"skipped: offset={skipped_off}, keyset={skipped_key}", skipped_off == 5 and skipped_key == 0)


def _walk(k, size, pages, with_id):
    """Paginate `pages` of `size`. with_id=False is the broken created_at-only cursor."""
    seen, cursor = [], None
    for _ in range(pages):
        if cursor is None:
            if with_id:
                rows = k.execute("""SELECT id, created_at FROM tickets WHERE workspace_id = 7
                                    ORDER BY created_at DESC, id DESC LIMIT %s""", (size,)).fetchall()
            else:
                rows = k.execute("""SELECT id, created_at FROM tickets WHERE workspace_id = 7
                                    ORDER BY created_at DESC LIMIT %s""", (size,)).fetchall()
        else:
            if with_id:
                rows = k.execute("""SELECT id, created_at FROM tickets WHERE workspace_id = 7
                                    AND (created_at, id) < (%s, %s) ORDER BY created_at DESC, id DESC LIMIT %s""",
                                 (cursor[0], cursor[1], size)).fetchall()
            else:
                rows = k.execute("""SELECT id, created_at FROM tickets WHERE workspace_id = 7 AND created_at < %s
                                    ORDER BY created_at DESC LIMIT %s""", (cursor[0], size)).fetchall()
        seen += [r[0] for r in rows]
        cursor = (rows[-1][1], rows[-1][0])
    return seen


def tie_breaker(k):
    # Cursor on created_at only (not unique): walk 30 pages of size 7 and compare with the true order.
    # Negative control: the same walk with (created_at, id) must miss nothing in-range.
    size, pages = 7, 30
    truth = [r[0] for r in k.execute("""SELECT id FROM tickets WHERE workspace_id = 7 ORDER BY created_at DESC, id DESC
                                        LIMIT %s""", (size * pages,)).fetchall()]
    seen_bad = _walk(k, size, pages, with_id=False)
    seen_good = _walk(k, size, pages, with_id=True)
    missing_bad = len([i for i in truth[:len(seen_bad)] if i not in set(seen_bad)])
    missing_good = len([i for i in truth[:len(seen_good)] if i not in set(seen_good)])
    data["tie_breaker"] = {"pages": pages, "page_size": size, "rows_returned": len(seen_bad),
                           "missed_within_range": missing_bad, "missed_with_id_tiebreaker": missing_good}
    report("Cursor on created_at alone (10 tickets share each timestamp)",
           "rows sharing a timestamp with the page boundary are silently skipped; (created_at, id) skips none",
           f"{pages} pages of {size}: broken cursor returned {len(seen_bad)} rows, skipped {missing_bad}; "
           f"with id tie-breaker skipped {missing_good}",
           missing_bad >= 20 and missing_good == 0 and len(seen_bad) == size * pages)


def backwards(k):
    # Previous page: flip the comparison and the order, then reverse in the application.
    page3 = k.execute(OFFSET_SQL, (PAGE, 2 * PAGE)).fetchall()
    first_of_page3 = (page3[0][1], page3[0][0])
    prev = k.execute("""SELECT id, created_at, subject FROM tickets WHERE workspace_id = 7
                        AND (created_at, id) > (%s, %s) ORDER BY created_at ASC, id ASC LIMIT %s""",
                     (first_of_page3[0], first_of_page3[1], PAGE)).fetchall()[::-1]
    truth = k.execute(OFFSET_SQL, (PAGE, PAGE)).fetchall()
    t, b = timed(k, """SELECT id FROM tickets WHERE workspace_id = 7 AND (created_at, id) > (%s, %s)
                       ORDER BY created_at ASC, id ASC LIMIT %s""", (first_of_page3[0], first_of_page3[1], PAGE))
    data["backwards"] = {"matches": prev == truth, "ms": t}
    report("Backward pagination with a flipped keyset query", "returns exactly the previous page, using the same index",
           f"matches page 2: {prev == truth}; {t} ms", prev == truth)


def counting(k):
    t, b = timed(k, "SELECT count(*) FROM tickets WHERE workspace_id = 7", (), runs=3)
    t2, _ = timed(k, "SELECT 1 FROM tickets WHERE workspace_id = 7 ORDER BY created_at DESC, id DESC LIMIT 51", (), runs=3)
    est = k.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = 'tickets'").fetchone()[0]
    data["count"] = {"exact_count_ms": t, "has_more_check_ms": t2, "estimate": est}
    report("Exact total count for 'page X of Y'", "counting every matching row costs far more than fetching a page",
           f"count(*): {t} ms; fetching 51 rows to know 'is there a next page': {t2} ms; planner estimate: {est:,}",
           t > 20 * t2)


if __name__ == "__main__":
    with c() as k:
        setup(k)
        depth(k)
        shifting(k)
        tie_breaker(k)
        backwards(k)
        counting(k)
    with open(__file__.replace("pagination_tests.py", "pagination_results.json"), "w") as f:
        json.dump(data, f, indent=1, default=str)
    print(f"{sum(results)}/{len(results)} passed")
