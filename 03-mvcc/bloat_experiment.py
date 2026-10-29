"""Measures how one idle Repeatable Read session changes table size and query cost over time.

Each round updates 10% of the tickets (like a busy day of agents working), then runs VACUUM
(standing in for autovacuum). Run once without a long transaction and once with one.
"""
import json
import re

import psycopg

ROWS, ROUNDS, PER_ROUND = 50_000, 12, 5_000


def pg():
    return psycopg.connect("host=localhost port=55432 user=postgres password=test", autocommit=True)


def setup(c):
    c.execute("DROP TABLE IF EXISTS tickets")
    c.execute("""CREATE TABLE tickets (id int PRIMARY KEY, workspace_id int, assignee_id int, state text,
                 priority text, updated_at timestamptz, subject text)""")
    c.execute("""INSERT INTO tickets SELECT g, g %% 50, g %% 400, 'open', 'normal',
                 now() - (g || ' seconds')::interval, repeat('x', 120) FROM generate_series(1, %s) g""", (ROWS,))
    c.execute("CREATE INDEX tickets_queue_idx ON tickets (workspace_id, assignee_id, updated_at DESC)")
    c.execute("VACUUM (ANALYZE) tickets")


def queue_cost(c):
    plan = c.execute("""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                        SELECT id, subject FROM tickets WHERE workspace_id = 7 AND assignee_id = 7
                        ORDER BY updated_at DESC LIMIT 50""").fetchone()[0][0]
    p = plan["Plan"]
    return p["Shared Hit Blocks"] + p.get("Shared Read Blocks", 0)


def run(hold):
    c = pg()
    setup(c)
    holder = None
    if hold:
        holder = pg()
        holder.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        holder.execute("SELECT count(*) FROM tickets")
    series = []
    for r in range(ROUNDS + 1):
        size = c.execute("SELECT pg_relation_size('tickets'), pg_relation_size('tickets_queue_idx')").fetchone()
        series.append({"round": r, "table_mb": round(size[0] / 2**20, 1), "index_mb": round(size[1] / 2**20, 2),
                       "queue_pages": queue_cost(c)})
        if r == ROUNDS:
            break
        lo = (r * PER_ROUND) % ROWS
        c.execute("UPDATE tickets SET updated_at = now(), priority = 'urgent' WHERE id > %s AND id <= %s",
                  (lo, lo + PER_ROUND))
        # agents on workspace 7 keep touching their own tickets too
        c.execute("UPDATE tickets SET updated_at = now() WHERE workspace_id = 7 AND assignee_id = 7")
        c.execute("VACUUM tickets")
    if holder:
        holder.execute("COMMIT")
        c.execute("VACUUM tickets")
        size = c.execute("SELECT pg_relation_size('tickets')").fetchone()[0]
        series.append({"round": "after commit + VACUUM", "table_mb": round(size / 2**20, 1),
                       "queue_pages": queue_cost(c)})
    return series


if __name__ == "__main__":
    out = {"without_long_tx": run(False), "with_idle_repeatable_read": run(True)}
    print(json.dumps(out, indent=1))
    with open(__file__.replace(".py", "_results.json"), "w") as f:
        json.dump(out, f, indent=1)
