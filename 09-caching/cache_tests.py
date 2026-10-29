"""Cache-aside behaviors and races for Part 9, against PostgreSQL 17 and Redis 7.4.

Races are replayed deterministically by running the steps of two "requests" in a fixed interleaving.
"""
import json
import statistics
import time

import psycopg
import redis

PG = "host=localhost port=55432 user=postgres password=test"
R = redis.Redis(host="localhost", port=56379, decode_responses=True)
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def db():
    return psycopg.connect(PG, autocommit=True)


def setup(k):
    k.execute("DROP TABLE IF EXISTS comments, tickets")
    k.execute("CREATE TABLE tickets (id int PRIMARY KEY, subject text, priority text, version int NOT NULL DEFAULT 1)")
    k.execute("CREATE TABLE comments (id serial PRIMARY KEY, ticket_id int, body text, created_at timestamptz)")
    k.execute("INSERT INTO tickets SELECT g, 'ticket ' || g, 'normal', 1 FROM generate_series(1, 20000) g")
    k.execute("""INSERT INTO comments (ticket_id, body, created_at)
                 SELECT (g % 20000) + 1, repeat('comment ', 20), now() - g * interval '1 minute' FROM generate_series(1, 400000) g""")
    k.execute("CREATE INDEX ON comments (ticket_id, created_at)")
    k.execute("VACUUM ANALYZE")
    R.flushall()


PAGE_SQL = """SELECT t.id, t.subject, t.priority, t.version,
                     (SELECT json_agg(c ORDER BY c.created_at) FROM comments c WHERE c.ticket_id = t.id) AS comments
              FROM tickets t WHERE t.id = %s"""


def load_from_db(k, tid):
    row = k.execute(PAGE_SQL, (tid,)).fetchone()
    return None if row is None else {"id": row[0], "subject": row[1], "priority": row[2], "version": row[3], "n": len(row[4] or [])}


DASH_SQL = """SELECT t.priority, count(*) FROM comments c JOIN tickets t ON t.id = c.ticket_id
               WHERE c.created_at > now() - interval '60 days' GROUP BY t.priority"""


def speed(k):
    def med(fn, n):
        ts = []
        for i in range(n):
            t0 = time.perf_counter(); fn(i); ts.append(time.perf_counter() - t0)
        return statistics.median(ts) * 1000
    for i in range(200):
        R.set(f"ticket:{1 + i}", json.dumps(load_from_db(k, 1 + i)))
    cheap_db = med(lambda i: load_from_db(k, 1 + i % 200), 300)
    cheap_cache = med(lambda i: json.loads(R.get(f"ticket:{1 + i % 200}")), 300)
    R.set("dash:7", json.dumps(k.execute(DASH_SQL).fetchall()))
    dash_db = med(lambda i: k.execute(DASH_SQL).fetchall(), 15)
    dash_cache = med(lambda i: json.loads(R.get("dash:7")), 300)
    data["speed_ms"] = {"ticket_page_db": round(cheap_db, 3), "ticket_page_cache": round(cheap_cache, 3),
                        "dashboard_db": round(dash_db, 2), "dashboard_cache": round(dash_cache, 3)}
    ticket_x = cheap_db / cheap_cache
    dash_x = dash_db / dash_cache
    report("Indexed ticket page: PostgreSQL vs Redis", "a fast, indexed query gains little from a cache (both are one round trip)",
           f"median: database {cheap_db:.3f} ms, Redis {cheap_cache:.3f} ms ({ticket_x:.1f}×)",
           ticket_x < 3 and cheap_db < 2.0)
    report("Workspace dashboard aggregate: PostgreSQL vs Redis", "an expensive query gains enormously — far more than the ticket page",
           f"median: database {dash_db:.1f} ms, Redis {dash_cache:.3f} ms ({dash_x:,.0f}×); ticket was only {ticket_x:.1f}×",
           dash_x > 50 and dash_x > 10 * ticket_x)


def stale_without_invalidation(k):
    R.flushall()
    R.set("ticket:1", json.dumps(load_from_db(k, 1)), ex=300)
    k.execute("UPDATE tickets SET priority = 'urgent', version = version + 1 WHERE id = 1")
    cached = json.loads(R.get("ticket:1"))["priority"]
    ttl = R.ttl("ticket:1")
    data["stale_no_invalidation"] = {"cached_priority": cached, "db_priority": "urgent", "ttl_left_s": ttl}
    report("Update the database, don't touch the cache", "readers see the old priority until the TTL expires",
           f"cache says {cached}, database says urgent, {ttl} s of TTL left", cached == "normal")


def race_update_on_write(k):
    """Two writers update DB then SET the cache; they finish in the opposite order."""
    R.flushall(); k.execute("UPDATE tickets SET priority='normal', version=1 WHERE id=2")
    # A: DB high
    k.execute("UPDATE tickets SET priority='high', version=version+1 WHERE id=2")
    a_value = load_from_db(k, 2)
    # B: DB urgent, B sets cache
    k.execute("UPDATE tickets SET priority='urgent', version=version+1 WHERE id=2")
    R.set("ticket:2", json.dumps(load_from_db(k, 2)), ex=300)
    # A's cache write arrives late
    R.set("ticket:2", json.dumps(a_value), ex=300)
    cached = json.loads(R.get("ticket:2"))["priority"]
    dbv = k.execute("SELECT priority FROM tickets WHERE id=2").fetchone()[0]
    data["race_update_on_write"] = {"cache": cached, "db": dbv}
    report("Race: update the cache on every write, two writers out of order",
           "the older value lands last and stays in the cache", f"cache={cached}, database={dbv}", cached != dbv)


def race_delete_on_write(k):
    """Reader misses and loads from DB; writer updates DB and deletes the key; reader then fills the cache."""
    R.flushall(); k.execute("UPDATE tickets SET priority='normal', version=1 WHERE id=3")
    assert R.get("ticket:3") is None
    reader_value = load_from_db(k, 3)                        # reader: miss, reads normal (then gets slow: GC pause, etc.)
    k.execute("UPDATE tickets SET priority='urgent', version=version+1 WHERE id=3")  # writer
    R.delete("ticket:3")                                     # writer invalidates
    R.set("ticket:3", json.dumps(reader_value), ex=300)      # reader finally fills the cache with what it read
    cached = json.loads(R.get("ticket:3"))["priority"]
    data["race_delete_on_write"] = {"cache": cached, "db": "urgent", "ttl_left_s": R.ttl("ticket:3")}
    report("Race: delete on write, slow reader fills the cache afterwards",
           "the stale value the reader loaded is cached after the delete", f"cache={cached}, database=urgent", cached == "normal")


SET_IF_NEWER = R.register_script("""
local cur = redis.call('GET', KEYS[1])
if cur then
  local c = cjson.decode(cur)
  local v = tonumber(c['version'])
  local new = tonumber(ARGV[2])
  -- a tombstone accepts the version it names; a real value only accepts strictly newer ones
  if c['tombstone'] then
    if new < v then return 0 end
  elseif new <= v then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
return 1
""")
TOMBSTONE = R.register_script("""
-- on write: remember the newest version so older fills are rejected
redis.call('SET', KEYS[1], cjson.encode({version = tonumber(ARGV[1]), tombstone = true}), 'EX', ARGV[2])
return 1
""")


def versioned_fix(k):
    # Race 1 again, with version-checked writes
    R.flushall(); k.execute("UPDATE tickets SET priority='normal', version=1 WHERE id=2")
    k.execute("UPDATE tickets SET priority='high', version=version+1 WHERE id=2"); a_value = load_from_db(k, 2)
    k.execute("UPDATE tickets SET priority='urgent', version=version+1 WHERE id=2"); b_value = load_from_db(k, 2)
    SET_IF_NEWER(keys=["ticket:2"], args=[json.dumps(b_value), b_value["version"], 300])
    late = SET_IF_NEWER(keys=["ticket:2"], args=[json.dumps(a_value), a_value["version"], 300])
    c1 = json.loads(R.get("ticket:2"))["priority"]
    # Race 2 again: writer leaves a tombstone with the new version instead of a plain delete
    R.flushall(); k.execute("UPDATE tickets SET priority='normal', version=1 WHERE id=3")
    reader_value = load_from_db(k, 3)
    new_version = k.execute("UPDATE tickets SET priority='urgent', version=version+1 WHERE id=3 RETURNING version").fetchone()[0]
    TOMBSTONE(keys=["ticket:3"], args=[new_version, 30])
    rejected = SET_IF_NEWER(keys=["ticket:3"], args=[json.dumps(reader_value), reader_value["version"], 300]) == 0
    fresh = load_from_db(k, 3)
    SET_IF_NEWER(keys=["ticket:3"], args=[json.dumps(fresh), fresh["version"], 300])
    c2 = json.loads(R.get("ticket:3")).get("priority")
    data["versioned_fix"] = {"race1_cache": c1, "race1_late_write_accepted": bool(late), "race2_stale_rejected": rejected, "race2_cache": c2}
    report("Fix: only write to the cache if the version is newer (Lua script)",
           "the late, older write is rejected in both races", f"race 1: cache={c1}, late write accepted={bool(late)}; "
           f"race 2: stale fill rejected={rejected}, cache after next fill={c2}", c1 == "urgent" and not late and rejected and c2 == "urgent")


def negative_caching(k):
    # 1,000 requests for 50 ticket ids that don't exist (deleted tickets in old email links, scanners, bugs)
    def run(negative):
        R.flushall(); hits_db = 0
        for i in range(1000):
            key = f"ticket:{900000 + i % 50}"
            v = R.get(key)
            if v is None:
                hits_db += 1
                row = load_from_db(k, 900000 + i % 50)
                if row is None and negative:
                    R.set(key, "null", ex=30)
        return hits_db
    without, with_ = run(False), run(True)
    # The danger: the ticket is created after "doesn't exist" was cached
    R.set("ticket:950000", "null", ex=30)
    k.execute("INSERT INTO tickets (id, subject, priority) VALUES (950000, 'just created', 'normal')")
    seen = R.get("ticket:950000")
    k.execute("DELETE FROM tickets WHERE id = 950000")
    data["negative_caching"] = {"db_queries_without": without, "db_queries_with": with_, "created_but_cached_as": seen}
    report("Negative caching for missing tickets", "1,000 lookups of 50 missing ids hit the database 1,000 times without it, 50 with it",
           f"database queries: without={without}, with={with_}; a ticket created after a cached miss still reads as {seen!r}",
           without == 1000 and with_ == 50 and seen == "null")


def eviction():
    out = {}
    for policy in ("noeviction", "allkeys-lru"):
        R.flushall(); R.config_set("maxmemory", "4mb"); R.config_set("maxmemory-policy", policy)
        err, written = None, 0
        try:
            for i in range(20000):
                R.set(f"ticket:{i}", "x" * 400); written += 1
        except redis.ResponseError as e:
            err = str(e).split(".")[0]
        out[policy] = {"written": written, "keys_left": R.dbsize(), "error": err,
                       "evicted": int(R.info("stats")["evicted_keys"])}
        R.config_resetstat()
    R.config_set("maxmemory", "0"); R.config_set("maxmemory-policy", "noeviction")
    data["eviction"] = out
    report("Redis at maxmemory: noeviction vs allkeys-lru", "noeviction rejects new writes with OOM; allkeys-lru evicts old keys",
           f"noeviction: wrote {out['noeviction']['written']} then '{out['noeviction']['error']}'; "
           f"allkeys-lru: wrote all {out['allkeys-lru']['written']}, kept {out['allkeys-lru']['keys_left']}, evicted {out['allkeys-lru']['evicted']}",
           out["noeviction"]["error"] and out["allkeys-lru"]["error"] is None and out["allkeys-lru"]["evicted"] > 0)


if __name__ == "__main__":
    with db() as k:
        setup(k)
        speed(k)
        stale_without_invalidation(k)
        race_update_on_write(k)
        race_delete_on_write(k)
        versioned_fix(k)
        negative_caching(k)
    eviction()
    with open(__file__.replace("cache_tests.py", "cache_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
