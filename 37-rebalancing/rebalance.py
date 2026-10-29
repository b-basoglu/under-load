"""Partition rebalancing, measured for Part 37.

Two PostgreSQL 17 shards (1 CPU each): ul-src (55460) and ul-dst (55461). Tenant 7 (300,000 tickets) moves from src to
dst while foreground traffic (reads and updates across all tenants, including tenant 7) keeps running.
"""
import json
import random
import threading
import time

import psycopg

SRC = "host=localhost port=55460 user=postgres password=test dbname=postgres"
DST = "host=localhost port=55461 user=postgres password=test dbname=postgres"
T = 7
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else 0


def setup():
    for d in (SRC, DST):
        with psycopg.connect(d, autocommit=True) as c:
            c.execute("DROP TABLE IF EXISTS tickets, changes, moved_tenants CASCADE")
            c.execute("CREATE TABLE tickets (id bigint PRIMARY KEY, tenant_id int, status text, body text, v int DEFAULT 0)")
            c.execute("CREATE INDEX ON tickets (tenant_id, id)")
            c.execute("CREATE TABLE moved_tenants (tenant_id int PRIMARY KEY)")
    with psycopg.connect(SRC, autocommit=True) as c:
        c.execute("INSERT INTO tickets SELECT g, CASE WHEN g <= 300000 THEN 7 ELSE 1 + g % 20 END, 'open', repeat('x', 400), 0 FROM generate_series(1, 600000) g")
        c.execute("UPDATE tickets SET tenant_id = 8 WHERE tenant_id = 7 AND id > 300000")
        c.execute("CREATE TABLE changes (seq bigserial PRIMARY KEY, id bigint)")
        # change capture for the moving tenant, and a fence that rejects its writes once it has moved
        c.execute("""CREATE OR REPLACE FUNCTION track() RETURNS trigger AS $$
            BEGIN
              IF EXISTS (SELECT 1 FROM moved_tenants WHERE tenant_id = NEW.tenant_id) THEN
                RAISE EXCEPTION 'tenant % has moved', NEW.tenant_id USING ERRCODE = 'P0001';
              END IF;
              IF NEW.tenant_id = 7 AND current_setting('lab.capture', true) IS DISTINCT FROM 'off' THEN
                INSERT INTO changes (id) VALUES (NEW.id);
              END IF;
              RETURN NEW;
            END $$ LANGUAGE plpgsql""")
        c.execute("CREATE TRIGGER t_track BEFORE INSERT OR UPDATE ON tickets FOR EACH ROW EXECUTE FUNCTION track()")
        c.execute("VACUUM ANALYZE tickets")


class Foreground:
    """800 ops/s: 70% point reads, 30% updates; 25% of operations hit tenant 7. Routing is a dict clients refresh."""
    def __init__(self, routing, refresh_every=0.0, threads=8, rate=800, seed=1):
        self.routing, self.refresh_every, self.rate = routing, refresh_every, rate
        self.lat, self.errors, self.stop = [], {"rejected": 0, "other": 0}, threading.Event()
        self.writes = {}              # id -> last v the client believes it wrote (and was told succeeded)
        self.lock = threading.Lock()
        self.ts = [threading.Thread(target=self.run, args=(seed + k,)) for k in range(threads)]
        self.t0 = time.time()
        for t in self.ts: t.start()

    def run(self, seed):
        rnd = random.Random(seed)
        conns = {"src": psycopg.connect(SRC, autocommit=True), "dst": psycopg.connect(DST, autocommit=True)}
        view = dict(self.routing)
        last_refresh = time.time()
        per = len(self.ts) / self.rate
        nxt = time.time()
        while not self.stop.is_set():
            nxt += per
            d = nxt - time.time()
            if d > 0:
                time.sleep(d)
            if time.time() - last_refresh >= self.refresh_every:
                view, last_refresh = dict(self.routing), time.time()
            tenant7 = rnd.random() < 0.25
            tid = rnd.randint(1, 300000) if tenant7 else rnd.randint(300001, 600000)
            shard = view.get(T, "src") if tenant7 else "src"
            t0 = time.perf_counter()
            for attempt in range(3):
                try:
                    if rnd.random() < 0.7:
                        conns[shard].execute("SELECT status, v FROM tickets WHERE id = %s", (tid,)).fetchone()
                    else:
                        v = conns[shard].execute("UPDATE tickets SET v = v + 1, status = 'pending' WHERE id = %s RETURNING v", (tid,)).fetchone()
                        if v and tenant7:
                            with self.lock:
                                self.writes[tid] = max(self.writes.get(tid, 0), v[0])
                    break
                except psycopg.errors.RaiseException:
                    with self.lock:
                        self.errors["rejected"] += 1
                    view, last_refresh = dict(self.routing), time.time()   # told it moved: refresh routing, retry
                    shard = view.get(T, "src")
                except Exception:
                    with self.lock:
                        self.errors["other"] += 1
                    break
            with self.lock:
                self.lat.append((time.time() - self.t0, (time.perf_counter() - t0) * 1000))

    def finish(self):
        self.stop.set()
        for t in self.ts: t.join()

    def window(self, a, b):
        return [l for t, l in self.lat if a <= t < b]


def copy(batch, max_rows_per_s=None, start_after=0, stop_after=None):
    """Copy tenant 7 in id order, in bounded batches. Upserts, so a resumed copy can overlap safely."""
    last, copied = start_after, 0
    t0 = time.time()
    with psycopg.connect(SRC, autocommit=True) as s, psycopg.connect(DST, autocommit=True) as d:
        while True:
            rows = s.execute("SELECT id, tenant_id, status, body, v FROM tickets WHERE tenant_id = %s AND id > %s ORDER BY id LIMIT %s", (T, last, batch)).fetchall()
            if not rows:
                break
            with d.cursor() as cur:
                cur.executemany("INSERT INTO tickets VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET status = excluded.status, v = excluded.v", rows)
            last, copied = rows[-1][0], copied + len(rows)
            if stop_after and copied >= stop_after:
                return last, copied
            if max_rows_per_s:
                ahead = copied / max_rows_per_s - (time.time() - t0)
                if ahead > 0:
                    time.sleep(ahead)
    return last, copied


def catch_up():
    """Apply captured changes to dst until the backlog is small. Returns rows applied."""
    applied = 0
    with psycopg.connect(SRC, autocommit=True) as s, psycopg.connect(DST, autocommit=True) as d:
        while True:
            rows = s.execute("DELETE FROM changes WHERE seq IN (SELECT seq FROM changes ORDER BY seq LIMIT 2000) RETURNING id").fetchall()
            if not rows:
                return applied
            ids = list({r[0] for r in rows})
            data_rows = s.execute("SELECT id, tenant_id, status, body, v FROM tickets WHERE id = ANY(%s)", (ids,)).fetchall()
            with d.cursor() as cur:
                cur.executemany("INSERT INTO tickets VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET status = excluded.status, v = excluded.v", data_rows)
            applied += len(ids)


def compare():
    with psycopg.connect(SRC) as s, psycopg.connect(DST) as d:
        a = dict(s.execute("SELECT id, v FROM tickets WHERE tenant_id = %s", (T,)).fetchall())
        b = dict(d.execute("SELECT id, v FROM tickets WHERE tenant_id = %s", (T,)).fetchall())
    missing = sum(1 for k in a if k not in b)
    stale = sum(1 for k in a if k in b and b[k] < a[k])
    return missing, stale


def lost_acknowledged(fg):
    """Writes the clients were told succeeded but that are not on the shard that now owns tenant 7."""
    with psycopg.connect(DST) as d:
        now = dict(d.execute("SELECT id, v FROM tickets WHERE tenant_id = %s", (T,)).fetchall())
    return sum(1 for k, v in fg.writes.items() if now.get(k, -1) < v)


def parallel_copy(streams, batch):
    """Unthrottled: several streams, each copying a slice of the id range."""
    def part(lo, hi):
        last = lo
        with psycopg.connect(SRC, autocommit=True) as s, psycopg.connect(DST, autocommit=True) as d:
            while True:
                rows = s.execute("SELECT id, tenant_id, status, body, v FROM tickets WHERE tenant_id = %s AND id > %s AND id <= %s ORDER BY id LIMIT %s",
                                 (T, last, hi, batch)).fetchall()
                if not rows:
                    return
                with d.cursor() as cur:
                    cur.executemany("INSERT INTO tickets VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET status = excluded.status, v = excluded.v", rows)
                last = rows[-1][0]
    step = 300000 // streams
    ts = [threading.Thread(target=part, args=(k * step, (k + 1) * step)) for k in range(streams)]
    for t in ts: t.start()
    for t in ts: t.join()


def scenario_speed(batch, rate_cap, label, streams=1):
    setup()
    routing = {T: "src"}
    fg = Foreground(routing)
    time.sleep(4)
    t_start = time.time() - fg.t0
    t0 = time.time()
    with psycopg.connect(SRC, autocommit=True) as c:
        c.execute("TRUNCATE changes")
    if streams > 1:
        parallel_copy(streams, batch)
    else:
        copy(batch, rate_cap)
    dur = time.time() - t0
    t_end = time.time() - fg.t0
    time.sleep(3)
    fg.finish()
    base = fg.window(1.5, t_start)                     # skip connection warm-up at the start
    during = fg.window(t_start, t_end)
    series = []
    for k in range(int(t_end) + 3):
        w = fg.window(k, k + 1)
        series.append({"t": k, "p99_ms": round(pct(w, .99), 1)})
    return {"label": label, "copy_s": round(dur, 1), "rows_per_s": round(300000 / dur), "baseline_p99_ms": round(pct(base, .99), 1),
            "during_p99_ms": round(pct(during, .99), 1), "copy_window": [round(t_start, 1), round(t_end, 1)], "series": series}


def scenario_cutover(mode):
    """mode: 'naive' (copy, then flip routing) | 'catch-up' (copy, catch up, fence, final catch-up, flip)
             | 'stale-clients-no-fence' | 'stale-clients-fence'"""
    setup()
    routing = {T: "src"}
    stale = mode.startswith("stale")
    fg = Foreground(routing, refresh_every=2.0 if stale else 0.0)
    time.sleep(2)
    with psycopg.connect(SRC, autocommit=True) as c:
        c.execute("TRUNCATE changes")
    copy(5000, 40000)
    out = {"mode": mode}
    if mode == "naive":
        routing[T] = "dst"
    else:
        caught = catch_up()
        t_fence = time.perf_counter()
        if mode != "stale-clients-no-fence":
            with psycopg.connect(SRC, autocommit=True) as c:
                c.execute("INSERT INTO moved_tenants VALUES (%s)", (T,))       # writes for tenant 7 on src now fail
        final = catch_up()
        routing[T] = "dst"
        out.update({"caught_up_rows": caught, "final_rows": final, "fence_to_flip_ms": round((time.perf_counter() - t_fence) * 1000, 1)})
    time.sleep(4)
    fg.finish()
    missing, stale_rows = compare()
    out.update({"lost_acknowledged_writes": lost_acknowledged(fg), "rows_missing_on_dst": missing, "rows_older_on_dst": stale_rows,
                "writes_rejected_then_retried": fg.errors["rejected"], "other_errors": fg.errors["other"]})
    print(out, flush=True)
    return out


def scenario_resume():
    setup()
    last, n1 = copy(5000, None, 0, stop_after=120000)          # the migration process dies here
    last2, n2 = copy(5000, None, last - 20000)                  # resumes from a checkpoint a little behind
    sumq = "SELECT count(*), md5(string_agg(id::text || ':' || coalesce(v::text, ''), ',' ORDER BY id)) FROM tickets WHERE tenant_id = %s"
    with psycopg.connect(DST) as d, psycopg.connect(SRC) as sc:
        rows, distinct = d.execute("SELECT count(*), count(DISTINCT id) FROM tickets WHERE tenant_id = %s", (T,)).fetchone()
        dst_sum, src_sum = d.execute(sumq, (T,)).fetchone(), sc.execute(sumq, (T,)).fetchone()
    return {"copied_before_crash": n1, "copied_after_resume": n2, "rows_on_dst": rows, "distinct": distinct,
            "same_rows_as_source": dst_sum == src_sum, "recopied_rows": n1 + n2 - rows}


if __name__ == "__main__":
    S = {"burst": scenario_speed(50000, None, "unthrottled, 4 streams × 50k-row batches", streams=4), "throttled": scenario_speed(2000, 20000, "2k-row batches, ≤ 20k rows/s")}
    for v in S.values():
        print({k: x for k, x in v.items() if k != "series"}, flush=True)
    C = {m: scenario_cutover(m) for m in ("naive", "catch-up", "stale-clients-no-fence", "stale-clients-fence")}
    RS = scenario_resume()
    print(RS, flush=True)
    data.update({"speed": S, "cutover": C, "resume": RS})
    b, t = S["burst"], S["throttled"]
    report("Copy as fast as possible", "the move competes with users: foreground p99 jumps while it runs",
           f"copy {b['copy_s']} s; foreground p99 {b['baseline_p99_ms']} ms before → {b['during_p99_ms']} ms during", b["during_p99_ms"] > 1.5 * b["baseline_p99_ms"])
    report("Throttled copy in small batches", "slower move, foreground barely notices",
           f"copy {t['copy_s']} s; p99 {t['baseline_p99_ms']} → {t['during_p99_ms']} ms (burst: {b['during_p99_ms']} ms)",
           t["during_p99_ms"] / t["baseline_p99_ms"] < 0.75 * b["during_p99_ms"] / b["baseline_p99_ms"])
    n, cu = C["naive"], C["catch-up"]
    report("Copy, then switch routing", "writes made during the copy never reach the new owner",
           f"acknowledged writes lost {n['lost_acknowledged_writes']}; rows older on dst {n['rows_older_on_dst']}",
           n["lost_acknowledged_writes"] > 50 and n["rows_older_on_dst"] == n["lost_acknowledged_writes"])
    report("Copy, catch up from a change log, fence, final catch-up, switch", "nothing lost; writes pause only for the final step",
           f"lost {cu['lost_acknowledged_writes']}; fence → switch {cu['fence_to_flip_ms']} ms; writes rejected and retried {cu['writes_rejected_then_retried']}",
           cu["lost_acknowledged_writes"] == 0 and cu["rows_older_on_dst"] == 0 and cu["fence_to_flip_ms"] < 100)
    sn, sf = C["stale-clients-no-fence"], C["stale-clients-fence"]
    report("Clients with a 2 s old routing table, old owner still accepts writes", "late writes land on the old shard and are lost",
           f"acknowledged writes lost {sn['lost_acknowledged_writes']}", sn["lost_acknowledged_writes"] > 5)
    report("Same clients, old owner rejects writes for the moved tenant", "rejected → refresh routing → retry on the new owner",
           f"lost {sf['lost_acknowledged_writes']}; rejected and retried {sf['writes_rejected_then_retried']}",
           sf["lost_acknowledged_writes"] == 0 and sf["writes_rejected_then_retried"] > 0)
    report("Migration killed and resumed from a checkpoint", "idempotent upserts: re-copied rows are harmless, and the result equals the source",
           f"{RS['rows_on_dst']:,} rows; {RS['recopied_rows']:,} copied twice; checksum of (id, v) equals the source: {RS['same_rows_as_source']}",
           RS["rows_on_dst"] == 300000 and RS["same_rows_as_source"] and RS["recopied_rows"] >= 10000)
    with open(__file__.replace("rebalance.py", "rebalance_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
