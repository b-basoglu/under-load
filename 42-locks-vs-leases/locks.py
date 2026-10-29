"""Locks vs leases, measured for Part 42. Redis 7.4 holds the lock; PostgreSQL 17 is the protected resource.

Job: "issue this month's invoice for tenant N" — must happen once per tenant. Workers take the lock for a tenant,
check whether an invoice exists, wait for the billing provider (~300 ms), then write the invoice.
"""
import json
import threading
import time
import uuid

import psycopg
import redis

R = redis.Redis(port=56379)
PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
results, data = [], {}

RELEASE = R.register_script("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end")


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def setup():
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS invoices, fences")
        c.execute("CREATE TABLE invoices (id bigserial PRIMARY KEY, tenant int, worker text, token bigint, at timestamptz DEFAULT clock_timestamp())")
        c.execute("CREATE TABLE fences (resource text PRIMARY KEY, max_token bigint)")
    for k in R.scan_iter("lock:*"):
        R.delete(k)
    R.delete("fence:counter")


def acquire(key, ttl_ms, wait_s=6.0):
    """Returns (owner id, fencing token) or None."""
    me = str(uuid.uuid4())
    t0 = time.time()
    while time.time() - t0 < wait_s:
        ok = R.set(key, me, nx=True, px=ttl_ms) if ttl_ms else R.set(key, me, nx=True)
        if ok:
            return me, R.incr("fence:counter")          # every grant gets a larger token
        time.sleep(0.02)
    return None


def write_invoice(c, tenant, worker, token, fenced):
    if fenced:   # the resource only accepts the newest token it has seen for this tenant
        ok = c.execute("""INSERT INTO fences VALUES (%s, %s) ON CONFLICT (resource) DO UPDATE SET max_token = excluded.max_token
                          WHERE fences.max_token <= excluded.max_token RETURNING 1""", (f"invoice:{tenant}", token)).fetchone()
        if not ok:
            return "rejected"
    exists = c.execute("SELECT 1 FROM invoices WHERE tenant = %s", (tenant,)).fetchone()
    if exists:
        return "skipped"
    c.execute("INSERT INTO invoices (tenant, worker, token) VALUES (%s, %s, %s)", (tenant, worker, token))
    return "written"


def worker(name, tenants, ttl_ms, out, pause_on=None, pause_s=0.0, crash_on=None, safe_release=True, fenced=False, renew=False, work_s=0.3):
    with psycopg.connect(PG, autocommit=True) as c:
        for t in tenants:
            key = f"lock:invoice:{t}"
            got = acquire(key, ttl_ms)
            if not got:
                out["gave_up"] += 1
                continue
            me, token = got
            stop_renew = threading.Event()
            if renew:                                          # a watchdog extends the lease while the work runs
                def keep():
                    while not stop_renew.wait(ttl_ms / 3000):
                        R.eval("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end", 1, key, me, ttl_ms)
                threading.Thread(target=keep, daemon=True).start()
            if crash_on == t:
                out["crashed"] = True
                return                                         # process dies holding the lock
            # check first: is the invoice already there?
            already = c.execute("SELECT 1 FROM invoices WHERE tenant = %s", (t,)).fetchone()
            if not already:
                time.sleep(work_s)                             # calling the billing provider
                if pause_on == t:
                    time.sleep(pause_s)                        # GC pause / VM freeze while 'holding' the lock
                res = c.execute("SELECT 1").fetchone() and None
                # write without re-checking (the check above happened under the lock — or so it believed)
                if fenced:
                    r = write_invoice(c, t, name, token, True)
                else:
                    c.execute("INSERT INTO invoices (tenant, worker, token) VALUES (%s, %s, %s)", (t, name, token))
                    r = "written"
                out[r] = out.get(r, 0) + 1
            stop_renew.set()
            if safe_release:
                RELEASE(keys=[key], args=[me])
            else:
                if R.get(key) not in (None, me.encode()):
                    out["deleted_someone_elses_lock"] = out.get("deleted_someone_elses_lock", 0) + 1
                R.delete(key)                                  # deletes whatever lock is there — maybe someone else's


def run(ttl_ms, tenants=20, **kw):
    setup()
    outs = [{"gave_up": 0}, {"gave_up": 0}, {"gave_up": 0}]
    names = ("worker-a", "worker-b", "worker-c")
    per = {n: {} for n in names}
    extra = kw.pop("extra", {})
    ts = []
    for i, n in enumerate(names):
        args = dict(kw)
        args.update(extra.get(n, {}))
        ts.append(threading.Thread(target=worker, args=(n, list(range(tenants)), ttl_ms, outs[i]), kwargs=args))
    t0 = time.time()
    for i, t in enumerate(ts):
        t.start()
        time.sleep(0.05)
    for t in ts:
        t.join()
    with psycopg.connect(PG) as c:
        per_tenant = dict(c.execute("SELECT tenant, count(*) FROM invoices GROUP BY tenant").fetchall())
    dup = sum(1 for v in per_tenant.values() if v > 1)
    missing = sum(1 for t in range(tenants) if t not in per_tenant)
    return {"ttl_ms": ttl_ms, "duplicated_invoices": dup, "missing_invoices": missing, "rejected_by_fence": sum(o.get("rejected", 0) for o in outs),
            "gave_up": sum(o["gave_up"] for o in outs), "deleted_foreign_locks": sum(o.get("deleted_someone_elses_lock", 0) for o in outs), "seconds": round(time.time() - t0, 1)}


if __name__ == "__main__":
    A = run(0, extra={"worker-a": {"crash_on": 3}})                                       # lock without expiry, holder crashes
    B = run(2000, extra={"worker-a": {"crash_on": 3}})                                    # lease, holder crashes
    slow = {"worker-b": {"work_s": 1.5}, "worker-c": {"work_s": 1.5}}                      # the billing provider is slow today
    C = run(2000, extra={"worker-a": {"pause_on": 5, "pause_s": 3.0}, **slow})            # lease, holder pauses longer than TTL
    D = run(2000, safe_release=False, extra={"worker-a": {"pause_on": 5, "pause_s": 3.0}, **slow})  # + unconditional release
    E = run(2000, fenced=True, extra={"worker-a": {"pause_on": 5, "pause_s": 3.0}})       # fencing tokens checked by PostgreSQL
    F = run(2000, renew=True, extra={"worker-a": {"pause_on": 5, "pause_s": 3.0}})        # renewal watchdog (thread keeps running during the pause)
    for k, v in zip("ABCDEF", (A, B, C, D, E, F)):
        print(k, v, flush=True)
    data.update({"no_ttl_crash": A, "lease_crash": B, "lease_pause": C, "unsafe_release": D, "fenced": E, "renewed": F})
    report("Lock without expiry, holder crashes", "the lock is held forever; that tenant's invoice is never issued",
           f"missing invoices {A['missing_invoices']}; other workers gave up waiting {A['gave_up']} time(s)",
           A["missing_invoices"] >= 1 and A["gave_up"] >= 1 and A["duplicated_invoices"] == 0)
    report("Lease (TTL 2 s), holder crashes", "the lease expires and another worker finishes the job", f"missing {B['missing_invoices']}, duplicated {B['duplicated_invoices']}",
           B["missing_invoices"] == 0 and B["duplicated_invoices"] == 0)
    report("Lease, holder pauses 3 s (longer than the TTL)", "another worker gets the lease; the paused one wakes up and writes too",
           f"duplicated invoices {C['duplicated_invoices']}", C["duplicated_invoices"] >= 1 and C["missing_invoices"] == 0)
    report("Releasing with a plain DEL after the pause", "the paused worker deletes the lock that now belongs to someone else",
           f"locks of other workers deleted: {D['deleted_foreign_locks']} (compare-and-delete: {C['deleted_foreign_locks']})",
           D["deleted_foreign_locks"] >= 1 and C["deleted_foreign_locks"] == 0)
    report("Fencing tokens checked by the database", "the paused worker's write carries an old token and is rejected",
           f"duplicated {E['duplicated_invoices']}, rejected {E['rejected_by_fence']}",
           E["duplicated_invoices"] == 0 and E["rejected_by_fence"] >= 1 and E["missing_invoices"] == 0)
    report("Lease renewed by a watchdog thread", "slow work keeps its lease while the watchdog runs (a stop-the-world pause would freeze the watchdog too)",
           f"duplicated {F['duplicated_invoices']}", F["duplicated_invoices"] == 0 and F["missing_invoices"] == 0)
    with open(__file__.replace("locks.py", "locks_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
