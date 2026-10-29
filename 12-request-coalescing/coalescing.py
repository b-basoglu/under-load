"""Request coalescing (single-flight) measured for Part 12, against PostgreSQL 17 limited to 2 CPUs."""
import json
import threading
import time
from concurrent.futures import Future

from psycopg_pool import ConnectionPool

PG = "host=localhost port=55432 user=postgres password=test"
POOL = ConnectionPool(PG, min_size=120, max_size=120, open=False)
results, data = [], {}
DASH = """SELECT t.priority, count(*), count(DISTINCT c.ticket_id) FROM comments c JOIN tickets t ON t.id = c.ticket_id
          WHERE t.workspace_id <> %s GROUP BY t.priority"""
calls = {"n": 0}
clock = threading.Lock()


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def backend(ws=-1, fail=False, slow=0.0):
    with clock:
        calls["n"] += 1
    if slow:
        time.sleep(slow)
    if fail:
        time.sleep(0.2)
        raise RuntimeError("database timeout")
    with POOL.connection() as k:
        return k.execute(DASH, (ws,)).fetchall()


class SingleFlight:
    """One in-flight call per key; concurrent callers wait on the same Future."""

    def __init__(self):
        self.lock = threading.Lock()
        self.inflight = {}
        self.stats = {"leaders": 0, "followers": 0}

    def do(self, key, fn, timeout=None):
        with self.lock:
            fut = self.inflight.get(key)
            leader = fut is None
            if leader:
                fut = Future(); self.inflight[key] = fut; self.stats["leaders"] += 1
            else:
                self.stats["followers"] += 1
        if leader:
            try:
                fut.set_result(fn())
            except Exception as e:
                fut.set_exception(e)
            finally:
                with self.lock:
                    del self.inflight[key]   # remove when done, so the next miss starts fresh work
        return fut.result(timeout=timeout)


def burst(n, fn):
    start = threading.Barrier(n)
    out = {"lat": [], "errors": 0, "values": []}
    lk = threading.Lock()

    def w(i):
        start.wait()
        t0 = time.perf_counter()
        try:
            v = fn(i)
            with lk:
                out["values"].append(v)
        except Exception as e:
            with lk:
                out["errors"] += 1; out.setdefault("error_types", set()).add(type(e).__name__)
        out["lat"].append(time.perf_counter() - t0)
    ts = [threading.Thread(target=w, args=(i,)) for i in range(n)]
    for t in ts: t.start()
    for t in ts: t.join()
    out["lat"].sort()
    out["max_ms"] = round(out["lat"][-1] * 1000)
    out["median_ms"] = round(out["lat"][len(out["lat"]) // 2] * 1000)
    return out


def basic():
    calls["n"] = 0
    single = burst(1, lambda i: backend())
    one_ms = single["max_ms"]
    calls["n"] = 0
    none = burst(100, lambda i: backend())
    n_none = calls["n"]
    sf = SingleFlight(); calls["n"] = 0
    with_sf = burst(100, lambda i: sf.do("dash", backend))
    data["basic"] = {"one_call_ms": one_ms, "without": {"backend_calls": n_none, "median_ms": none["median_ms"], "max_ms": none["max_ms"]},
                     "with": {"backend_calls": calls["n"], "median_ms": with_sf["median_ms"], "max_ms": with_sf["max_ms"],
                              "followers": sf.stats["followers"]}}
    report(
        "100 concurrent requests for the same dashboard, one server",
        "without coalescing: 100 backend calls; with single-flight: 1, shared by 99 followers",
        f"one call alone: {one_ms} ms · without: {n_none} calls, max {none['max_ms']} ms · with: {calls['n']} call, "
        f"max {with_sf['max_ms']} ms, {sf.stats['followers']} followers",
        n_none == 100
        and calls["n"] == 1
        and sf.stats["followers"] == 99
        and with_sf["max_ms"] < none["max_ms"] / 10,
    )


def multi_node():
    servers = [SingleFlight() for _ in range(4)]
    calls["n"] = 0
    burst(100, lambda i: servers[i % 4].do("dash", backend))
    data["multi_node"] = {"servers": 4, "backend_calls": calls["n"]}
    report("Same burst spread over 4 servers, each with its own single-flight",
           "process-local coalescing gives one call per server, not one overall",
           f"backend calls: {calls['n']}", calls["n"] == 4)


def failure():
    sf = SingleFlight(); calls["n"] = 0
    r = burst(100, lambda i: sf.do("dash", lambda: backend(fail=True)))
    stale = [("normal", 0, 0)]
    sf2 = SingleFlight()

    def with_stale(i):
        try:
            return sf2.do("dash", lambda: backend(fail=True))
        except Exception:
            return stale  # serve the last good value
    r2 = burst(100, with_stale)
    data["failure"] = {"backend_calls": calls["n"], "callers_failed": r["errors"], "with_stale_fallback_failed": r2["errors"]}
    report(
        "The leader's call fails",
        "every waiting caller gets the same failure; a stale fallback absorbs it",
        f"backend calls: {calls['n']} across both bursts; failed callers: {r['errors']} of 100; with stale fallback: {r2['errors']} failed",
        calls["n"] == 2 and r["errors"] == 100 and r2["errors"] == 0,
    )


def slow_leader():
    sf = SingleFlight(); calls["n"] = 0
    r = burst(50, lambda i: sf.do("dash", lambda: backend(slow=3.0)))
    sf2 = SingleFlight()
    stale = [("normal", 0, 0)]

    def bounded(i):
        try:
            return sf2.do("dash", lambda: backend(slow=3.0), timeout=0.5)
        except Exception:
            return stale
    r2 = burst(50, bounded)
    data["slow_leader"] = {"unbounded_median_ms": r["median_ms"], "bounded_median_ms": r2["median_ms"], "bounded_max_ms": r2["max_ms"]}
    report(
        "A slow leader (3 s) with and without a waiter deadline",
        "unbounded, everyone waits for the slowest call; with a 500 ms deadline followers fall back while the leader finishes",
        f"median wait: unbounded {r['median_ms']} ms; bounded {r2['median_ms']} ms (bounded max {r2['max_ms']} ms includes the leader)",
        r["median_ms"] > 2900 and r2["median_ms"] < 700 and r2["median_ms"] < r["median_ms"] / 3,
    )


def bad_key():
    sf = SingleFlight()
    users = [("agent", "en"), ("admin", "en"), ("agent", "tr")]

    def render(role, lang):
        time.sleep(0.2)
        return f"dashboard for {role} in {lang}" + (" + billing" if role == "admin" else "")

    def bad(i):
        role, lang = users[i % 3]
        return (role, lang, sf.do("dash", lambda: render(role, lang)))          # key ignores role and language

    def good(i):
        role, lang = users[i % 3]
        return (role, lang, sf.do(f"dash:{role}:{lang}", lambda: render(role, lang)))
    rb = burst(30, bad)
    wrong = sum(1 for role, lang, v in rb["values"] if v != f"dashboard for {role} in {lang}" + (" + billing" if role == "admin" else ""))
    leaked = sum(1 for role, lang, v in rb["values"] if role != "admin" and "billing" in v)
    rg = burst(30, good)
    wrong_g = sum(1 for role, lang, v in rg["values"] if v != f"dashboard for {role} in {lang}" + (" + billing" if role == "admin" else ""))
    data["bad_key"] = {"wrong_with_bad_key": wrong, "non_admins_shown_billing": leaked, "wrong_with_full_key": wrong_g, "requests": 30}
    report(
        "Coalescing key that ignores role and language",
        "callers get someone else's result — possibly data they may not see",
        f"wrong results: {wrong}/30 (non-admins shown admin billing: {leaked}); with the full key: {wrong_g}/30",
        wrong >= 10 and wrong_g == 0,
    )


def coalescing_vs_cache():
    sf = SingleFlight(); calls["n"] = 0
    for _ in range(20):            # sequential requests: never overlap in time
        sf.do("dash", backend)
    data["sequential"] = {"requests": 20, "backend_calls": calls["n"]}
    report("20 requests that don't overlap in time", "single-flight saves nothing — it only shares work that is in flight right now",
           f"backend calls: {calls['n']} of 20", calls["n"] == 20)


if __name__ == "__main__":
    POOL.open(); POOL.wait()
    basic()
    multi_node()
    failure()
    slow_leader()
    bad_key()
    coalescing_vs_cache()
    POOL.close()
    with open(__file__.replace("coalescing.py", "coalescing_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
