"""Graceful degradation, measured for Part 20 (real threads, simulated dependencies, wall-clock time).

The ticket page is built from:
  core:     ticket + comments (database)            ~15 ms   — required
  widgets:  related tickets (search cluster)        ~25 ms   — optional
            customer profile (CRM)                  ~30 ms   — optional, can be served stale
            SLA timer (rules service)               ~10 ms   — optional
From t = 5 s to t = 15 s the search cluster hangs (calls wait for their timeout).
40 page loads/s served by a pool of 40 threads, 20 s per scenario. Each page is counted in the second it finished, so a
page that waited out the outage in a queue counts as late, not as served on time. In the degrade modes the search client
keeps its own 2 s timeout (the page stops waiting after 150 ms, but the call still holds a widget thread), and the core
has its own small pool.
"""
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION

RATE, WORKERS, DURATION = 40, 40, 20.0
DOWN = (5.0, 15.0)
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class Search:
    def __init__(self, t0):
        self.t0 = t0

    def __call__(self, timeout):
        if DOWN[0] <= time.time() - self.t0 < DOWN[1]:
            time.sleep(timeout)
            raise TimeoutError("search")
        time.sleep(0.025)
        return ["#48190", "#47021"]


def core():
    time.sleep(0.015)
    return {"id": 48213, "subject": "Refund for order 1182"}


def crm():
    time.sleep(0.03)
    return {"plan": "Enterprise", "csat": 4.6}


def sla():
    time.sleep(0.01)
    return "2 h 14 min left"


def run(mode):
    t0 = time.time()
    search = Search(t0)
    widget_pool = ThreadPoolExecutor(60)
    core_pool = ThreadPoolExecutor(20) if mode != "all-or-nothing" else widget_pool   # degrade modes: the core has its own pool
    lock = threading.Lock()
    per_s = [{"ok": 0, "full": 0, "failed": 0, "lat": []} for _ in range(int(DURATION) + 60)]   # bucketed by when the page finished
    flag = {"related_enabled": True}
    pages = []

    def page(arrived):
        try:
            if mode == "all-or-nothing":
                # every part is required; a 2 s timeout on each call; any failure fails the page
                parts = [widget_pool.submit(core), widget_pool.submit(search, 2.0), widget_pool.submit(crm), widget_pool.submit(sla)]
                for f in parts:
                    f.result()
                full = True
            else:
                deadline = 0.15                      # optional widgets get 150 ms in total
                fut_core = core_pool.submit(core)
                opt = {"crm": widget_pool.submit(crm), "sla": widget_pool.submit(sla)}
                use_search = mode != "degrade+kill-switch" or flag["related_enabled"]
                if use_search:
                    opt["related"] = widget_pool.submit(search, 2.0)   # the client keeps its own 2 s timeout; the page waits only 150 ms
                fut_core.result()                    # core must succeed
                done, _ = wait(list(opt.values()), timeout=deadline)
                full = all(f in done and f.exception() is None for f in opt.values()) and use_search
            s = int(time.time() - t0)
            with lock:
                pages.append((arrived - t0, time.time() - arrived, True, full))
                per_s[s]["ok"] += 1
                per_s[s]["full"] += 1 if full else 0
                per_s[s]["lat"].append(time.time() - arrived)
        except Exception:
            s = int(time.time() - t0)
            with lock:
                pages.append((arrived - t0, time.time() - arrived, False, False))
                per_s[s]["failed"] += 1
                per_s[s]["lat"].append(time.time() - arrived)

    def kill_switch_watcher():
        # an operator (or an automatic rule) turns off "related tickets" 3 s into the outage, on again after it ends
        while time.time() - t0 < DURATION:
            el = time.time() - t0
            flag["related_enabled"] = not (DOWN[0] + 3 <= el < DOWN[1] + 1)
            time.sleep(0.05)
    if mode == "degrade+kill-switch":
        threading.Thread(target=kill_switch_watcher, daemon=True).start()

    with ThreadPoolExecutor(WORKERS) as ex:
        i = 0
        while True:
            at = t0 + i / RATE
            if at - t0 >= DURATION:
                break
            d = at - time.time()
            if d > 0:
                time.sleep(d)
            ex.submit(page, at); i += 1
    widget_pool.shutdown(wait=True)
    core_pool.shutdown(wait=True)
    out = []
    for s in range(int(DURATION) + 10):
        p = per_s[s]
        lat = sorted(p["lat"])
        n = p["ok"] + p["failed"]
        out.append({"s": s, "served_pct": round(100 * p["ok"] / n, 1) if n else None,
                    "full_pct": round(100 * p["full"] / n, 1) if n else None,
                    "p99_ms": round(lat[int(len(lat) * .99) - 1] * 1000) if lat else None,
                    "p50_ms": round(lat[len(lat) // 2] * 1000) if lat else None})
    return {"per_second": out, "pages": pages}


def window(run_, lo=DOWN[0], hi=DOWN[1]):
    """Pages that arrived while search was down: served (core page shown) within 1 s, complete, latency."""
    sel = [p for p in run_["pages"] if lo <= p[0] < hi]
    lat = sorted(p[1] for p in sel)
    ok_fast = sum(1 for p in sel if p[2] and p[1] <= 1.0)
    return {"pages": len(sel), "served": round(100 * ok_fast / len(sel), 1), "served_eventually": round(100 * sum(p[2] for p in sel) / len(sel), 1),
            "full": round(100 * sum(p[3] for p in sel) / len(sel), 1), "p50_median": round(1000 * lat[len(lat) // 2]),
            "p99_worst": round(1000 * lat[int(len(lat) * .99) - 1])}


if __name__ == "__main__":
    for mode in ("all-or-nothing", "degrade", "degrade+kill-switch"):
        r = run(mode)
        data[mode] = r["per_second"]
        w = window(r)
        data[mode + " outage"] = w
        print(f"{mode:22s} during outage: served {w['served']}%  full {w['full']}%  p50 {w['p50_median']} ms  worst p99 {w['p99_worst']} ms", flush=True)
    a, d, k = (data[m + " outage"] for m in ("all-or-nothing", "degrade", "degrade+kill-switch"))
    report("Search hangs; the page needs every widget (2 s timeouts)",
           "pages fail, and the ones that succeed wait seconds behind a backlog of stuck widget calls",
           f"of {a['pages']} pages requested during the outage: {a['served']}% shown within 1 s, {a['served_eventually']}% eventually; median {a['p50_median']:,} ms; p99 {a['p99_worst']:,} ms",
           a["served"] < 60 and a["p50_median"] > 2000 and a["p99_worst"] > 4000)
    report("Same outage; optional widgets have a 150 ms deadline and a placeholder", "the core page stays up; only the related-tickets box is missing",
           f"shown within 1 s {d['served']}%, complete {d['full']}%; median {d['p50_median']} ms, p99 {d['p99_worst']} ms",
           d["served"] > 99 and d["p99_worst"] < 300 and d["full"] < 20 and d["p50_median"] < 200)
    report("Plus a kill switch that turns the widget off", "once off, pages don't even wait the 150 ms",
           f"median {k['p50_median']} ms (vs {d['p50_median']} ms without the switch); complete {k['full']}% vs {d['full']}%",
           k["p50_median"] < d["p50_median"] * 0.5 and k["served"] > 99 and k["full"] <= d["full"])
    with open(__file__.replace("degradation.py", "degradation_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
