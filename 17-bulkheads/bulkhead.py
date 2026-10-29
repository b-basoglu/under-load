"""Bulkheads, measured for Part 17 (real threads, simulated work, wall-clock time).

One API process with 20 worker threads serves:
- ticket page: 100 requests/s, 30 ms each
- CSV export: 5 requests/s between t = 5 s and t = 15 s, 4 s each (a big customer exporting)
20 s per scenario.
"""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

WORKERS, DURATION = 20, 20.0
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def run(export_limit=None, ticket_rate=100, export_rate=5.0, export_window=(5, 15)):
    t0 = time.time()
    sem = threading.BoundedSemaphore(export_limit) if export_limit else None
    lock = threading.Lock()
    page = [[] for _ in range(int(DURATION) + 6)]
    ex_stats = {"offered": 0, "done": 0, "rejected": 0}
    busy = {"export": 0, "ticket": 0, "peak_export": 0}

    def ticket(arrived):
        with lock:
            busy["ticket"] += 1
        time.sleep(0.03)
        with lock:
            busy["ticket"] -= 1
            page[int(arrived - t0)].append(time.time() - arrived)

    def export(arrived):
        if sem and not sem.acquire(blocking=False):
            with lock:
                ex_stats["rejected"] += 1        # 429: "export capacity full, try again shortly"
            return
        try:
            with lock:
                busy["export"] += 1; busy["peak_export"] = max(busy["peak_export"], busy["export"])
            time.sleep(4.0)
            with lock:
                ex_stats["done"] += 1
        finally:
            with lock:
                busy["export"] -= 1
            if sem:
                sem.release()

    events = []
    for i in range(int(DURATION * ticket_rate)):
        events.append((i / ticket_rate, ticket))
    t = export_window[0]
    while t < export_window[1]:
        events.append((t, export)); t += 1 / export_rate
    events.sort(key=lambda e: e[0])
    with ThreadPoolExecutor(WORKERS) as ex:
        for at, fn in events:
            d = t0 + at - time.time()
            if d > 0:
                time.sleep(d)
            if fn is export:
                ex_stats["offered"] += 1
            ex.submit(fn, t0 + at)
    series = []
    for s in range(int(DURATION)):
        lat = sorted(page[s])
        series.append({"s": s, "p99_ms": round(lat[int(len(lat) * .99) - 1] * 1000) if lat else None,
                       "under_200ms": sum(1 for x in lat if x < 0.2), "n": len(lat)})
    return {"series": series, "exports": ex_stats, "peak_concurrent_exports": busy["peak_export"]}


def window(r, lo=5, hi=15):
    return {"p99_worst_ms": max(x["p99_ms"] for x in r["series"][lo:hi]),
            "fast_pct": round(100 * sum(x["under_200ms"] for x in r["series"][lo:hi]) / sum(x["n"] for x in r["series"][lo:hi]), 1)}


if __name__ == "__main__":
    shared = run(None)
    bulk = run(export_limit=4)
    tiny = run(export_limit=1, ticket_rate=10, export_rate=1.0)      # quiet day: only 10 page req/s, 1 export/s
    roomy = run(None, ticket_rate=10, export_rate=1.0)
    data.update(shared=shared, bulkhead=bulk, tiny_pool_quiet=tiny, shared_quiet=roomy)
    ws, wb = window(shared), window(bulk)
    report("Exports and the ticket page share 20 workers",
           "exports take every worker; ticket page requests queue behind them",
           f"ticket page during exports: worst p99 {ws['p99_worst_ms']:,} ms, {ws['fast_pct']}% under 200 ms; "
           f"peak concurrent exports {shared['peak_concurrent_exports']}",
           ws["p99_worst_ms"] > 1000 and ws["fast_pct"] < 60 and shared["peak_concurrent_exports"] >= 15)
    report("Bulkhead: exports may use at most 4 workers, reject when full",
           "the ticket page keeps 16 workers and stays fast; excess exports are rejected immediately",
           f"ticket page: worst p99 {wb['p99_worst_ms']} ms, {wb['fast_pct']}% under 200 ms; exports done {bulk['exports']['done']} / "
           f"rejected {bulk['exports']['rejected']} of {bulk['exports']['offered']}; peak {bulk['peak_concurrent_exports']}",
           wb["p99_worst_ms"] < 200 and wb["fast_pct"] >= 95
           and bulk["exports"]["rejected"] >= 20 and bulk["peak_concurrent_exports"] == 4)
    report("A bulkhead that's too small, on a quiet day",
           "exports are rejected even though most workers are idle",
           f"limit 1: exports done {tiny['exports']['done']}, rejected {tiny['exports']['rejected']} of {tiny['exports']['offered']}; "
           f"shared pool: done {roomy['exports']['done']} of {roomy['exports']['offered']}",
           tiny["exports"]["rejected"] >= 5 and tiny["peak_concurrent_exports"] == 1
           and roomy["exports"]["rejected"] == 0 and roomy["exports"]["done"] == roomy["exports"]["offered"])
    with open(__file__.replace("bulkhead.py", "bulkhead_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
