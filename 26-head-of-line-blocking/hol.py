"""Head-of-line blocking, simulated for Part 26 (discrete-event, fixed seeds, pure Python).

A. Worker pool: 4 workers share one FIFO queue. Ticket reads (10 ms, 150/s) and CSV exports (2 s, 0.75/s) mix.
B. Ordered partition: one webhook-delivery partition, 50 tenants, 100 msgs/s at 5 ms each; one tenant's endpoint
   becomes slow (2 s per call until timeout).
C. One connection, 8 responses of 120 KB: HTTP/1.1 (one at a time), HTTP/2 over TCP, HTTP/3 over QUIC, 1% packet loss.
"""
import heapq
import json
import math
import random
from collections import deque, defaultdict

results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else 0


# ---- A. worker pool ---------------------------------------------------------------------------
def pool(layout, seed=3, T=600.0):
    """layout: 'shared' (4 workers, one FIFO) | 'split' (2 for reads, 2 for exports) | 'shared8' (8 workers, one FIFO)"""
    rnd = random.Random(seed)
    jobs = []
    for cls, rate, svc in (("read", 150, 0.010), ("export", 0.75, 2.0)):
        t = 0.0
        while True:
            t += rnd.expovariate(rate)
            if t > T:
                break
            jobs.append((t, cls, rnd.lognormvariate(math.log(svc), 0.25)))
    jobs.sort()
    pools = {"shared": {"read": ("all", 4), "export": ("all", 4)},
             "split": {"read": ("fast", 2), "export": ("slow", 2)},
             "shared8": {"read": ("all", 8), "export": ("all", 8)}}[layout]
    free = {name: [0.0] * n for name, n in set(pools.values())}
    for v in free.values():
        heapq.heapify(v)
    wait, svc_t = defaultdict(list), defaultdict(list)
    for t, cls, s in jobs:                      # FIFO per pool: each job takes the earliest free worker
        name = pools[cls][0]
        f = heapq.heappop(free[name])
        start = max(t, f)
        heapq.heappush(free[name], start + s)
        wait[cls].append(start - t)
        svc_t[cls].append(s)
    return {"layout": layout,
            "read_wait_p50_ms": round(pct(wait["read"], .5) * 1000, 1), "read_wait_p99_ms": round(pct(wait["read"], .99) * 1000),
            "read_service_p99_ms": round(pct(svc_t["read"], .99) * 1000, 1),
            "read_waited_over_1s_pct": round(100 * sum(w > 1 for w in wait["read"]) / len(wait["read"]), 2),
            "export_wait_p50_s": round(pct(wait["export"], .5), 2), "export_wait_p99_s": round(pct(wait["export"], .99), 2)}


# ---- B. ordered partition ----------------------------------------------------------------------
def partition(mode, seed=4, T=120.0):
    """mode: 'serial' | 'timeout-park' (50 ms timeout, failed message moved to a retry topic) | 'per-key' (16 slots, order kept per tenant)"""
    rnd = random.Random(seed)
    msgs, t = [], 0.0
    while t < T:
        t += rnd.expovariate(100)
        tenant = 0 if rnd.random() < 0.05 else rnd.randint(1, 49)
        slow = tenant == 0 and t >= 20
        msgs.append((t, tenant, 2.0 if slow else rnd.lognormvariate(math.log(0.005), 0.3)))
    lag = defaultdict(list)
    if mode in ("serial", "timeout-park"):
        clock = 0.0
        for t, tenant, s in msgs:
            if mode == "timeout-park":
                s = min(s, 0.050)
            clock = max(clock, t) + s
            if t >= 20:
                lag["others" if tenant else "slow"].append(clock - t)
    else:
        slots = [0.0] * 16
        key_free = defaultdict(float)
        for t, tenant, s in msgs:               # a message starts when a slot is free AND the previous message of its key is done
            heapq.heapify(slots)
            f = heapq.heappop(slots)
            start = max(t, f, key_free[tenant])
            if key_free[tenant] > max(t, f):    # key busy: it waits for its own key, the slot stays free for others
                heapq.heappush(slots, f)
                done = start + s
            else:
                done = start + s
                heapq.heappush(slots, done)
            key_free[tenant] = done
            if t >= 20:
                lag["others" if tenant else "slow"].append(done - t)
    return {"mode": mode, "others_lag_p50_s": round(pct(lag["others"], .5), 3), "others_lag_p99_s": round(pct(lag["others"], .99), 3),
            "others_lag_max_s": round(max(lag["others"]), 1), "slow_lag_p99_s": round(pct(lag["slow"], .99), 1)}


# ---- C. one connection, many responses ---------------------------------------------------------
def connection(proto, seed=7, loss=0.01, streams=8, size=120_000, mss=1200, gap=0.0001, rtt=0.040, slow_first=0.0):
    """slow_first: seconds the server needs to produce response 0 (the others are ready at once)."""
    rnd = random.Random(seed)
    per = math.ceil(size / mss)
    ready = [slow_first if s == 0 else 0.0 for s in range(streams)]
    left = [per] * streams
    order, clock, cur = [], 0.0, 0
    while any(left):
        if proto == "http1":                     # one response at a time, in request order
            s = cur
            clock = max(clock, ready[s])
        else:                                    # HTTP/2 and HTTP/3: interleave packets of every response that is ready
            cand = [x for x in range(streams) if left[x] and ready[x] <= clock]
            if not cand:
                clock = min(ready[x] for x in range(streams) if left[x])
                continue
            s = cand[len(order) % len(cand)]
        order.append((s, per - left[s], clock))
        left[s] -= 1
        clock += gap
        if proto == "http1" and left[s] == 0:
            cur += 1
    arrive = []
    for s, i, sent in order:
        a = sent + rtt / 2
        if rnd.random() < loss:
            a += rtt + 0.010                     # detected ~1 RTT later, retransmitted
        arrive.append((s, i, a))
    done = defaultdict(float)
    if proto in ("http1", "http2"):              # TCP: bytes are handed to the app strictly in order across ALL streams
        hi = 0.0
        for s, i, a in arrive:
            hi = max(hi, a)
            done[s] = max(done[s], hi)
    else:                                        # QUIC: ordering only within each stream
        hi = defaultdict(float)
        for s, i, a in arrive:
            hi[s] = max(hi[s], a)
            done[s] = hi[s]
    times = sorted(done.values())
    return {"proto": proto, "first_done_ms": round(times[0] * 1000), "median_done_ms": round(times[len(times) // 2] * 1000),
            "last_done_ms": round(times[-1] * 1000), "per_stream_ms": [round(done[s] * 1000) for s in range(streams)]}


def connection_many(proto, runs=200):
    meds = [connection(proto, seed=s)["median_done_ms"] for s in range(runs)]
    firsts = [connection(proto, seed=s)["first_done_ms"] for s in range(runs)]
    return {"proto": proto, "median_of_median_ms": pct(meds, .5), "p95_of_median_ms": pct(meds, .95), "median_first_done_ms": pct(firsts, .5)}


if __name__ == "__main__":
    A = {l: pool(l) for l in ("shared", "split", "shared8")}
    for r in A.values():
        print(r)
    B = {m: partition(m) for m in ("serial", "timeout-park", "per-key")}
    for r in B.values():
        print(r)
    C = {p: connection_many(p) for p in ("http1", "http2", "http3")}
    C0 = {p: connection(p, loss=0.0) for p in ("http1", "http2", "http3")}
    for p in C:
        print(C[p], "no loss:", C0[p]["median_done_ms"], C0[p]["first_done_ms"])
    data["slow_first"] = {p: connection(p, loss=0.0, slow_first=0.5) for p in ("http1", "http2")}
    print("slow first:", {p: (v["median_done_ms"], v["per_stream_ms"]) for p, v in data["slow_first"].items()})
    data.update({"pool": A, "partition": B, "connection": C, "connection_noloss": C0, "connection_example": {p: connection(p, seed=11) for p in C}})
    s, sp, s8 = A["shared"], A["split"], A["shared8"]
    report("Shared FIFO pool: reads behind exports", "10 ms reads wait far longer than they run whenever exports fill the workers",
           f"read service P99 {s['read_service_p99_ms']} ms, read queue-wait P99 {s['read_wait_p99_ms']} ms; {s['read_waited_over_1s_pct']}% waited > 1 s",
           s["read_wait_p99_ms"] > 20 * s["read_service_p99_ms"] and s["read_waited_over_1s_pct"] > 5)
    report("Separate pools (2 for reads, 2 for exports)", "reads no longer wait behind exports; exports wait for each other",
           f"read wait P99 {sp['read_wait_p99_ms']} ms; export wait P50 {sp['export_wait_p50_s']} s (shared: {s['export_wait_p50_s']} s)",
           sp["read_wait_p99_ms"] < 100 and sp["read_wait_p99_ms"] * 10 < s["read_wait_p99_ms"]
           and sp["export_wait_p50_s"] > s["export_wait_p50_s"])
    report("Doubling workers on the shared queue", "at this export rate, enough extra workers also hide the problem — until exports outnumber them",
           f"read wait P99 with 8 shared workers: {s8['read_wait_p99_ms']} ms vs {sp['read_wait_p99_ms']} ms with 4 split and {s['read_wait_p99_ms']} ms with 4 shared",
           s8["read_wait_p99_ms"] * 10 < s["read_wait_p99_ms"] and s8["read_waited_over_1s_pct"] == 0)
    se, tp, pk = B["serial"], B["timeout-park"], B["per-key"]
    report("One slow tenant on an ordered partition", "every other tenant's webhooks wait behind it",
           f"other tenants' lag P99 {se['others_lag_p99_s']} s (max {se['others_lag_max_s']} s), growing for as long as the tenant stays slow",
           se["others_lag_p99_s"] > 30 and se["others_lag_max_s"] > 100)
    report("Short timeout + park the message on a retry topic", "the partition keeps moving",
           f"other tenants' lag P99 {tp['others_lag_p99_s']} s", tp["others_lag_p99_s"] < 1)
    report("Per-key concurrency inside the partition (order kept per tenant)", "only the slow tenant waits",
           f"other tenants' lag P99 {pk['others_lag_p99_s']} s; slow tenant's own lag P99 {pk['slow_lag_p99_s']} s",
           pk["others_lag_p99_s"] < 0.1 and pk["slow_lag_p99_s"] > 100)
    h1, h2, h3 = C["http1"], C["http2"], C["http3"]
    S1, S2 = data["slow_first"]["http1"], data["slow_first"]["http2"]
    report("First response takes 500 ms to produce: HTTP/1.1 vs HTTP/2 on one connection", "HTTP/1.1 holds the 7 ready responses behind it; HTTP/2 sends them at once",
           f"median response: HTTP/1.1 {S1['median_done_ms']} ms, HTTP/2 {S2['median_done_ms']} ms (no loss)",
           S1["median_done_ms"] > 3 * S2["median_done_ms"] and S2["median_done_ms"] < 150)
    report("HTTP/2 vs HTTP/3 under 1% packet loss", "HTTP/2 fixes HTTP-level HOL, but one lost TCP packet stalls every stream; QUIC stalls only one",
           f"median response (200 runs): HTTP/2 {h2['median_of_median_ms']} ms, HTTP/3 {h3['median_of_median_ms']} ms (no loss: {C0['http2']['median_done_ms']} ms)",
           h2["median_of_median_ms"] > h3["median_of_median_ms"] * 1.2)
    with open(__file__.replace("hol.py", "hol_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
