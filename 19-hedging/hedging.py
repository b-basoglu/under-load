"""Request hedging, simulated for Part 19 (discrete-event, fixed seeds, pure Python).

Model:
- 3 read replicas, each a FIFO server. Service time: lognormal, median 20 ms.
- Each replica independently pauses ~0.1 times per second for 300 ms (about 3% of the time) (GC, noisy neighbour, compaction):
  requests queued or running on it during a pause wait it out.
- Requests arrive at random; each goes to a random replica.
- Hedge: if no answer after `delay`, send one copy to a different replica; the first answer wins and the
  other copy is removed if it hasn't started yet (cancellation), otherwise it runs to completion (wasted work).
"""
import bisect
import heapq
import json
import math
import random
from collections import deque

results, data = [], {}
SIM_S = 120.0


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    s = sorted(xs)
    return s[min(len(s) - 1, int(math.ceil(p / 100 * len(s))) - 1)]


def make_pauses(rnd, rate=0.1, length=0.3):
    t, out = 0.0, []
    while t < SIM_S + 5:
        t += rnd.expovariate(rate)
        out.append((t, t + length))
    return out


def finish(pauses, starts, start, svc):
    """Finish time of `svc` seconds of work starting at `start`, stretched by any pauses."""
    t, left = start, svc
    i = max(0, bisect.bisect_right(starts, t) - 1)
    while True:
        while i < len(pauses) and pauses[i][1] <= t:
            i += 1
        if i < len(pauses) and pauses[i][0] <= t < pauses[i][1]:
            t = pauses[i][1]; i += 1; continue
        nxt = pauses[i][0] if i < len(pauses) else float("inf")
        if t + left <= nxt:
            return t + left
        left -= nxt - t
        t = pauses[i][1]; i += 1


def simulate(rate, policy, delay=None, budget=None, seed=11, n_rep=3):
    rnd = random.Random(seed)
    pauses = [make_pauses(random.Random(seed * 100 + r)) for r in range(n_rep)]
    pstarts = [[a for a, _ in p] for p in pauses]
    queues = [deque() for _ in range(n_rep)]
    busy = [None] * n_rep                       # (req, copy) currently running
    ev = []
    seq = 0

    def push(t, kind, *a):
        nonlocal seq
        seq += 1
        heapq.heappush(ev, (t, seq, kind, a))

    t = 0.0
    nreq = 0
    while t < SIM_S:
        t += rnd.expovariate(rate)
        push(t, "arrive", nreq); nreq += 1
    req_start, req_done, copies, primary = {}, {}, {}, {}
    sent = wasted = hedges = hedge_wins = 0
    tokens = 0.0

    def start_next(r, now):
        while queues[r]:
            q, c = queues[r].popleft()
            if q in req_done:            # cancelled: the other copy already answered
                continue
            busy[r] = (q, c)
            svc = rnd.lognormvariate(math.log(0.020), 0.3)
            push(finish(pauses[r], pstarts[r], now, svc), "done", r)
            return
        busy[r] = None

    def send(q, r, now, copy):
        nonlocal sent
        sent += 1
        queues[r].append((q, copy))
        copies.setdefault(q, []).append(r)
        if busy[r] is None:
            start_next(r, now)

    while ev:
        now, _, kind, a = heapq.heappop(ev)
        if kind == "arrive":
            q = a[0]
            req_start[q] = now
            r = rnd.randrange(n_rep)
            primary[q] = r
            send(q, r, now, 0)
            tokens = min(tokens + (budget or 0), 50)
            if policy == "immediate":
                send(q, (r + 1 + rnd.randrange(n_rep - 1)) % n_rep, now, 1); hedges += 1
            elif policy in ("delayed", "delayed+budget"):
                push(now + delay, "hedge", q)
        elif kind == "hedge":
            q = a[0]
            if q in req_done:
                continue
            if policy == "delayed+budget":
                if tokens < 1:
                    continue
                tokens -= 1
            hedges += 1
            send(q, (primary[q] + 1 + rnd.randrange(n_rep - 1)) % n_rep, now, 1)
        elif kind == "done":
            r = a[0]
            q, c = busy[r]
            if q in req_done:
                wasted += 1
            else:
                req_done[q] = now
                if c == 1:
                    hedge_wins += 1
            start_next(r, now)
    lat = [(req_done[q] - req_start[q]) * 1000 for q in req_done if req_start[q] > 5]
    if policy == "none":
        simulate.last_p95 = pct(lat, 95)
    return {"rate": rate, "policy": policy, "delay_ms": None if delay is None else round(delay * 1000),
            "p50": round(pct(lat, 50), 1), "p99": round(pct(lat, 99), 1), "p999": round(pct(lat, 99.9), 1),
            "extra_load_pct": round(100 * (sent - nreq) / nreq, 1), "wasted_work_pct": round(100 * wasted / nreq, 1),
            "hedges_pct": round(100 * hedges / nreq, 1), "hedge_win_pct": round(100 * hedge_wins / max(hedges, 1), 1)}


if __name__ == "__main__":
    data["runs"] = []
    for rate, label in ((60, "moderate load"), (115, "high load")):
        none = simulate(rate, "none")
        delay_s = simulate.last_p95 / 1000            # hedge after the measured P95
        runs = [none, simulate(rate, "immediate"), simulate(rate, "delayed", delay=delay_s),
                simulate(rate, "delayed+budget", delay=delay_s, budget=0.05)]
        for r in runs:
            r["load"] = label
            data["runs"].append(r)
            print(f"{label:14s} {r['policy']:15s} delay {r['delay_ms']}  p50 {r['p50']:6.1f}  p99 {r['p99']:7.1f}  p999 {r['p999']:7.1f}  "
                  f"extra {r['extra_load_pct']:5.1f}%  wasted {r['wasted_work_pct']:5.1f}%  wins {r['hedge_win_pct']}%")
    data["aggressive"] = []
    for pol, bud in (("delayed", None), ("delayed+budget", 0.05)):
        r = simulate(115, pol, delay=0.030, budget=bud)       # hedge after a fixed 30 ms — below the high-load median
        r["load"] = "high load, 30 ms delay"; data["aggressive"].append(r)
        print(f"high, 30 ms     {pol:15s} p50 {r['p50']:8.1f}  p99 {r['p99']:8.1f}  extra {r['extra_load_pct']:5.1f}%")
    A = {r["policy"]: r for r in data["aggressive"]}
    report("High load: an aggressive fixed 30 ms hedge delay", "most requests get hedged and the extra load collapses latency; a budget prevents it",
           f"no budget: +{A['delayed']['extra_load_pct']}% load, P99 {A['delayed']['p99']:,} ms; 5% budget: +{A['delayed+budget']['extra_load_pct']}%, "
           f"P99 {A['delayed+budget']['p99']:,} ms",
           A["delayed"]["extra_load_pct"] > 90 and A["delayed"]["p99"] > 2 * A["delayed+budget"]["p99"]
           and A["delayed+budget"]["extra_load_pct"] <= 6)
    R = {(r["load"], r["policy"]): r for r in data["runs"]}
    m = lambda p: R[("moderate load", p)]
    h = lambda p: R[("high load", p)]
    report("Moderate load: hedge after the P95 vs no hedging", "P99 drops sharply for a few percent extra load",
           f"P99 {m('none')['p99']} → {m('delayed')['p99']} ms; P99.9 {m('none')['p999']} → {m('delayed')['p999']} ms; "
           f"extra load {m('delayed')['extra_load_pct']}% (hedge delay {m('delayed')['delay_ms']} ms)",
           m("delayed")["p99"] < m("none")["p99"] / 2 and m("delayed")["extra_load_pct"] < 10
           and m("delayed")["p999"] < m("none")["p999"])
    report("Hedging every request immediately", "doubles the load; at high load it makes the tail worse",
           f"extra load {m('immediate')['extra_load_pct']}%; high load P99 {h('none')['p99']} (none) vs {h('immediate')['p99']} ms (immediate)",
           m("immediate")["extra_load_pct"] > 90 and h("immediate")["p99"] > h("none")["p99"]
           and h("immediate")["extra_load_pct"] > 90)
    report("High load: P95 hedge, with and without a 5% budget", "unbudgeted hedging adds load when it's least affordable; the budget caps it",
           f"none P99 {h('none')['p99']} ms; P95 hedge P99 {h('delayed')['p99']} ms at +{h('delayed')['extra_load_pct']}% load; "
           f"with budget P99 {h('delayed+budget')['p99']} ms at +{h('delayed+budget')['extra_load_pct']}%",
           h("delayed+budget")["extra_load_pct"] <= 6 and h("delayed+budget")["p99"] < h("none")["p99"]
           and h("delayed")["extra_load_pct"] > h("delayed+budget")["extra_load_pct"])
    with open(__file__.replace("hedging.py", "hedging_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
