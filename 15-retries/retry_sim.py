"""Retry policies during a dependency outage — a discrete-event simulation for Part 15, plus a real layered-retry count.

Model (deliberately simple, every parameter below):
- New email-send operations arrive at 400/s (Poisson), from the ticket service's workers.
- The email provider can complete 600 requests/s. Above that it is overloaded and fails the excess.
  While overloaded (more than 110% of capacity over the last second) it degrades: capacity halves until 2 s after the overload ends
  (a common real-world behavior: queues and timeouts make an overloaded service do less useful work).
- The provider is fully down from t = 10 s to t = 20 s.
- Simulated for 90 s in 10 ms steps. Same random seed for every policy.
"""
import json
import math
import random

results = []


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)

TICK = 0.01
DURATION = 90.0
ARRIVAL = 400.0
CAPACITY = 600.0
OUTAGE = (10.0, 20.0)


def simulate(policy, seed=7, degrade=True, factor=0.5, attempts=6):
    rnd = random.Random(seed)
    ticks = int(DURATION / TICK)
    pending = [[] for _ in range(ticks + 1)]        # attempts scheduled per tick: list of (op_start, attempt_no)
    offered = [0] * int(DURATION)                   # attempts per second
    succeeded = [0] * int(DURATION)
    attempts_total = ok_ops = failed_ops = 0
    degraded_until = 0.0
    budget_tokens = 0.0
    lat = []
    window = [0] * 100     # attempts in the last 100 ticks (1 s)
    for t in range(ticks):
        now = t * TICK
        # new operations
        n_new = sum(1 for _ in range(int(ARRIVAL * TICK * 3)) if rnd.random() < 1 / 3)
        work = [(now, 0)] * n_new + pending[t]
        pending[t] = []
        attempts_total += len(work)
        offered[int(now)] += len(work)
        # provider
        down = OUTAGE[0] <= now < OUTAGE[1]
        window[t % 100] = len(work)
        if degrade and sum(window) > CAPACITY * 1.1:          # sustained overload over the last second
            degraded_until = now + 2.0
        cap_per_s = CAPACITY * (factor if now < degraded_until else 1.0)
        cap = cap_per_s * TICK
        rnd.shuffle(work)
        served = 0 if down else int(cap) + (1 if rnd.random() < cap - int(cap) else 0)
        ok, failed = work[:served], work[served:]
        for start, _ in ok:
            ok_ops += 1; succeeded[int(now)] += 1; lat.append(now - start)
        # retry budget: retries allowed up to 10% of recent new requests
        budget_tokens = min(budget_tokens + n_new * 0.1, 200)
        for start, n in failed:
            nxt = None
            if policy == "no retries":
                nxt = None
            elif policy == "immediate retry":
                nxt = TICK if n < 50 else None
            elif policy == "fixed 1 s delay":
                nxt = 1.0 if n < attempts else None
            elif policy == "exponential backoff":
                nxt = min(0.1 * 2 ** n, 10.0) if n < attempts else None
            elif policy in ("exponential + full jitter", "jitter + retry budget"):
                nxt = rnd.uniform(0, min(0.1 * 2 ** n, 10.0)) if n < attempts else None
                if policy == "jitter + retry budget" and nxt is not None:
                    if budget_tokens >= 1:
                        budget_tokens -= 1
                    else:
                        nxt = None
            if nxt is None:
                failed_ops += 1
            else:
                k = t + max(1, int(round(nxt / TICK)))
                if k <= ticks:
                    pending[k].append((start, n + 1))
    # recovery: first second after the outage where successes are >= 90% of arrivals and load is back near normal, for 5 s
    recovered_at = None
    for s in range(int(OUTAGE[1]), int(DURATION) - 5):
        if all(succeeded[x] >= 0.9 * ARRIVAL for x in range(s, s + 5)) and all(offered[x] <= 1.3 * ARRIVAL for x in range(s, s + 5)):
            recovered_at = s; break
    lat.sort()
    return {"policy": policy, "offered_per_s": offered, "succeeded_per_s": succeeded,
            "peak_offered": max(offered), "attempts_total": attempts_total, "ops_succeeded": ok_ops, "ops_failed": failed_ops,
            "recovered_s_after_outage": None if recovered_at is None else recovered_at - OUTAGE[1],
            "p99_latency_s": round(lat[int(len(lat) * .99) - 1], 2) if lat else None}


def synchronized_blip(policy, clients=2000, seed=3):
    """2,000 requests fail at the same instant (a connection reset); measure retry arrivals per 10 ms."""
    rnd = random.Random(seed)
    hist = [0] * 1200   # 12 s in 10 ms buckets
    for _ in range(clients):
        t = 0.0
        for n in range(6):
            base = min(0.1 * 2 ** n, 10.0)
            t += base if policy == "exponential backoff" else rnd.uniform(0, base)
            b = int(t / TICK)
            if b < len(hist):
                hist[b] += 1
    return {"policy": policy, "per_10ms": hist, "peak_per_10ms": max(hist)}


def layered_retries(layers=3, tries=3):
    calls = {"n": 0}

    def backend():
        calls["n"] += 1
        raise TimeoutError()

    def wrap(fn):
        def inner():
            for i in range(tries):
                try:
                    return fn()
                except TimeoutError:
                    if i == tries - 1:
                        raise
        return inner
    fn = backend
    for _ in range(layers):
        fn = wrap(fn)
    try:
        fn()
    except TimeoutError:
        pass
    return calls["n"]


if __name__ == "__main__":
    policies = ["no retries", "immediate retry", "fixed 1 s delay", "exponential backoff", "exponential + full jitter", "jitter + retry budget"]
    out = {"model": {"arrival_per_s": ARRIVAL, "capacity_per_s": CAPACITY, "outage_s": OUTAGE, "duration_s": DURATION},
           "runs": [simulate(p) for p in policies],
           "runs_no_degradation": [simulate(p, degrade=False) for p in policies],
           "blip": [synchronized_blip(p) for p in ("exponential backoff", "exponential + full jitter")],
           "layered": {"layers": 3, "tries_each": 3, "backend_calls_for_one_request": layered_retries()}}
    # sensitivity: how much the overloaded provider degrades, and how long clients keep retrying, decide who recovers
    out["sweep"] = {f"factor {f}, {a} attempts": {r["policy"]: {"recovered": r["recovered_s_after_outage"], "peak": r["peak_offered"], "ok": r["ops_succeeded"]}
                                                  for r in (simulate(p, factor=f, attempts=a) for p in policies)}
                    for f in (0.5, 0.75, 0.9) for a in (6, 9)}
    for k, v in out["sweep"].items():
        print("[sweep]", k, {p: x["recovered"] for p, x in v.items()}, flush=True)
    for r in out["runs"]:
        print(f"{r['policy']:28s} peak {r['peak_offered']:6d}/s  attempts {r['attempts_total']:8d}  ok {r['ops_succeeded']:6d}  "
              f"failed {r['ops_failed']:6d}  recovered +{r['recovered_s_after_outage']} s  p99 {r['p99_latency_s']} s")
    for r in out["runs_no_degradation"]:
        print(f"[no degradation] {r['policy']:28s} peak {r['peak_offered']:6d}/s ok {r['ops_succeeded']:6d} failed {r['ops_failed']:6d} recovered +{r['recovered_s_after_outage']} s")
    for r in out["blip"]:
        print(f"[blip] {r['policy']:28s} peak per 10 ms: {r['peak_per_10ms']}")
    print("layered retries: one request → backend calls:", out["layered"]["backend_calls_for_one_request"])
    R = {r["policy"]: r for r in out["runs"]}
    N = {r["policy"]: r for r in out["runs_no_degradation"]}
    report("Immediate retries during an outage", "a retry storm many times normal load", f"peak {R['immediate retry']['peak_offered']:,}/s vs {ARRIVAL:.0f}/s normal",
           R["immediate retry"]["peak_offered"] > 20 * ARRIVAL)
    report("Provider that halves its capacity under overload", "freely retrying policies keep it overloaded; a retry budget doesn't",
           " · ".join(f"{p}: {'never' if r['recovered_s_after_outage'] is None else '+%g s' % r['recovered_s_after_outage']}" for p, r in R.items()),
           R["jitter + retry budget"]["recovered_s_after_outage"] == 0 and R["exponential backoff"]["recovered_s_after_outage"] is None)
    report("Provider that doesn't degrade", "jitter recovers faster than plain backoff and fixed delays",
           f"jitter +{N['exponential + full jitter']['recovered_s_after_outage']} s, backoff +{N['exponential backoff']['recovered_s_after_outage']} s, fixed +{N['fixed 1 s delay']['recovered_s_after_outage']} s",
           N["exponential + full jitter"]["recovered_s_after_outage"] < N["exponential backoff"]["recovered_s_after_outage"])
    report("The budget across model variants", "recovers immediately whatever the degradation and retry window",
           " · ".join(f"{k}: {v['jitter + retry budget']['recovered']}" for k, v in out["sweep"].items()),
           all(v["jitter + retry budget"]["recovered"] == 0 for v in out["sweep"].values()))
    b0, b1 = out["blip"]
    report("Synchronized failure (2,000 at once)", "plain backoff retries in lockstep; full jitter spreads them", f"peak per 10 ms: {b0['peak_per_10ms']} vs {b1['peak_per_10ms']}",
           b1["peak_per_10ms"] < b0["peak_per_10ms"] / 3)
    report("Retries at 3 layers × 3 tries", "multiply", f"{out['layered']['backend_calls_for_one_request']} backend calls for one request", out["layered"]["backend_calls_for_one_request"] == 27)
    with open(__file__.replace("retry_sim.py", "retry_sim_results.json"), "w") as f:
        json.dump(out, f)
    print(f"{sum(results)}/{len(results)} passed")
