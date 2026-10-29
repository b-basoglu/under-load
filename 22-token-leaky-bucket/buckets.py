"""Rate limiter algorithms compared on the same traffic, for Part 22 (deterministic, pure Python, 1 ms resolution).

Limit for every algorithm: an average of 10 requests per second per API client.
"""
import json
import math
from collections import deque

results, data = [], {}
RATE = 10.0


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class FixedWindow:
    name = "fixed window (10 per second)"

    def __init__(self):
        self.win, self.count = -1, 0

    def allow(self, t):
        w = int(t)
        if w != self.win:
            self.win, self.count = w, 0
        if self.count < RATE:
            self.count += 1
            return "ok"
        return "reject"


class SlidingLog:
    name = "sliding window (10 in any 1 s)"

    def __init__(self):
        self.log = deque()

    def allow(self, t):
        while self.log and self.log[0] <= t - 1.0:
            self.log.popleft()
        if len(self.log) < RATE:
            self.log.append(t)
            return "ok"
        return "reject"


class TokenBucket:
    def __init__(self, capacity, rate=RATE):
        self.cap, self.rate, self.tokens, self.last = capacity, rate, capacity, 0.0
        self.name = f"token bucket (rate {rate:g}/s, capacity {capacity})"

    def allow(self, t):
        self.tokens = min(self.cap, self.tokens + (t - self.last) * self.rate)
        self.last = t
        if self.tokens >= 1:
            self.tokens -= 1
            return "ok"
        return "reject"


class LeakyQueue:
    """Leaky bucket as a queue: requests wait and leave at exactly RATE per second; the queue holds at most `cap`."""

    def __init__(self, cap):
        self.cap, self.next_free, self.name = cap, 0.0, f"leaky bucket (drain 10/s, queue {cap})"
        self.departures = deque()

    def allow(self, t):
        while self.departures and self.departures[0] <= t:
            self.departures.popleft()
        if len(self.departures) >= self.cap:
            return "reject"
        start = max(t, self.next_free)
        self.next_free = start + 1 / RATE
        self.departures.append(start)
        return ("ok", start)


def traffic(kind, seconds=120):
    ts = []
    if kind == "steady":                     # a well-behaved client: 8 req/s, evenly spread
        ts = [i / 8 for i in range(seconds * 8)]
    elif kind == "bursty":                   # a sync job: 300 requests in 1 s, then idle for 59 s
        for m in range(seconds // 60):
            ts += [m * 60 + i / 300 for i in range(300)]
    elif kind == "boundary":                 # 10 at the end of second 4, 10 at the start of second 5
        ts = [4.9 + i * 0.01 for i in range(10)] + [5.0 + i * 0.01 for i in range(10)]
    elif kind == "abusive":                  # a broken retry loop: 50 req/s, constantly
        ts = [i / 50 for i in range(seconds * 50)]
    return ts


def run(limiter, ts):
    acc, rej, waits, sent = 0, 0, [], []
    for t in ts:
        r = limiter.allow(t)
        if r == "reject":
            rej += 1
        else:
            acc += 1
            at = r[1] if isinstance(r, tuple) else t
            waits.append(at - t)
            sent.append(at)
    per_100ms = {}
    for s in sent:
        b = int(s * 10 + 1e-9)                       # float-safe bucket: 0.30000000000000004 belongs to 0.3
        per_100ms[b] = per_100ms.get(b, 0) + 1
    waits.sort()
    return {"accepted": acc, "rejected": rej, "max_downstream_per_100ms": max(per_100ms.values()) if per_100ms else 0,
            "max_wait_s": round(waits[-1], 2) if waits else 0, "p99_wait_s": round(waits[int(len(waits) * .99) - 1], 2) if waits else 0}


if __name__ == "__main__":
    makers = [FixedWindow, SlidingLog, lambda: TokenBucket(10), lambda: TokenBucket(100), lambda: LeakyQueue(100)]
    for kind in ("steady", "bursty", "boundary", "abusive"):
        ts = traffic(kind)
        data[kind] = {"requests": len(ts), "results": {}}
        for mk in makers:
            lim = mk()
            data[kind]["results"][lim.name] = run(lim, ts)
        print(kind, len(ts))
        for k, v in data[kind]["results"].items():
            print(f"   {k:40s} {v}")
    B = data["bursty"]["results"]
    tb100, lq, tb10 = B["token bucket (rate 10/s, capacity 100)"], B["leaky bucket (drain 10/s, queue 100)"], B["token bucket (rate 10/s, capacity 10)"]
    report("Bursty sync job (300 requests in 1 s, then idle 59 s)",
           "a token bucket with capacity 100 admits a 100-request burst; capacity 10 admits only 10",
           f"accepted per minute: capacity 100 → {tb100['accepted'] // 2}, capacity 10 → {tb10['accepted'] // 2} (of 300)",
           tb100["accepted"] > 4 * tb10["accepted"] and tb10["accepted"] < 50 and tb100["accepted"] > 150)
    report("Token bucket vs leaky bucket on the same burst", "token bucket passes the burst on at once; leaky bucket smooths it but makes requests wait",
           f"downstream peak per 100 ms: token bucket {tb100['max_downstream_per_100ms']}, leaky {lq['max_downstream_per_100ms']}; "
           f"longest wait: token bucket {tb100['max_wait_s']} s, leaky {lq['max_wait_s']} s",
           tb100["max_downstream_per_100ms"] > 10 * lq["max_downstream_per_100ms"]
           and lq["max_downstream_per_100ms"] == 1 and lq["max_wait_s"] > 5 and tb100["max_wait_s"] == 0)
    bd = data["boundary"]["results"]
    report("Fixed window at the boundary (10 at 4.9 s, 10 at 5.0 s)", "a fixed window lets 2× the limit through in 0.2 s; a sliding window doesn't",
           f"accepted: fixed window {bd['fixed window (10 per second)']['accepted']}, sliding window {bd['sliding window (10 in any 1 s)']['accepted']}",
           bd["fixed window (10 per second)"]["accepted"] == 20 and bd["sliding window (10 in any 1 s)"]["accepted"] == 10)
    ab = data["abusive"]["results"]
    report("Broken client sending 50 req/s forever", "every algorithm caps it near 10/s on average",
           " · ".join(f"{k}: {v['accepted'] / 120:.1f}/s" for k, v in ab.items()),
           all(abs(v["accepted"] / 120 - 10) < 1.2 for v in ab.values())
           and all(v["rejected"] > v["accepted"] for v in ab.values()))
    st = data["steady"]["results"]
    report("Well-behaved client at 8 req/s", "nothing is rejected by any algorithm", " · ".join(f"{k.split(' (')[0]}: {v['rejected']}" for k, v in st.items()),
           all(v["rejected"] == 0 and v["accepted"] == data["steady"]["requests"] for v in st.values()))
    with open(__file__.replace("buckets.py", "buckets_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
