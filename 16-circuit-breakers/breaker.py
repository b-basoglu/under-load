"""Circuit breaker, measured for Part 16 (real threads, simulated dependency, wall-clock time).

The ticket page runs on a pool of 20 worker threads and receives 100 requests/s. Each request does 5 ms of its own
work and then calls the notification service (normally ~20 ms). From t = 5 s to t = 15 s the notification service
hangs; calls give up after a 2 s timeout. 25 s per scenario.
"""
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

RATE, WORKERS, DURATION = 100, 20, 25.0
DOWN = (5.0, 15.0)
TIMEOUT = 2.0
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class Breaker:
    """Count-based: open when >= failure_rate of the last `window` calls failed (with at least min_calls)."""

    def __init__(self, window=20, min_calls=10, failure_rate=0.5, open_for=3.0, probes=3):
        self.window, self.min_calls, self.rate, self.open_for, self.probes = window, min_calls, failure_rate, open_for, probes
        self.lock = threading.Lock()
        self.calls = []
        self.state, self.opened_at, self.probe_ok, self.probe_inflight = "closed", 0.0, 0, False
        self.transitions = []
        self.gen = 0                                  # bumps on every state change; results from older calls are ignored

    def _set(self, s, now):
        if s != self.state:
            self.transitions.append((round(now, 2), s))
            self.gen += 1
        self.state = s

    def allow(self, now):
        """Returns the generation the call was admitted under, or None if rejected."""
        with self.lock:
            if self.state == "open" and now - self.opened_at >= self.open_for:
                self._set("half-open", now); self.probe_ok = 0
            if self.state == "closed":
                return self.gen
            if self.state == "half-open" and not self.probe_inflight:
                self.probe_inflight = True
                return self.gen
            return None

    def record(self, ok, now, gen):
        with self.lock:
            if gen != self.gen:                       # admitted before the last state change: a stale result
                return
            if self.state == "half-open":
                self.probe_inflight = False
                if ok:
                    self.probe_ok += 1
                    if self.probe_ok >= self.probes:
                        self._set("closed", now); self.calls = []
                else:
                    self._set("open", now); self.opened_at = now
                return
            self.calls = (self.calls + [ok])[-self.window:]
            fails = self.calls.count(False)
            if len(self.calls) >= self.min_calls and fails / len(self.calls) >= self.rate:
                self._set("open", now); self.opened_at = now; self.calls = []


def scenario(use_breaker, t0, fail_noise=0.0, breaker_kwargs=None, down=DOWN, timeout=TIMEOUT):
    rnd = random.Random(1)
    br = Breaker(**(breaker_kwargs or {})) if use_breaker else None
    stats = {"dep_calls": 0, "dep_calls_while_down": 0, "fallbacks": 0}
    per_sec = [[] for _ in range(int(DURATION) + 5)]
    lock = threading.Lock()

    def notify(now):
        with lock:
            stats["dep_calls"] += 1
            if down[0] <= now - t0 < down[1]:
                stats["dep_calls_while_down"] += 1
        if down[0] <= now - t0 < down[1]:
            time.sleep(timeout)
            raise TimeoutError()
        time.sleep(0.02)
        if rnd.random() < fail_noise:
            raise ConnectionError()

    def handle(arrived):
        time.sleep(0.005)
        now = time.time()
        gen = br.allow(now - t0) if br else 0
        if gen is None:
            with lock:
                stats["fallbacks"] += 1          # skip: queue the notification for later
        else:
            try:
                notify(time.time())
                ok = True
            except Exception:
                ok = False
            if br:
                br.record(ok, time.time() - t0, gen)
        done = time.time()
        with lock:
            per_sec[int(arrived - t0)].append(done - arrived)

    with ThreadPoolExecutor(WORKERS) as ex:
        i = 0
        while True:
            target = t0 + i / RATE
            if target - t0 >= DURATION:
                break
            d = target - time.time()
            if d > 0:
                time.sleep(d)
            ex.submit(handle, target)
            i += 1
    series = []
    for s in range(int(DURATION)):
        lat = sorted(per_sec[s])
        series.append({"s": s, "p50_ms": round(lat[len(lat) // 2] * 1000) if lat else None,
                       "p99_ms": round(lat[int(len(lat) * .99) - 1] * 1000) if lat else None,
                       "max_ms": round(lat[-1] * 1000) if lat else None,     # ~100 requests a second: P99 is the 2nd-slowest
                       "under_500ms": sum(1 for x in lat if x < 0.5)})
    return {"series": series, "stats": stats, "transitions": br.transitions if br else []}


if __name__ == "__main__":
    a = scenario(False, time.time())
    b = scenario(True, time.time())
    c = scenario(True, time.time(), timeout=0.3)
    data["no_breaker"], data["breaker"], data["breaker_short_timeout"] = a, b, c
    def window_p99(r, lo, hi):
        vals = [x["p99_ms"] for x in r["series"][lo:hi] if x["p99_ms"] is not None]
        return max(vals) if vals else None
    def served_fast(r, lo, hi):
        return sum(x["under_500ms"] for x in r["series"][lo:hi])
    a_p99, a_fast, a_down = window_p99(a, 5, 15), served_fast(a, 5, 15), a["stats"]["dep_calls_while_down"]
    report("Notification service hangs for 10 s; ticket page with a 2 s timeout, no breaker",
           "every worker ends up waiting on the dependency; page latency explodes for everyone",
           f"worst p99 during the outage: {a_p99:,} ms; under 500 ms: {a_fast} of 1,000; dep calls while down: {a_down}",
           a_p99 is not None and a_p99 > 5000 and a_fast < 50 and a_down >= 80)
    b_p99, b_fast, b_down = window_p99(b, 5, 15), served_fast(b, 5, 15), b["stats"]["dep_calls_while_down"]
    report("Same outage with a circuit breaker", "the breaker opens within the first calls; the page skips notifications and stays fast",
           f"worst p99 during the outage: {b_p99:,} ms; under 500 ms: {b_fast} of 1,000; "
           f"calls to the dead service: {b_down} (vs {a_down}); transitions: {b['transitions']}",
           b_fast > 700 and b_down < a_down * 0.5 and b["stats"]["fallbacks"] > 500)
    first_open = lambda r: next((t for t, s in r["transitions"] if s == "open"), None)
    open_b, open_c = first_open(b), first_open(c)
    c_fast = served_fast(c, 5, 15)
    report("Breaker with a 2 s timeout vs a 300 ms timeout", "the breaker only learns about failures when calls time out, so a shorter timeout opens it sooner",
           f"opened at t = {open_b} s (2 s timeout) vs t = {open_c} s (300 ms); under 500 ms {b_fast} vs {c_fast}; "
           f"worst p99 {window_p99(b, 5, 15):,} vs {window_p99(c, 5, 15):,} ms",
           open_b is not None and open_c is not None and open_c < open_b and c_fast >= b_fast)
    rec = [t for t, s in b["transitions"] if s == "closed" and t >= DOWN[1]]
    half = any(s == "half-open" for _, s in b["transitions"])
    report("Recovery through half-open probes", "after the service is back, a few probes succeed and the breaker closes",
           f"closed again at t = {rec[0] if rec else None} s (service back at {DOWN[1]} s); half-open seen={half}",
           bool(rec) and rec[0] < DOWN[1] + 4 and half)
    # noisy dependency: 8% random failures, never actually down
    noisy = {}
    for name, kw in (("aggressive (30% of 10, min 5)", dict(window=10, min_calls=5, failure_rate=0.3, open_for=3.0)),
                     ("reasonable (50% of 20, min 10)", dict(window=20, min_calls=10, failure_rate=0.5, open_for=3.0))):
        r = scenario(True, time.time(), fail_noise=0.08, breaker_kwargs=kw, down=(999, 999))
        noisy[name] = {"opened": sum(1 for _, s in r["transitions"] if s == "open"), "skipped": r["stats"]["fallbacks"]}
        print("  ", name, noisy[name], flush=True)
    data["noisy"] = noisy
    ag, rs = noisy["aggressive (30% of 10, min 5)"], noisy["reasonable (50% of 20, min 10)"]
    report("Healthy dependency with 8% random errors", "an aggressive threshold trips on noise; a reasonable one doesn't",
           f"aggressive: opened {ag['opened']} times, skipped {ag['skipped']} calls; reasonable: opened {rs['opened']} times, skipped {rs['skipped']}",
           ag["opened"] >= 3 and ag["skipped"] > 500 and rs["opened"] == 0 and rs["skipped"] == 0)
    with open(__file__.replace("breaker.py", "breaker_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
