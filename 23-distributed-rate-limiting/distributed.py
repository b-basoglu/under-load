"""Distributed rate limiting, measured for Part 23 against Redis 7.4 (port 56379).

10 "API servers" (threads) share one client's traffic. The client's limit is 20 requests/s.
The client sends 200 requests/s for 5 s, spread over the servers by the load balancer.
"""
import json
import random
import threading
import time

import redis

R = redis.Redis(host="localhost", port=56379, decode_responses=True, max_connections=200)
LIMIT, CLIENT_RATE, SECONDS, SERVERS = 20.0, 200, 5.0, 10
results, data = [], {}

TOKEN_BUCKET = R.register_script("""
-- KEYS[1] bucket key; ARGV: rate, capacity, now (seconds, float), cost
local rate, cap, now, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens, ts = tonumber(b[1]) or cap, tonumber(b[2]) or now
if now < ts then now = ts end      -- calls can arrive out of order; never move the bucket's clock backwards
tokens = math.min(cap, tokens + math.max(0, now - ts) * rate)
local ok = 0
if tokens >= cost then tokens = tokens - cost; ok = 1 end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil(cap / rate) * 2)
return ok
""")


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class Local:
    def __init__(self, rate, cap):
        self.rate, self.cap, self.tokens, self.ts, self.lock = rate, cap, cap, time.time(), threading.Lock()

    def allow(self):
        with self.lock:
            now = time.time()
            self.tokens = min(self.cap, self.tokens + (now - self.ts) * self.rate); self.ts = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


def drive(allow_fn, routing="round-robin"):
    """Send CLIENT_RATE req/s for SECONDS; allow_fn(server_index) -> bool. Returns allowed per second and limiter latencies."""
    t0 = time.time()
    n = int(CLIENT_RATE * SECONDS)
    allowed, lat = 0, []
    lock = threading.Lock()
    rnd = random.Random(1)

    def one(i, server):
        nonlocal allowed
        s = time.perf_counter()
        ok = allow_fn(server)
        d = time.perf_counter() - s
        with lock:
            lat.append(d)
            allowed += 1 if ok else 0
    threads = []
    for i in range(n):
        at = t0 + i / CLIENT_RATE
        while time.time() < at:
            time.sleep(0.0005)
        server = i % SERVERS if routing == "round-robin" else rnd.choice([0, 1])   # sticky: only 2 servers see this client
        th = threading.Thread(target=one, args=(i, server)); th.start(); threads.append(th)
    for th in threads:
        th.join()
    lat.sort()
    return {"allowed_per_s": round(allowed / SECONDS, 1), "limiter_p50_ms": round(lat[len(lat) // 2] * 1000, 3),
            "limiter_p99_ms": round(lat[int(len(lat) * .99) - 1] * 1000, 3)}


def main():
    # 1. local buckets only
    local = [Local(LIMIT, LIMIT) for _ in range(SERVERS)]
    r1 = drive(lambda s: local[s].allow())
    data["local_only"] = r1
    report("Each of 10 servers enforces 20/s on its own", "the client effectively gets ~10× the limit",
           f"allowed {r1['allowed_per_s']}/s (limit {LIMIT:g}/s)", r1["allowed_per_s"] > 8 * LIMIT)

    # 2. central token bucket in Redis, atomic Lua
    R.delete("rl:client:42")
    r2 = drive(lambda s: TOKEN_BUCKET(keys=["rl:client:42"], args=[LIMIT, LIMIT, time.time(), 1]) == 1)
    data["redis_lua"] = r2
    report("One token bucket in Redis, updated atomically by a Lua script", "the client gets the real limit; each check costs a network round trip",
           f"allowed {r2['allowed_per_s']}/s; limiter latency p50 {r2['limiter_p50_ms']} ms, p99 {r2['limiter_p99_ms']} ms",
           abs(r2["allowed_per_s"] - LIMIT) < LIMIT * 0.35 and r2["limiter_p50_ms"] > 0.1)

    # 3. non-atomic read-modify-write in Redis
    R.delete("rl:client:43")

    def racy(s):
        b = R.hgetall("rl:client:43")
        now = time.time()
        tokens = float(b.get("tokens", LIMIT)); ts = float(b.get("ts", now))
        tokens = min(LIMIT, tokens + (now - ts) * LIMIT)
        time.sleep(0.005)                    # the gap between read and write (network, GC)
        ok = tokens >= 1
        R.hset("rl:client:43", mapping={"tokens": tokens - 1 if ok else tokens, "ts": now})
        return ok
    r3 = drive(racy)
    data["redis_racy"] = r3
    report("Same bucket in Redis, but GET then SET from the application", "concurrent servers read the same tokens and over-admit",
           f"allowed {r3['allowed_per_s']}/s vs {r2['allowed_per_s']}/s with the atomic script (limit {LIMIT:g}/s)",
           r3["allowed_per_s"] > 1.3 * r2["allowed_per_s"] and r3["allowed_per_s"] > LIMIT)

    # 4. split the quota locally: each server gets limit / N
    split = [Local(LIMIT / SERVERS, LIMIT / SERVERS) for _ in range(SERVERS)]
    r4 = drive(lambda s: split[s].allow())
    split2 = [Local(LIMIT / SERVERS, LIMIT / SERVERS) for _ in range(SERVERS)]
    r5 = drive(lambda s: split2[s].allow(), routing="sticky")
    data["local_split"] = {"round_robin": r4, "sticky_two_servers": r5}
    report("Each server gets limit ÷ 10 locally (no shared state)", "accurate only if traffic is spread evenly; uneven routing under-admits",
           f"even routing: {r4['allowed_per_s']}/s; client sticky to 2 servers: {r5['allowed_per_s']}/s (limit {LIMIT:g}/s); limiter p50 {r4['limiter_p50_ms']} ms",
           abs(r4["allowed_per_s"] - LIMIT) < LIMIT * 0.35 and r5["allowed_per_s"] < 0.5 * LIMIT
           and r5["allowed_per_s"] < r4["allowed_per_s"] * 0.5)

    # 5. Redis is down: fail-open vs fail-closed vs local fallback
    # an address that drops packets (a dead host or a partition), not one that refuses at once: each connect waits the timeout
    bad = redis.Redis(host="10.255.255.1", port=6379, socket_connect_timeout=0.05, socket_timeout=0.05, retry_on_timeout=False)
    out = {}
    for mode in ("fail-open", "fail-closed", "local fallback", "local fallback + breaker"):
        fallback = [Local(LIMIT / SERVERS, LIMIT / SERVERS) for _ in range(SERVERS)]
        skip_until = [0.0] * SERVERS                          # per-server breaker: stop trying the store for 2 s after a failure

        def f(s, mode=mode, fallback=fallback, skip_until=skip_until):
            try:
                if mode.endswith("breaker") and time.time() < skip_until[s]:
                    raise redis.ConnectionError("breaker open")
                time.sleep(0.05)                              # a store that doesn't answer: the client waits its 50 ms timeout
                raise redis.TimeoutError("store unreachable")  # (simulated: this sandbox refuses connections instantly)
            except redis.RedisError:
                if mode.endswith("breaker"):
                    skip_until[s] = time.time() + 2.0
                if mode == "fail-open":
                    return True
                if mode == "fail-closed":
                    return False
                return fallback[s].allow()
        out[mode] = drive(f)
        print("  ", mode, out[mode], flush=True)
    data["store_down"] = out
    report("Rate-limit store unreachable", "fail-open lets everything through, fail-closed rejects everything, a local fallback approximates the limit",
           f"fail-open {out['fail-open']['allowed_per_s']}/s · fail-closed {out['fail-closed']['allowed_per_s']}/s · local fallback {out['local fallback']['allowed_per_s']}/s",
           out["fail-open"]["allowed_per_s"] > 150 and out["fail-closed"]["allowed_per_s"] == 0 and 10 < out["local fallback"]["allowed_per_s"] < 40)
    lf, lb = out["local fallback"], out["local fallback + breaker"]
    report("What the outage costs every request", "each check waits for the store's timeout (50 ms, simulated), unless a breaker stops trying",
           f"limiter latency p50: fallback {lf['limiter_p50_ms']} ms, fallback + breaker {lb['limiter_p50_ms']} ms (p99 {lf['limiter_p99_ms']} vs {lb['limiter_p99_ms']} ms)",
           lf["limiter_p50_ms"] > 20 and lb["limiter_p50_ms"] < 1)


if __name__ == "__main__":
    main()
    with open(__file__.replace("distributed.py", "distributed_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
