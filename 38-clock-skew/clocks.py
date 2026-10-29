"""Clock skew, measured for Part 38.

Containers share the host's clock, so each server's clock is modelled explicitly: true time + an offset (+ jumps).
Writes are applied to a real PostgreSQL 17 table in true-time order; only the timestamps the servers attach differ.
"""
import json
import random

import psycopg

PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class HLC:
    """Hybrid logical clock: (wall ms, counter). Never goes backwards; jumps past any timestamp it has seen."""
    def __init__(self, clock):
        self.clock, self.l, self.c = clock, 0, 0

    def now(self, t):
        pt = self.clock(t)
        if pt > self.l:
            self.l, self.c = pt, 0
        else:
            self.c += 1
        return (self.l, self.c)

    def observe(self, ts, t):
        pt = self.clock(t)
        l0 = self.l
        self.l = max(l0, ts[0], pt)
        if self.l == l0 == ts[0]:
            self.c = max(self.c, ts[1]) + 1
        elif self.l == l0:
            self.c += 1
        elif self.l == ts[0]:
            self.c = ts[1] + 1
        else:
            self.c = 0


def lww(skew_ms, mode, n=2000, seed=1):
    """Two app servers update the same tickets. Server B's clock is skew_ms behind. Each update first reads the ticket
    (as the UI does), then writes it 'if newer'. mode: wall-clock | hlc | db-version."""
    rnd = random.Random(seed)
    clocks = {"A": lambda t: t, "B": lambda t: t - skew_ms}
    hlcs = {s: HLC(clocks[s]) for s in clocks}
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS tickets_lww")
        c.execute("CREATE TABLE tickets_lww (id int PRIMARY KEY, status text, ts_l bigint, ts_c int, version int)")
        c.execute("INSERT INTO tickets_lww SELECT g, 'open', 0, 0, 0 FROM generate_series(1, 5) g")
        t, lost, last_true = 1_000_000.0, 0, {}
        for i in range(n):
            t += rnd.expovariate(1 / 150)                        # true time between updates: 150 ms on average
            server, tid = rnd.choice("AB"), rnd.randint(1, 5)   # five busy tickets, worked from both servers
            status = f"s{i}"
            row = c.execute("SELECT ts_l, ts_c, version FROM tickets_lww WHERE id = %s", (tid,)).fetchone()
            if mode == "wall-clock":
                ts = (int(clocks[server](t)), 0)
                r = c.execute("UPDATE tickets_lww SET status = %s, ts_l = %s WHERE id = %s AND ts_l < %s RETURNING 1", (status, ts[0], tid, ts[0])).fetchone()
            elif mode == "hlc":
                hlcs[server].observe((row[0], row[1]), t)       # reading the row carries its timestamp to this server
                ts = hlcs[server].now(t)
                r = c.execute("UPDATE tickets_lww SET status = %s, ts_l = %s, ts_c = %s WHERE id = %s AND (ts_l, ts_c) < (%s, %s) RETURNING 1",
                              (status, ts[0], ts[1], tid, ts[0], ts[1])).fetchone()
            else:                                                # the database orders writes: version from what was read
                r = c.execute("UPDATE tickets_lww SET status = %s, version = version + 1 WHERE id = %s AND version = %s RETURNING 1",
                              (status, tid, row[2])).fetchone()
            last_true[tid] = status
            lost += r is None                                    # a later write was discarded as 'older'
        final = dict(c.execute("SELECT id, status FROM tickets_lww").fetchall())
    wrong = sum(1 for k, v in last_true.items() if final[k] != v)
    return {"skew_ms": skew_ms, "mode": mode, "updates": n, "discarded": lost, "tickets_wrong_final_state": wrong}


def log_order(skew_ms, n=2000, seed=2):
    """Ticket created on server A, first reply 20–200 ms later on server B. Sorted by timestamp, does the reply come first?"""
    rnd = random.Random(seed)
    inverted = 0
    for _ in range(n):
        t = rnd.uniform(0, 1e6)
        created = t                                           # A: accurate clock
        reply = t + rnd.uniform(20, 200) - skew_ms            # B: skew_ms slow
        inverted += reply < created
    return {"skew_ms": skew_ms, "pairs": n, "reply_before_create_pct": round(100 * inverted / n, 1)}


def timeout_with_jump(jump_ms, timeout_ms=5000):
    """A request with a 5 s timeout; 1 s in, NTP steps the wall clock by jump_ms. When does the timeout fire?"""
    true_fire_wall = timeout_ms - jump_ms                    # deadline computed as wall_start + 5 s, wall clock moved
    true_fire_mono = timeout_ms                              # monotonic clocks never jump
    return {"jump_ms": jump_ms, "fires_after_ms_wall": true_fire_wall, "fires_after_ms_monotonic": true_fire_mono}


def lease(skew_ms=0, drift=0.0, start="absolute", margin_ms=0, ttl_ms=10_000, n=1000, seed=4):
    """Worker 1 gets a 10 s lease from a lock service; the service hands it to worker 2 when it expires on the service's side.
    Overlap = time both workers believe they hold it.

    start="absolute": the service sends "valid until T" in its own clock; the worker compares T with its own clock.
                      A constant offset between the clocks (skew_ms) shifts the end directly.
    start="receipt":  the service sends a TTL; the worker starts its own monotonic timer when the reply ARRIVES.
    start="send":     the worker starts its timer when it SENT the request, before the service granted anything.
    A constant offset cancels for relative TTLs; what remains is clock rate (drift: the worker's clock runs slow by this
    fraction, so its 10 s last longer) and the network delay of the request and the reply."""
    rnd = random.Random(seed)
    overlaps, total_overlap, worst = 0, 0.0, 0.0
    for _ in range(n):
        granted = rnd.uniform(0, 1e6)                               # true time the service granted it
        up, down = rnd.uniform(1, 40), rnd.uniform(1, 40)           # request and reply delay, ms
        service_end = granted + ttl_ms
        if start == "absolute":
            worker_end = granted + ttl_ms + skew_ms                 # worker's clock is behind: "until T" happens later for it
        elif start == "receipt":
            worker_end = granted + down + ttl_ms * (1 + drift)
        else:
            worker_end = granted - up + ttl_ms * (1 + drift)
        worker_end -= margin_ms
        o = worker_end - service_end
        if o > 0:
            overlaps += 1
            total_overlap += o
            worst = max(worst, o)
    return {"skew_ms": skew_ms, "drift": drift, "start": start, "margin_ms": margin_ms, "overlap_pct": round(100 * overlaps / n, 1),
            "overlap_ms": round(total_overlap / max(1, overlaps), 1), "worst_ms": round(worst, 1)}


if __name__ == "__main__":
    L = {f"{m}@{s}": lww(s, m) for s in (0, 50, 300) for m in ("wall-clock", "hlc", "db-version")}
    for v in L.values():
        print(v, flush=True)
    O = {s: log_order(s) for s in (0, 50, 300)}
    print(O, flush=True)
    J = {j: timeout_with_jump(j) for j in (-2000, 2000)}
    print(J, flush=True)
    LS = {"absolute expiry, clocks 500 ms apart": lease(skew_ms=500),
          "absolute expiry, 500 ms apart, 1 s margin": lease(skew_ms=500, margin_ms=1000),
          "relative TTL, clocks 500 ms apart, no drift": lease(skew_ms=500, start="send"),
          "relative TTL from receipt, clock 0.5% slow": lease(drift=0.005, start="receipt"),
          "relative TTL from send, clock 0.5% slow": lease(drift=0.005, start="send"),
          "relative TTL from send, 0.5% slow, 100 ms margin": lease(drift=0.005, start="send", margin_ms=100)}
    print(LS, flush=True)
    data.update({"lww": L, "log_order": {str(k): v for k, v in O.items()}, "jump": {str(k): v for k, v in J.items()}, "lease": LS})
    w0, w50, w300 = L["wall-clock@0"], L["wall-clock@50"], L["wall-clock@300"]
    h300, d300 = L["hlc@300"], L["db-version@300"]
    report("Last-write-wins on wall-clock timestamps, one server 300 ms slow", "later updates are discarded as 'older'",
           f"discarded: {w0['discarded']} with no skew, {w50['discarded']} at 50 ms, {w300['discarded']} at 300 ms; tickets ending in the wrong state at 300 ms: {w300['tickets_wrong_final_state']}",
           w300["discarded"] > 100 and w300["discarded"] > w50["discarded"] > w0["discarded"] and w300["tickets_wrong_final_state"] > 0)
    report("Same with a hybrid logical clock", "reading the ticket moves the slow server's clock past it",
           f"discarded {h300['discarded']}, wrong final state {h300['tickets_wrong_final_state']}", h300["discarded"] == 0 and h300["tickets_wrong_final_state"] == 0)
    report("Same with a database-assigned version", "ordering comes from the database, not from clocks",
           f"discarded {d300['discarded']}, wrong final state {d300['tickets_wrong_final_state']}", d300["discarded"] == 0 and d300["tickets_wrong_final_state"] == 0)
    report("Sorting logs by timestamp across servers", "effects appear before their causes",
           f"reply before ticket creation: {O[0]['reply_before_create_pct']}% (no skew), {O[50]['reply_before_create_pct']}% (50 ms), {O[300]['reply_before_create_pct']}% (300 ms)",
           O[300]["reply_before_create_pct"] == 100 and O[50]["reply_before_create_pct"] > 0 and O[0]["reply_before_create_pct"] == 0)
    report("Timeouts measured with the wall clock across an NTP step", "a 5 s timeout fires after 3 s or 7 s",
           f"wall clock: {J[2000]['fires_after_ms_wall']} ms / {J[-2000]['fires_after_ms_wall']} ms; monotonic: {J[2000]['fires_after_ms_monotonic']} ms",
           J[2000]["fires_after_ms_wall"] == 3000 and J[-2000]["fires_after_ms_wall"] == 7000 and J[2000]["fires_after_ms_monotonic"] == 5000)
    ab, abm, rel0, rr, rs, rsm = LS.values()
    report("Lease with an absolute expiry time, clocks 500 ms apart", "the offset shifts the end directly: both workers hold it for ~the offset",
           f"overlap in {ab['overlap_pct']}% of leases, {ab['overlap_ms']} ms each; with a 1 s margin: {abm['overlap_pct']}%",
           ab["overlap_pct"] == 100 and ab["overlap_ms"] >= 400 and abm["overlap_pct"] == 0)
    report("Lease with a relative TTL (what Redis PX / etcd / ZooKeeper sessions use)", "a constant offset cancels; rate drift and network delay remain",
           f"500 ms offset, no drift: {rel0['overlap_pct']}% overlap · 0.5% slow clock, timer from receipt: {rr['overlap_pct']}% (avg {rr['overlap_ms']} ms, worst {rr['worst_ms']} ms) · "
           f"timer from send: {rs['overlap_pct']}% (worst {rs['worst_ms']} ms) · plus a 100 ms margin: {rsm['overlap_pct']}%",
           rel0["overlap_pct"] == 0 and rr["overlap_ms"] > rs["overlap_ms"] and rsm["overlap_pct"] == 0)
    with open(__file__.replace("clocks.py", "clocks_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
