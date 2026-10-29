"""Multi-layer cache coherence for Part 13: 4 app servers, each with an in-process cache, plus Redis and PostgreSQL 17."""
import json
import threading
import time

import psycopg
import redis

PG = "host=localhost port=55432 user=postgres password=test"
R = redis.Redis(host="localhost", port=56379, decode_responses=True, max_connections=200)
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def setup():
    with psycopg.connect(PG, autocommit=True) as k:
        k.execute("DROP TABLE IF EXISTS tickets CASCADE")
        k.execute("CREATE TABLE tickets (id int PRIMARY KEY, priority text, version int NOT NULL DEFAULT 1)")
        k.execute("INSERT INTO tickets SELECT g, 'normal', 1 FROM generate_series(1, 100) g")
    R.flushall()


class Server:
    """An app server: in-process cache (L1) → Redis (L2) → PostgreSQL."""

    def __init__(self, name, l1_ttl, subscribe=False, versioned=False):
        self.name, self.l1_ttl, self.versioned = name, l1_ttl, versioned
        self.l1 = {}
        self.db = psycopg.connect(PG, autocommit=True)
        self.lock = threading.Lock()
        self.received = []
        if subscribe:
            self.ps = R.pubsub(ignore_subscribe_messages=True)
            self.ps.subscribe(**{"ticket-changes": self._on_message})
            self.thread = self.ps.run_in_thread(sleep_time=0.001, daemon=True)

    def _on_message(self, msg):
        m = json.loads(msg["data"])
        self.received.append((time.time(), m))
        with self.lock:
            cur = self.l1.get(m["id"])
            if self.versioned and "value" in m:
                if cur is None or m["version"] > cur[1]["version"]:
                    self.l1[m["id"]] = (time.time() + self.l1_ttl, {"priority": m["value"], "version": m["version"]})
            else:
                self.l1.pop(m["id"], None)

    def get(self, tid):
        with self.lock:
            e = self.l1.get(tid)
            if e and e[0] > time.time():
                return e[1]
        raw = R.get(f"ticket:{tid}")
        if raw is None:
            p, v = self.db.execute("SELECT priority, version FROM tickets WHERE id = %s", (tid,)).fetchone()
            val = {"priority": p, "version": v}
            R.set(f"ticket:{tid}", json.dumps(val), ex=300)
        else:
            val = json.loads(raw)
        with self.lock:
            self.l1[tid] = (time.time() + self.l1_ttl, val)
        return val

    def write(self, tid, priority, broadcast=False):
        v = self.db.execute("UPDATE tickets SET priority = %s, version = version + 1 WHERE id = %s RETURNING version",
                            (priority, tid)).fetchone()[0]
        R.delete(f"ticket:{tid}")
        with self.lock:
            self.l1.pop(tid, None)
        if broadcast:
            R.publish("ticket-changes", json.dumps({"id": tid}))
        return v

    def close(self):
        if hasattr(self, "thread"):
            self.thread.stop()
        self.db.close()


def stale_window(broadcast):
    setup()
    servers = [Server(f"s{i}", l1_ttl=3.0, subscribe=broadcast) for i in range(4)]
    time.sleep(0.2)
    for s in servers:
        s.get(7)
    t0 = time.time()
    servers[0].write(7, "urgent", broadcast=broadcast)
    buckets = []
    while time.time() - t0 < 4.0:
        views = [s.get(7)["priority"] for s in servers]
        buckets.append((round(time.time() - t0, 2), views.count("normal")))
        time.sleep(0.05)
    for s in servers:
        s.close()
    last_stale = max([t for t, n in buckets if n > 0], default=0)
    return buckets, last_stale


def test_ttl_vs_broadcast():
    b1, last1 = stale_window(False)
    b2, last2 = stale_window(True)
    data["stale_window"] = {"ttl_only_last_stale_s": last1, "broadcast_last_stale_s": last2,
                            "ttl_only_series": b1, "broadcast_series": b2}
    peak1 = max(n for _, n in b1)
    report(
        "Priority changed on server 1; 4 servers each hold it in a 3 s in-process cache",
        "with TTL only, the other 3 servers serve the old value for up to the local TTL; with a broadcast, almost not at all",
        f"last stale read: TTL only at {last1:.2f} s ({peak1} servers stale); with pub/sub invalidation at {last2:.2f} s",
        last1 > 2.5 and peak1 >= 3 and last2 < 0.2,
    )


def test_disagreement():
    """TTL alone doesn't coordinate copies: entries cached at different times expire at different times."""
    setup()
    servers = [Server(f"s{i}", l1_ttl=4.0) for i in range(4)]
    for i, s in enumerate(servers):
        s.get(9)
        time.sleep(1.0)          # servers first saw ticket 9 at different moments
    servers[0].write(9, "high")
    seen = []
    t0 = time.time()
    while time.time() - t0 < 4.2:
        seen.append(tuple(s.get(9)["priority"] for s in servers))
        time.sleep(0.1)
    for s in servers:
        s.close()
    mixed = sum(1 for v in seen if len(set(v)) > 1)
    data["disagreement"] = {"samples": len(seen), "samples_where_servers_disagree": mixed, "example": seen[len(seen) // 3]}
    report("Same TTL, cached at different moments",
           "servers disagree with each other for seconds, even though every entry has a TTL",
           f"{mixed} of {len(seen)} samples had servers showing different priorities, e.g. {seen[len(seen) // 3]}", mixed > 10)


def test_lost_message():
    setup()
    servers = [Server(f"s{i}", l1_ttl=3.0, subscribe=True) for i in range(4)]
    time.sleep(0.2)
    for s in servers:
        s.get(11)
    servers[2].ps.unsubscribe("ticket-changes")           # server 3 briefly disconnected (deploy, network blip)
    time.sleep(0.1)
    servers[0].write(11, "urgent", broadcast=True)
    time.sleep(0.2)
    servers[2].ps.subscribe(**{"ticket-changes": servers[2]._on_message})
    t0 = time.time()
    stale_for = 0
    while time.time() - t0 < 3.5:
        if servers[2].get(11)["priority"] == "normal":
            stale_for = time.time() - t0
        time.sleep(0.05)
    others = [servers[i].get(11)["priority"] for i in (1, 3)]
    for s in servers:
        s.close()
    data["lost_message"] = {"server3_stale_for_s": round(stale_for, 2), "other_servers": others}
    report(
        "Server 3 is disconnected while the invalidation is published",
        "Redis pub/sub doesn't store messages: server 3 never hears it and stays stale until its TTL",
        f"server 3 stale for another {stale_for:.2f} s; servers 2 and 4 showed {others}",
        stale_for > 2.0 and others == ["urgent", "urgent"],
    )


def test_out_of_order():
    setup()
    s_naive = Server("naive", l1_ttl=60)
    s_ver = Server("versioned", l1_ttl=60, versioned=True)
    # update messages carrying values arrive out of order: v3 (solved) then v2 (pending)
    msgs = [{"id": 5, "value": "solved", "version": 3}, {"id": 5, "value": "pending", "version": 2}]
    for m in msgs:
        with s_naive.lock:   # naive: apply every update as it arrives
            s_naive.l1[5] = (time.time() + 60, {"priority": m["value"], "version": m["version"]})
        s_ver._on_message({"data": json.dumps(m)})
    naive, ver = s_naive.l1[5][1]["priority"], s_ver.l1[5][1]["priority"]
    s_naive.close(); s_ver.close()
    data["out_of_order"] = {"naive": naive, "versioned": ver}
    report("Update messages arrive out of order (v3 then v2)", "applying each one leaves the older value; a version check keeps the newer one",
           f"naive cache: {naive}; version-aware cache: {ver}", naive == "pending" and ver == "solved")


def test_propagation():
    setup()
    servers = [Server(f"s{i}", l1_ttl=30, subscribe=True) for i in range(8)]
    time.sleep(0.3)
    lat = []
    for n in range(50):
        for s in servers:
            s.received.clear()
        t0 = time.time()
        R.publish("ticket-changes", json.dumps({"id": 1}))
        deadline = t0 + 1
        while time.time() < deadline and not all(s.received for s in servers):
            time.sleep(0.0005)
        lat.append(max(s.received[0][0] for s in servers if s.received) - t0)
    for s in servers:
        s.close()
    lat.sort()
    data["propagation_ms"] = {"median": round(lat[len(lat) // 2] * 1000, 2), "p99": round(lat[-1] * 1000, 2)}
    med = lat[len(lat) // 2]
    report(
        "Pub/sub invalidation reaching 8 servers",
        "the stale window shrinks to milliseconds",
        f"time until all 8 had the message: median {med * 1000:.2f} ms, worst {lat[-1] * 1000:.2f} ms",
        lat[-1] < 0.1 and med < 0.05,
    )


if __name__ == "__main__":
    test_ttl_vs_broadcast()
    test_disagreement()
    test_lost_message()
    test_out_of_order()
    test_propagation()
    with open(__file__.replace("coherence.py", "coherence_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
