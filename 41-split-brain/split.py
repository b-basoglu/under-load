"""Split brain, measured for Part 41.

Two data centers. DC-A: PostgreSQL ul-pga (55470) + etcd ul-e1 (52381). DC-B: PostgreSQL ul-pgb (55471) + etcd ul-e2,
ul-e3 (52382, 52383). Containers talk to each other over the 'ul-cluster' network; the partition disconnects DC-A's
containers from it. The ticket database runs active/passive: one PG node is primary (accepts writes), the other is
read-only. Clients in each DC write to the node in their own DC.
"""
import base64
import json
import subprocess
import threading
import time
import urllib.request

import psycopg

PG = {"A": "host=localhost port=55470 user=postgres password=test dbname=postgres connect_timeout=2",
      "B": "host=localhost port=55471 user=postgres password=test dbname=postgres connect_timeout=2"}
ETCD = {"A": "http://localhost:52381", "B": "http://localhost:52382"}
DC_A = ["ul-e1", "ul-pga"]
KEY = "/tickets-db/primary"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def b64(s):
    return base64.b64encode(s.encode()).decode()


def etcd(side, path, body, timeout=1.0):
    req = urllib.request.Request(ETCD[side] + path, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def partition(on):
    for c in DC_A:
        subprocess.run(["docker", "network", "disconnect" if on else "connect", "ul-cluster", c], capture_output=True)


def set_primary(side, primary):
    with psycopg.connect(PG[side], autocommit=True) as c:
        c.execute(f"ALTER SYSTEM SET default_transaction_read_only = {'off' if primary else 'on'}")
        c.execute("SELECT pg_reload_conf()")
        c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename = 'postgres' AND backend_type = 'client backend'")


def setup():
    partition(False)
    time.sleep(3)
    for side in "AB":
        with psycopg.connect(PG[side], autocommit=True) as c:
            c.execute("ALTER SYSTEM RESET default_transaction_read_only")
            c.execute("SELECT pg_reload_conf()")
        with psycopg.connect(PG[side], autocommit=True) as c:
            c.execute("DROP TABLE IF EXISTS ticket_updates")
            c.execute("CREATE TABLE ticket_updates (id bigserial PRIMARY KEY, ticket_id int, side text, at timestamptz DEFAULT clock_timestamp())")
    for side in "AB":
        try:
            etcd(side, "/v3/kv/deleterange", {"key": b64(KEY)})
        except Exception:
            pass
    time.sleep(1)


def clients(stop, accepted):
    """Agents in each DC keep updating the same 20 tickets through their local database, 20 updates/s per side."""
    def run(side):
        i = 0
        while not stop.is_set():
            try:
                with psycopg.connect(PG[side], autocommit=True) as c:
                    while not stop.is_set():
                        c.execute("INSERT INTO ticket_updates (ticket_id, side) VALUES (%s, %s)", (i % 20, side))
                        accepted[side].append(time.time())
                        i += 1
                        time.sleep(0.05)
            except Exception:
                time.sleep(0.1)                   # read-only or unreachable: the write fails, the agent retries
    ts = [threading.Thread(target=run, args=(s,)) for s in "AB"]
    for t in ts: t.start()
    return ts


# ---- 1. heartbeat failover, no quorum -----------------------------------------------------------------------
def naive(secs_after=6):
    """Each node's failover agent pings its peer; after 2 s without an answer it promotes itself."""
    setup()
    set_primary("A", True)
    set_primary("B", False)
    stop, accepted = threading.Event(), {"A": [], "B": []}
    agents_stop = threading.Event()
    promoted = {"A": None, "B": None}

    def agent(me, peer_container, my_container):
        last_ok = time.time()
        while not agents_stop.is_set():
            r = subprocess.run(["docker", "exec", my_container, "pg_isready", "-h", peer_container, "-t", "1"], capture_output=True)
            if r.returncode == 0:
                last_ok = time.time()
            elif time.time() - last_ok > 2 and promoted[me] is None:
                set_primary(me, True)               # "my peer is dead, I am the primary now"
                promoted[me] = time.time()
            time.sleep(0.3)
    ag = [threading.Thread(target=agent, args=("A", "ul-pgb", "ul-pga")), threading.Thread(target=agent, args=("B", "ul-pga", "ul-pgb"))]
    for a in ag: a.start()
    cs = clients(stop, accepted)
    time.sleep(3)
    t_part = time.time()
    partition(True)
    time.sleep(secs_after)
    stop.set()
    agents_stop.set()
    for t in cs + ag: t.join()
    partition(False)
    after = {s: sum(1 for t in accepted[s] if t > t_part) for s in "AB"}
    both = 0
    for s in "AB":
        pass
    a_t = [t for t in accepted["A"] if t > t_part]
    b_t = [t for t in accepted["B"] if t > t_part]
    overlap_s = max(0.0, min(max(a_t, default=0), max(b_t, default=0)) - max(min(a_t, default=1e18), min(b_t, default=1e18)))
    with psycopg.connect(PG["A"]) as ca, psycopg.connect(PG["B"]) as cb:
        ta = {r[0] for r in ca.execute("SELECT DISTINCT ticket_id FROM ticket_updates WHERE at > to_timestamp(%s)", (t_part,)).fetchall()}
        tb = {r[0] for r in cb.execute("SELECT DISTINCT ticket_id FROM ticket_updates WHERE at > to_timestamp(%s)", (t_part,)).fetchall()}
    return {"accepted_after_partition": after, "both_primary_s": round(overlap_s, 1), "tickets_changed_on_both_sides": len(ta & tb),
            "b_promoted_after_s": round(promoted["B"] - t_part, 1) if promoted["B"] else None}


# ---- 2. quorum lease, with and without self-fencing ------------------------------------------------------
def quorum(self_fence, ttl=3, secs_after=10):
    """The primary role is an etcd lease. DC-A's agent talks to e1, DC-B's to e2. A primary that cannot renew its lease
    within `renew_deadline` makes its database read-only (self_fence) — or keeps going until it notices (no self_fence)."""
    setup()
    set_primary("A", False)
    set_primary("B", False)
    stop, accepted = threading.Event(), {"A": [], "B": []}
    agents_stop = threading.Event()
    events = []

    def agent(me):
        lease, primary, last_renew = None, False, 0.0
        while not agents_stop.is_set():
            now = time.time()
            try:
                if not primary:
                    if not etcd(me, "/v3/kv/range", {"key": b64(KEY)}, 0.5).get("kvs"):
                        lease = etcd(me, "/v3/lease/grant", {"TTL": ttl}, 0.5)["ID"]
                        r = etcd(me, "/v3/kv/txn", {"compare": [{"key": b64(KEY), "target": "CREATE", "create_revision": "0"}],
                                                    "success": [{"request_put": {"key": b64(KEY), "value": b64(me), "lease": lease}}]}, 0.5)
                        if r.get("succeeded"):
                            set_primary(me, True)
                            primary, last_renew = True, time.time()
                            events.append((time.time(), me, "promoted"))
                else:
                    k = etcd(me, "/v3/lease/keepalive", {"ID": lease}, 0.5).get("result", {})
                    if int(k.get("TTL", 0)) > 0:
                        last_renew = time.time()
                    else:
                        raise RuntimeError("lease lost")
            except Exception:
                if primary and (self_fence and now - last_renew > ttl / 3 or not self_fence and now - last_renew > ttl * 3):
                    set_primary(me, False)        # can't prove I'm still primary → stop accepting writes
                    primary = False
                    events.append((time.time(), me, "demoted"))
            time.sleep(0.2)
    ag = [threading.Thread(target=agent, args=(s,)) for s in "AB"]
    ag[0].start()
    deadline = time.time() + 8                      # wait until DC-A actually holds the lease
    while time.time() < deadline and not any(e[1] == "A" and e[2] == "promoted" for e in events):
        time.sleep(0.2)
    if not any(e[1] == "A" and e[2] == "promoted" for e in events):
        raise RuntimeError("DC-A failed to become primary before starting DC-B")
    time.sleep(0.5)
    ag[1].start()
    cs = clients(stop, accepted)
    time.sleep(3)
    t_part = time.time()
    partition(True)
    time.sleep(secs_after)
    stop.set()
    agents_stop.set()
    for t in cs + ag: t.join()
    partition(False)
    a_t = [t for t in accepted["A"] if t > t_part]
    b_t = [t for t in accepted["B"] if t > t_part]
    overlap = 0.0
    if a_t and b_t:
        overlap = max(0.0, min(max(a_t), max(b_t)) - max(min(a_t), min(b_t)))
    return {"self_fence": self_fence, "ttl": ttl, "accepted_after_partition": {"A": len(a_t), "B": len(b_t)},
            "a_last_write_after_s": round(max(a_t) - t_part, 1) if a_t else None, "b_first_write_after_s": round(min(b_t) - t_part, 1) if b_t else None,
            "both_accepting_s": round(overlap, 1), "events": [(round(t - t_part, 1), s, e) for t, s, e in events]}


# ---- 3. etcd itself: majority vs minority ---------------------------------------------------------------
def etcd_sides(secs=5):
    partition(False)
    time.sleep(3)
    partition(True)
    time.sleep(2)
    ok = {"A": 0, "B": 0}
    fail = {"A": 0, "B": 0}
    t0 = time.time()
    i = 0
    while time.time() - t0 < secs:
        for side in "AB":
            try:
                etcd(side, "/v3/kv/put", {"key": b64(f"/probe/{side}/{i}"), "value": b64("x")}, 0.5)
                ok[side] += 1
            except Exception:
                fail[side] += 1
        i += 1
    partition(False)
    return {"minority_e1": {"ok": ok["A"], "failed": fail["A"]}, "majority_e2e3": {"ok": ok["B"], "failed": fail["B"]}}


if __name__ == "__main__":
    N = naive()
    print("naive", N, flush=True)
    Q0 = quorum(False)
    print("quorum, no self-fence", {k: v for k, v in Q0.items()}, flush=True)
    Q1 = quorum(True)
    print("quorum, self-fence", {k: v for k, v in Q1.items()}, flush=True)
    E = etcd_sides()
    print("etcd", E, flush=True)
    data.update({"naive": N, "quorum_no_fence": Q0, "quorum_fence": Q1, "etcd": E})
    report("Heartbeat failover without quorum, network partition", "both sides decide the other died; both accept writes",
           f"writes accepted after the partition: DC-A {N['accepted_after_partition']['A']}, DC-B {N['accepted_after_partition']['B']}; both primary for {N['both_primary_s']} s; "
           f"{N['tickets_changed_on_both_sides']} tickets changed on both sides",
           N["accepted_after_partition"]["A"] > 20 and N["accepted_after_partition"]["B"] > 20 and N["both_primary_s"] > 1 and N["tickets_changed_on_both_sides"] > 5)
    report("etcd during the partition", "the majority side keeps working; the minority side refuses",
           f"minority (e1): {E['minority_e1']['ok']} ok / {E['minority_e1']['failed']} failed · majority (e2, e3): {E['majority_e2e3']['ok']} ok / {E['majority_e2e3']['failed']} failed",
           E["minority_e1"]["ok"] == 0 and E["minority_e1"]["failed"] > 0 and E["majority_e2e3"]["ok"] > 0 and E["majority_e2e3"]["failed"] == 0)
    report("Quorum lease, old primary keeps writing until it notices", "a new primary is elected while the old one still accepts writes",
           f"both accepting writes for {Q0['both_accepting_s']} s; DC-A last write at +{Q0['a_last_write_after_s']} s, DC-B first write at +{Q0['b_first_write_after_s']} s",
           Q0["both_accepting_s"] > 2 and Q0["a_last_write_after_s"] > Q0["b_first_write_after_s"])
    report("Quorum lease + self-fencing (read-only when it can't renew within TTL/3)", "the old primary stops before the new one starts",
           f"both accepting for {Q1['both_accepting_s']} s; DC-A last write +{Q1['a_last_write_after_s']} s, DC-B first write +{Q1['b_first_write_after_s']} s",
           Q1["both_accepting_s"] == 0 and Q1["a_last_write_after_s"] < Q1["b_first_write_after_s"] and Q1["accepted_after_partition"]["B"] > 0)
    with open(__file__.replace("split.py", "split_results.json"), "w") as fh:
        json.dump(data, fh, default=str)
    print(f"{sum(results)}/{len(results)} passed")
