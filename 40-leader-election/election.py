"""Leader election, measured for Part 40. etcd 3.5 (leases + compare-and-swap, via its JSON gateway) + PostgreSQL 17.

Three scheduler instances; only the leader should run the 'send SLA reminders' job, every 100 ms. Each run is
recorded in PostgreSQL with the instance and the epoch (the etcd revision at which it became leader).
"""
import base64
import json
import threading
import time
import urllib.request

import psycopg

ETCD = "http://localhost:52379"
PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
KEY = "/schedulers/sla-reminders/leader"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def b64(s):
    return base64.b64encode(s.encode()).decode()


def etcd(path, body):
    req = urllib.request.Request(ETCD + path, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.loads(r.read())


def setup():
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS job_runs, fence")
        c.execute("CREATE TABLE job_runs (id bigserial PRIMARY KEY, instance text, epoch bigint, at timestamptz DEFAULT clock_timestamp())")
        c.execute("CREATE TABLE fence (job text PRIMARY KEY, max_epoch bigint)")
        c.execute("INSERT INTO fence VALUES ('sla', 0)")
    etcd("/v3/kv/deleterange", {"key": b64(KEY)})


class Instance(threading.Thread):
    def __init__(self, name, ttl, mode, stop, pause_at=None, pause_s=0, crash_at=None, fenced=False, mid_job_pause=None):
        super().__init__()
        self.mid_job_pause_at, self.mid_job_pause_s = mid_job_pause or (None, 0)
        self.name_, self.ttl, self.mode, self.stop, self.fenced = name, ttl, mode, stop, fenced
        self.pause_at, self.pause_s, self.crash_at = pause_at, pause_s, crash_at
        self.lease, self.epoch, self.leader = None, None, False
        self.elections_won, self.rejected = 0, 0

    def campaign(self):
        if etcd("/v3/kv/range", {"key": b64(KEY)}).get("kvs"):
            return                                                   # someone leads; watch and wait
        self.lease = etcd("/v3/lease/grant", {"TTL": self.ttl})["ID"]
        r = etcd("/v3/kv/txn", {"compare": [{"key": b64(KEY), "target": "CREATE", "create_revision": "0"}],
                                "success": [{"request_put": {"key": b64(KEY), "value": b64(self.name_), "lease": self.lease}}],
                                "failure": [{"request_range": {"key": b64(KEY)}}]})
        if r.get("succeeded"):
            self.leader, self.epoch = True, int(r["header"]["revision"])       # epoch = revision at which we won
            self.elections_won += 1
        else:
            etcd("/v3/lease/revoke", {"ID": self.lease})
            self.lease = None

    def run_job(self, c, now=0.0):
        if self.mid_job_pause_at and now >= self.mid_job_pause_at:
            self.mid_job_pause_at = None
            time.sleep(self.mid_job_pause_s)     # checked leadership, then froze mid-job; resumes as if nothing happened
        for _ in range(5):                        # one run = reminders for 5 tickets
            self._write(c)

    def _write(self, c):
        if self.fenced:   # the database only accepts work from the newest epoch it has seen
            ok = c.execute("UPDATE fence SET max_epoch = %s WHERE job = 'sla' AND max_epoch <= %s RETURNING 1", (self.epoch, self.epoch)).fetchone()
            if not ok:
                self.rejected += 1
                return
        c.execute("INSERT INTO job_runs (instance, epoch) VALUES (%s, %s)", (self.name_, self.epoch or 0))

    def run(self):
        t0 = time.time()
        last_keep = 0.0
        with psycopg.connect(PG, autocommit=True) as c:
            while not self.stop.is_set():
                now = time.time() - t0
                if self.crash_at and now >= self.crash_at:
                    return                                           # process dies: no more keepalives
                if self.pause_at and self.pause_at <= now < self.pause_at + 0.2:
                    time.sleep(self.pause_s)                         # a long GC pause or a frozen VM: nothing runs, no keepalives
                    self.pause_at = None
                    now = time.time() - t0
                try:
                    if self.mode == "everyone":
                        self.run_job(c)
                    else:
                        if not self.leader:
                            self.campaign()
                        elif now - last_keep >= self.ttl / 4:
                            k = etcd("/v3/lease/keepalive", {"ID": self.lease}).get("result", {})
                            last_keep = now
                            if int(k.get("TTL", 0)) <= 0:            # our lease is gone: we are not the leader any more
                                self.leader, self.lease = False, None
                        if self.leader:
                            self.run_job(c, now)
                except Exception:
                    pass
                time.sleep(0.1)


def scenario(mode, secs=12, ttl=2, crash=None, pause=None, fenced=False, staggered=True, mid=None):
    setup()
    stop = threading.Event()
    insts = []
    for i, n in enumerate(("sched-a", "sched-b", "sched-c")):
        kw = {}
        if crash and n == crash[0]:
            kw["crash_at"] = crash[1]
        if pause and n == pause[0]:
            kw["pause_at"], kw["pause_s"] = pause[1], pause[2]
        if mid and n == mid[0]:
            kw["mid_job_pause"] = (mid[1], mid[2])
        insts.append(Instance(n, ttl, mode, stop, fenced=fenced, **kw))
    insts[0].start()
    time.sleep(0.3 if staggered else 0)
    for x in insts[1:]:
        x.start()
    time.sleep(secs)
    stop.set()
    for x in insts:
        x.join()
    with psycopg.connect(PG) as c:
        rows = c.execute("SELECT instance, epoch, extract(epoch from at) FROM job_runs ORDER BY at").fetchall()
    # overlap: 100 ms buckets in which runs came from more than one instance
    buckets = {}
    for inst, ep, at in rows:
        buckets.setdefault(int(float(at) * 10), set()).add(inst)
    overlap = sum(1 for s in buckets.values() if len(s) > 1)
    stale, top = 0, 0
    for inst, ep, at in rows:                          # accepted work from an epoch older than one already seen
        stale += ep < top
        top = max(top, ep)
    ats = [float(r[2]) for r in rows]
    gaps = [b - a for a, b in zip(ats, ats[1:])]
    return {"mode": mode, "ttl": ttl, "runs": len(rows), "expected_runs": int(secs * 10) * 5, "instances_that_ran": sorted({r[0] for r in rows}),
            "overlap_buckets": overlap, "stale_writes_accepted": stale, "max_gap_s": round(max(gaps), 2) if gaps else None,
            "elections": sum(x.elections_won for x in insts), "rejected_by_fence": sum(x.rejected for x in insts)}


if __name__ == "__main__":
    E = scenario("everyone")
    print(E, flush=True)
    L = scenario("lease", ttl=2)
    print(L, flush=True)
    F = {t: scenario("lease", ttl=t, crash=("sched-a", 4), secs=16) for t in (2, 5, 10)}   # etcd grants at least 2 s
    for v in F.values():
        print(v, flush=True)
    P = {t: scenario("lease", ttl=t, pause=("sched-a", 4, 3.0)) for t in (2, 5)}
    for v in P.values():
        print(v, flush=True)
    S = {"unfenced": scenario("lease", ttl=2, mid=("sched-a", 4, 4.0)), "fenced": scenario("lease", ttl=2, mid=("sched-a", 4, 4.0), fenced=True)}
    for v in S.values():
        print(v, flush=True)
    data.update({"everyone": E, "lease": L, "failover": {str(k): v for k, v in F.items()}, "pause": {str(k): v for k, v in P.items()}, "stale": S})
    report("No election: every instance runs the job", "the job runs three times as often", f"{E['runs']} runs in 12 s (expected ~{E['expected_runs']}), from {E['instances_that_ran']}",
           E["runs"] > 2.5 * E["expected_runs"] and len(E["instances_that_ran"]) == 3)
    report("etcd lease election", "one instance runs the job", f"{L['runs']} runs from {L['instances_that_ran']}; overlapping 100 ms windows {L['overlap_buckets']}",
           len(L["instances_that_ran"]) == 1 and L["overlap_buckets"] == 0 and L["elections"] == 1)
    report("Leader crashes: failover time follows the lease TTL", "a shorter TTL fails over faster",
           " · ".join(f"TTL {k}s → job paused {v['max_gap_s']} s" for k, v in F.items()),
           F[2]["max_gap_s"] < F[5]["max_gap_s"] < F[10]["max_gap_s"] and F[2]["max_gap_s"] > 1.5)
    report("A 3 s pause on a healthy leader", "a short TTL mistakes it for a crash and elects again; a long TTL rides it out",
           f"TTL 2 s: {P[2]['elections']} elections, runs from {P[2]['instances_that_ran']} · TTL 5 s: {P[5]['elections']} election(s), runs from {P[5]['instances_that_ran']}",
           P[2]["elections"] > P[5]["elections"] and len(P[5]["instances_that_ran"]) == 1)
    u, f = S["unfenced"], S["fenced"]
    report("Leader freezes mid-job for 4 s (TTL 2 s), unfenced", "it finishes its job after a new leader took over: work from two leaders",
           f"writes accepted from the old epoch after the new one: {u['stale_writes_accepted']}; runs from {u['instances_that_ran']}",
           u["stale_writes_accepted"] > 0 and len(u["instances_that_ran"]) > 1)
    report("Same, with the epoch checked by the database", "the stale leader's writes are rejected", f"rejected {f['rejected_by_fence']}; stale writes accepted {f['stale_writes_accepted']}",
           f["rejected_by_fence"] > 0 and f["stale_writes_accepted"] == 0)
    with open(__file__.replace("election.py", "election_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
