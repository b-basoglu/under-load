"""Dead letter queues, measured for Part 28. Redis 7.4 Streams (main stream, retry handling, DLQ stream) + PostgreSQL 17.

Domain: "ticket.updated" events consumed by the search indexer.
"""
import json
import random
import threading
import time

import psycopg
import redis

R = redis.Redis(port=56379)
PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class Transient(Exception):
    pass


class Permanent(Exception):
    pass


def reset(*streams):
    for s in streams:
        R.delete(s)
    R.xgroup_create(streams[0], "idx", id="0", mkstream=True)


# ---- 1. A poison message on an ordered stream ---------------------------------------------------
def poison(max_attempts, secs=5.0):
    """One consumer, strict order: it doesn't move past a message until it succeeds or gives up."""
    s, dlq = "ul:upd:poison", "ul:upd:poison:dlq"
    reset(s, dlq)
    for i in range(1000):
        R.xadd(s, {"ticket_id": i, "schema": 3 if i == 10 else 2, "body": "x"})
    t0, done, attempts = time.time(), 0, 0
    while time.time() - t0 < secs and done < 1000:
        got = R.xreadgroup("idx", "c", {s: "0"}, count=1)          # first: our own un-acked message, if any
        msgs = got[0][1] if got else []
        if not msgs:
            got = R.xreadgroup("idx", "c", {s: ">"}, count=1)
            msgs = got[0][1] if got else []
        if not msgs:
            break
        mid, f = msgs[0]
        tries = 0
        while True:
            tries += 1
            attempts += 1
            try:
                if int(f[b"schema"]) == 3:
                    raise Permanent("unknown schema version 3")
                time.sleep(0.001)
                break
            except Permanent as e:
                time.sleep(0.02)                        # backoff between attempts
                if max_attempts and tries >= max_attempts:
                    R.xadd(dlq, {**{k.decode(): v for k, v in f.items()}, "error": str(e), "attempts": tries, "source_id": mid})
                    break
                if time.time() - t0 > secs:
                    break
        if time.time() - t0 > secs:
            break
        R.xack(s, "idx", mid)
        done += 1
    return {"max_attempts": max_attempts, "processed": done, "attempts": attempts, "dlq": R.xlen(dlq), "seconds": round(time.time() - t0, 1)}


# ---- 2. Classifying failures ----------------------------------------------------------------------
def classify(policy, n=2000, seed=3):
    """5% of messages hit a transient error (DB timeout) on their first 1–2 attempts; 1% are permanently bad.
    policy: 'no-retry' | 'retry-all' (5 attempts, backoff) | 'classified' (retry transient, DLQ permanent at once)."""
    rnd = random.Random(seed)
    msgs = []
    for i in range(n):
        r = rnd.random()
        kind = "permanent" if r < 0.01 else ("transient" if r < 0.06 else "ok")
        msgs.append((i, kind, rnd.randint(1, 2)))
    dlq, wasted, total_attempts, recovered, backoff_s = [], 0, 0, 0, 0.0
    for i, kind, fails in msgs:
        for attempt in range(1, 6):
            total_attempts += 1
            try:
                if kind == "permanent":
                    raise Permanent("deserialize: unknown field 'priority_v2'")
                if kind == "transient" and attempt <= fails:
                    raise Transient("db timeout")
                if kind == "transient":
                    recovered += 1
                break
            except Permanent:
                if policy == "retry-all" and attempt < 5:
                    wasted += 1
                    backoff_s += 0.1 * 2 ** (attempt - 1)
                    continue
                dlq.append((i, kind))
                break
            except Transient:
                if policy == "no-retry" or attempt == 5:
                    dlq.append((i, kind))
                    break
                backoff_s += 0.1 * 2 ** (attempt - 1)
    return {"policy": policy, "dlq": len(dlq), "dlq_recoverable": sum(1 for _, k in dlq if k == "transient"),
            "dlq_permanent": sum(1 for _, k in dlq if k == "permanent"), "wasted_attempts": wasted, "attempts": total_attempts,
            "recovered_by_retry": recovered, "backoff_spent_s": round(backoff_s, 1),
            "permanent": sum(1 for _, k, _ in msgs if k == "permanent"), "transient": sum(1 for _, k, _ in msgs if k == "transient")}


# ---- 3. Replay: idempotent vs not ----------------------------------------------------------------
def replay(idempotent, n=500):
    """The indexer wrote the search document, then failed on the follow-up step (audit write) → DLQ.
    After the fix, the DLQ is replayed."""
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS search_docs, audit_log")
        c.execute("CREATE TABLE search_docs (id bigserial PRIMARY KEY, ticket_id int, version int)" if not idempotent else
                  "CREATE TABLE search_docs (id bigserial PRIMARY KEY, ticket_id int, version int, UNIQUE (ticket_id, version))")
        c.execute("CREATE TABLE audit_log (ticket_id int, version int)")
    dlq = "ul:upd:replay:dlq"
    R.delete(dlq)
    bug = True
    with psycopg.connect(PG, autocommit=True) as c:
        for t in range(n):                               # first pass: half the messages hit the audit bug after indexing
            ins = ("INSERT INTO search_docs (ticket_id, version) VALUES (%s, 1)" if not idempotent else
                   "INSERT INTO search_docs (ticket_id, version) VALUES (%s, 1) ON CONFLICT DO NOTHING")
            c.execute(ins, (t,))
            if bug and t % 2 == 0:
                R.xadd(dlq, {"ticket_id": t, "version": 1, "error": "audit_log: column 'actor' does not exist", "attempts": 3})
                continue
            c.execute("INSERT INTO audit_log VALUES (%s, 1)", (t,))
        replayed = 0
        for mid, f in R.xrange(dlq):                     # bug fixed; replay every DLQ message through the normal path
            t = int(f[b"ticket_id"])
            c.execute(ins, (t,))
            c.execute("INSERT INTO audit_log VALUES (%s, 1)", (t,))
            R.xdel(dlq, mid)
            replayed += 1
        dup = c.execute("SELECT count(*) FROM (SELECT ticket_id FROM search_docs GROUP BY ticket_id HAVING count(*) > 1) x").fetchone()[0]
    return {"idempotent": idempotent, "replayed": replayed, "duplicate_docs": dup, "dlq_left": R.xlen(dlq)}


# ---- 4. Replay speed vs live traffic --------------------------------------------------------------
def replay_load(replay_rate, backlog=1500, live_rate=150, secs=12, consumers=2, work=0.005):
    """Live traffic at 150/s; 2 consumers (~240/s total). A DLQ backlog of 1,500 is replayed into the main stream
    either all at once or throttled. Measures how long live messages wait."""
    s = f"ul:upd:live:{replay_rate}"
    reset(s)
    lat, stop = [], threading.Event()
    lock = threading.Lock()

    def worker(name):
        r = redis.Redis(port=56379)
        while not stop.is_set():
            got = r.xreadgroup("idx", name, {s: ">"}, count=10, block=100)
            for _, msgs in got or []:
                for mid, f in msgs:
                    time.sleep(work)
                    r.xack(s, "idx", mid)
                    if f[b"kind"] == b"live":
                        with lock:
                            lat.append((float(f[b"ts"]), time.time() - float(f[b"ts"])))
    ts = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(consumers)]
    for t in ts:
        t.start()
    t0, sent_live, sent_replay = time.time(), 0, 0
    while time.time() - t0 < secs:
        el = time.time() - t0
        p = R.pipeline()
        for _ in range(int(el * live_rate) - sent_live):
            p.xadd(s, {"kind": "live", "ts": time.time()})
            sent_live += 1
        if el >= 2:                                          # replay starts at t = 2 s
            want = backlog if replay_rate == 0 else min(backlog, int((el - 2) * replay_rate))
            for _ in range(want - sent_replay):
                p.xadd(s, {"kind": "replay", "ts": time.time()})
                sent_replay += 1
        p.execute()
        time.sleep(0.02)
    while time.time() - t0 < 90:                            # let consumers drain everything that was published
        g = R.xinfo_groups(s)[0]
        if int(g.get("lag") or 0) + int(g["pending"]) == 0:
            break
        time.sleep(0.1)
    stop.set()
    for t in ts:
        t.join()
    after = sorted(l for at, l in lat if at - t0 >= 2)
    series = []
    for k in range(int(secs * 2) + 2):
        w = [l for at, l in lat if k / 2 <= at - t0 < (k + 1) / 2]
        series.append({"t": k / 2, "max_wait_ms": round(max(w) * 1000) if w else None})
    return {"replay_rate": replay_rate, "live_p99_ms": round(after[int(len(after) * .99)] * 1000), "live_max_ms": round(after[-1] * 1000),
            "replayed": sent_replay, "series": series}


if __name__ == "__main__":
    P = {str(m): poison(m) for m in (0, 5)}
    for r in P.values():
        print(r)
    C = {p: classify(p) for p in ("no-retry", "retry-all", "classified")}
    for r in C.values():
        print(r)
    RP = {str(i): replay(i) for i in (False, True)}
    for r in RP.values():
        print(r)
    RL = {str(r): replay_load(r) for r in (0, 60)}
    for r in RL.values():
        print({k: v for k, v in r.items() if k != "series"})
    data.update({"poison": P, "classify": C, "replay": RP, "replay_load": RL})
    p0, p5 = P["0"], P["5"]
    report("Poison message, retried forever, on an ordered stream", "everything behind it stops",
           f"processed {p0['processed']} of 1,000 in {p0['seconds']} s, {p0['attempts']} attempts",
           p0["processed"] <= 10 and p0["attempts"] > 50 and p0["dlq"] == 0)
    report("Same, with 5 attempts then DLQ", "the stream keeps moving; the bad message is kept aside with its error",
           f"processed {p5['processed']} of 1,000; DLQ {p5['dlq']}",
           p5["processed"] == 1000 and p5["dlq"] == 1)
    nr, ra, cl = C["no-retry"], C["retry-all"], C["classified"]
    report("No retries", "transient failures land in the DLQ although a retry would have worked",
           f"DLQ {nr['dlq']}, of which recoverable {nr['dlq_recoverable']}",
           nr["dlq_recoverable"] > 50 and nr["dlq"] > nr["dlq_permanent"])
    report("Retry everything 5 times", "permanent failures waste attempts and backoff time",
           f"DLQ {ra['dlq']} (all permanent: {ra['dlq_permanent']}); wasted attempts {ra['wasted_attempts']}, backoff {ra['backoff_spent_s']} s",
           ra["wasted_attempts"] > 40 and ra["dlq_recoverable"] == 0 and ra["dlq_permanent"] == ra["permanent"])
    report("Classify: retry transient, dead-letter permanent immediately", "DLQ holds only real failures; no wasted attempts",
           f"DLQ {cl['dlq']} (recoverable {cl['dlq_recoverable']}), wasted {cl['wasted_attempts']}, backoff {cl['backoff_spent_s']} s",
           cl["dlq_recoverable"] == 0 and cl["wasted_attempts"] == 0 and cl["dlq"] == cl["permanent"]
           and cl["backoff_spent_s"] < ra["backoff_spent_s"])
    a, b = RP["False"], RP["True"]
    report("Blind replay through a non-idempotent path", "work done before the original failure is done again",
           f"replayed {a['replayed']}, duplicate search documents {a['duplicate_docs']}",
           a["duplicate_docs"] > 100 and a["duplicate_docs"] == a["replayed"])
    report("Replay through an idempotent path", "safe to replay",
           f"replayed {b['replayed']}, duplicates {b['duplicate_docs']}", b["duplicate_docs"] == 0)
    f0, f1 = RL["0"], RL["60"]
    report("Replaying 1,500 messages at once into the live stream", "live messages wait behind the replay",
           f"live wait P99 {f0['live_p99_ms']:,} ms, max {f0['live_max_ms']:,} ms",
           f0["live_p99_ms"] > 2000 and f0["replayed"] == 1500)
    report("Throttled replay (60/s, within spare capacity)", "live traffic barely notices; the replay takes longer",
           f"live wait P99 {f1['live_p99_ms']} ms, max {f1['live_max_ms']} ms; replayed {f1['replayed']} in the 10 s window",
           f1["live_p99_ms"] < 300 and f1["live_p99_ms"] * 5 < f0["live_p99_ms"])
    with open(__file__.replace("dlq.py", "dlq_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
