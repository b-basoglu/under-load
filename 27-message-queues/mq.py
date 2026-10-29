"""Message queues and delivery guarantees, measured for Part 27.

Broker: Redis 7.4 Streams with a consumer group (XREADGROUP / XACK / XAUTOCLAIM — the same ack-and-redeliver model as
SQS visibility timeouts, RabbitMQ acks and Kafka offset commits). Side effects: PostgreSQL 17.
"ticket.created" events: the consumer creates an SLA timer row and sends a welcome email (simulated provider).
The consumer "crashes" at a random point with 2% probability per message; a restarted consumer claims what was pending.
"""
import json
import random
import time
from collections import Counter

import psycopg
import redis

R = redis.Redis(port=56379)
PG = "host=localhost port=55432 user=postgres password=test dbname=postgres"
N, CRASH = 2000, 0.02
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class Crash(Exception):
    pass


def setup(stream):
    R.delete(stream)
    R.xgroup_create(stream, "sla", id="0", mkstream=True)
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS sla_timers, processed_messages, tickets_view")
        c.execute("CREATE TABLE sla_timers (id bigserial PRIMARY KEY, ticket_id int NOT NULL)")
        c.execute("CREATE TABLE processed_messages (message_id text PRIMARY KEY, at timestamptz DEFAULT now())")
        c.execute("CREATE TABLE tickets_view (ticket_id int PRIMARY KEY, status text, version int)")


def consume(stream, handle, seed):
    """Runs consumers until the stream is fully acknowledged. handle(conn, msg_id, fields, rnd) may raise Crash."""
    rnd = random.Random(seed)
    crashes, deliveries = 0, 0
    conn = psycopg.connect(PG)
    while True:
        _, claimed, _ = R.xautoclaim(stream, "sla", "c", min_idle_time=50, start_id="0-0", count=50)
        batch = claimed or (R.xreadgroup("sla", "c", {stream: ">"}, count=50) or [[None, []]])[0][1]
        if not batch:
            if R.xpending(stream, "sla")["pending"] == 0:
                break
            time.sleep(0.06)
            continue
        for mid, fields in batch:
            deliveries += 1
            try:
                handle(conn, mid.decode(), fields, rnd)
            except Crash:
                crashes += 1
                conn.close()                          # the process dies: open transaction rolled back, batch abandoned
                conn = psycopg.connect(PG)
                time.sleep(0.06)                      # restart; un-acked messages become claimable after 50 ms idle
                break
    conn.close()
    return crashes, deliveries


def produce(stream, n):
    p = R.pipeline()
    for i in range(n):
        p.xadd(stream, {"ticket_id": i})
    p.execute()


def sla_counts():
    with psycopg.connect(PG) as c:
        rows = c.execute("SELECT ticket_id, count(*) FROM sla_timers GROUP BY ticket_id").fetchall()
    per = dict(rows)
    return {"missing": sum(1 for i in range(N) if i not in per), "duplicated": sum(1 for v in per.values() if v > 1),
            "extra_rows": sum(v - 1 for v in per.values() if v > 1)}


def run_guarantee(mode, seed=1):
    stream = f"ul:tickets:{mode}"
    setup(stream)
    produce(stream, N)
    emails = Counter()
    provider_keys = set()

    def handle(conn, mid, f, rnd):
        tid = int(f[b"ticket_id"])
        roll = rnd.random() < CRASH
        if mode == "at-most-once":                       # ack first, then work
            R.xack(stream, "sla", mid)
            if roll:
                raise Crash
            conn.execute("INSERT INTO sla_timers (ticket_id) VALUES (%s)", (tid,)); conn.commit()
            emails[tid] += 1
        elif mode == "at-least-once":                    # work, then ack
            conn.execute("INSERT INTO sla_timers (ticket_id) VALUES (%s)", (tid,)); conn.commit()
            emails[tid] += 1
            if roll:
                raise Crash                              # crashed after the work, before the ack
            R.xack(stream, "sla", mid)
        elif mode in ("idempotent", "idempotent+key"):   # dedupe table in the same transaction as the DB effect
            new = conn.execute("INSERT INTO processed_messages (message_id) VALUES (%s) ON CONFLICT DO NOTHING RETURNING 1", (mid,)).fetchone()
            if new:
                conn.execute("INSERT INTO sla_timers (ticket_id) VALUES (%s)", (tid,))
                key = f"welcome-{mid}"
                if mode == "idempotent" or key not in provider_keys:   # the provider dedupes only when given a key
                    emails[tid] += 1
                    provider_keys.add(key)
            if roll:
                raise Crash                              # crashed after sending, before commit: the DB work rolls back
            conn.commit()
            R.xack(stream, "sla", mid)
    t0 = time.time()
    crashes, deliveries = consume(stream, handle, seed)
    s = sla_counts()
    r = {"mode": mode, "crashes": crashes, "deliveries": deliveries, **s,
         "emails_missing": sum(1 for i in range(N) if emails[i] == 0), "emails_duplicated": sum(1 for v in emails.values() if v > 1),
         "seconds": round(time.time() - t0, 1)}
    print(r, flush=True)
    return r


def run_ordering(mode, seed=2, tickets=300):
    """Each ticket gets 3 status events (open → pending → solved). 10% of handling attempts fail transiently;
    the consumer leaves the failed message un-acked and moves on; it is retried later (out of order)."""
    stream = f"ul:status:{mode}"
    setup(stream)
    p = R.pipeline()
    for t in range(tickets):
        for v, st in enumerate(("open", "pending", "solved"), 1):
            p.xadd(stream, {"ticket_id": t, "status": st, "version": v})
    p.execute()

    def handle(conn, mid, f, rnd):
        tid, st, v = int(f[b"ticket_id"]), f[b"status"].decode(), int(f[b"version"])
        if rnd.random() < 0.10:
            return                                       # transient failure: no ack → redelivered after the idle timeout
        if mode == "last-write-wins":
            conn.execute("INSERT INTO tickets_view VALUES (%s,%s,%s) ON CONFLICT (ticket_id) DO UPDATE SET status=excluded.status, version=excluded.version", (tid, st, v))
        else:                                            # apply only if newer than what is stored
            conn.execute("INSERT INTO tickets_view VALUES (%s,%s,%s) ON CONFLICT (ticket_id) DO UPDATE SET status=excluded.status, version=excluded.version "
                         "WHERE tickets_view.version < excluded.version", (tid, st, v))
        conn.commit()
        R.xack(stream, "sla", mid)
    consume(stream, handle, seed)
    with psycopg.connect(PG) as c:
        wrong = c.execute("SELECT count(*) FROM tickets_view WHERE status <> 'solved'").fetchone()[0]
        reverted = c.execute("SELECT status, count(*) FROM tickets_view WHERE status <> 'solved' GROUP BY 1").fetchall()
    r = {"mode": mode, "tickets": tickets, "wrong_final_status": wrong, "wrong_by_status": dict(reverted)}
    print(r, flush=True)
    return r


def run_lag(consumers, rate=400, secs=6, work=0.005):
    """Producer at 400 msgs/s; each consumer handles ~1/work msgs/s. Lag = messages not yet acknowledged."""
    import threading
    stream = f"ul:lag:{consumers}"
    setup(stream)
    stop = threading.Event()
    series = []

    def worker(name):
        r = redis.Redis(port=56379)
        while not stop.is_set():
            got = r.xreadgroup("sla", name, {stream: ">"}, count=10, block=100)
            for _, msgs in got or []:
                for mid, _ in msgs:
                    time.sleep(work)
                    r.xack(stream, "sla", mid)
    ts = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(consumers)]
    for t in ts:
        t.start()
    t0 = time.time()
    sent = 0
    while time.time() - t0 < secs:
        target = int((time.time() - t0) * rate)
        p = R.pipeline()
        for _ in range(target - sent):
            p.xadd(stream, {"x": 1})
        p.execute()
        sent = target
        info = R.xinfo_groups(stream)[0]
        series.append({"t": round(time.time() - t0, 2), "lag": int(info.get("lag") or 0) + int(info["pending"])})
        time.sleep(0.2)
    stop.set()
    for t in ts:
        t.join()
    return {"consumers": consumers, "final_lag": series[-1]["lag"], "series": series}


if __name__ == "__main__":
    G = {m: run_guarantee(m) for m in ("at-most-once", "at-least-once", "idempotent", "idempotent+key")}
    O = {m: run_ordering(m) for m in ("last-write-wins", "version-check")}
    L = {c: run_lag(c) for c in (1, 2, 4)}
    for c, r in L.items():
        print("lag", c, r["final_lag"])
    data.update({"guarantees": G, "ordering": O, "lag": {str(k): v for k, v in L.items()}})
    a, b, i, k = G["at-most-once"], G["at-least-once"], G["idempotent"], G["idempotent+key"]
    report("At-most-once: ack before processing", "a crash after the ack loses the message for good",
           f"{a['crashes']} crashes → {a['missing']} tickets without an SLA timer, 0 duplicates ({a['duplicated']})",
           a["missing"] > 20 and a["duplicated"] == 0 and a["missing"] == a["crashes"])
    report("At-least-once: ack after processing", "nothing is lost, but a crash between work and ack redelivers — duplicates",
           f"{b['crashes']} crashes → missing {b['missing']}, duplicated SLA timers {b['duplicated']}",
           b["missing"] == 0 and b["duplicated"] > 20 and b["deliveries"] > N)
    report("Idempotent consumer: processed-message table in the same transaction", "redeliveries become no-ops for the database",
           f"{i['crashes']} crashes → missing {i['missing']}, duplicated {i['duplicated']}",
           i["missing"] == 0 and i["duplicated"] == 0 and i["crashes"] > 20)
    report("…but the email is outside the transaction", "the external side effect is still duplicated unless the provider dedupes by key",
           f"duplicate emails: without key {i['emails_duplicated']}, with idempotency key {k['emails_duplicated']}",
           i["emails_duplicated"] > 20 and k["emails_duplicated"] == 0 and k["emails_missing"] == 0)
    lw, vc = O["last-write-wins"], O["version-check"]
    report("Retries reorder messages", "a retried old event overwrites a newer one",
           f"{lw['wrong_final_status']} of {lw['tickets']} tickets ended in the wrong status ({lw['wrong_by_status']})",
           lw["wrong_final_status"] > 20)
    report("Apply only if newer (version check)", "out-of-order delivery no longer corrupts state",
           f"wrong final status: {vc['wrong_final_status']}", vc["wrong_final_status"] == 0)
    report("Consumer lag as a capacity signal", "1 consumer (~120/s) falls behind a 400/s producer; more consumers keep up",
           f"lag after 6 s: 1 consumer {L[1]['final_lag']}, 2 consumers {L[2]['final_lag']}, 4 consumers {L[4]['final_lag']}",
           L[1]["final_lag"] > 500 and L[4]["final_lag"] < 100 and L[1]["final_lag"] > L[2]["final_lag"])
    with open(__file__.replace("mq.py", "mq_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
