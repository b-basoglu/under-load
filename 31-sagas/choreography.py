"""Choreographed sagas, measured for Part 31. Redis 7.4 Streams carry events; PostgreSQL 17 holds one database per service.

Same "upgrade to Premium Support" flow as saga.py, but with no orchestrator: each service reacts to events and emits
the next one.
  accounts:      upgrade.requested ──▶
  billing:       on upgrade.requested → charge → payment.charged
  entitlements:  on payment.charged → grant seats (15% fail) → seats.granted | seats.failed
  accounts:      on seats.granted → activate plan (5% fail) → plan.activated | plan.failed
  entitlements:  on plan.failed → revoke seats → seats.revoked
  billing:       on seats.failed | seats.revoked → refund → payment.refunded
"""
import json
import random
import sys
import uuid

import psycopg
import redis

BASE = "host=localhost port=55432 user=postgres password=test dbname="
DBS = ("billing", "entitlements", "accounts")
R = redis.Redis(port=56379)
STREAM = "ul:saga:events"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def setup():
    with psycopg.connect(BASE + "postgres", autocommit=True) as c:
        for d in DBS:
            c.execute(f"DROP DATABASE IF EXISTS {d} WITH (FORCE)")
            c.execute(f"CREATE DATABASE {d}")
    for d in DBS:
        with psycopg.connect(BASE + d, autocommit=True) as c:
            c.execute("CREATE TABLE outbox (id bigserial PRIMARY KEY, event jsonb, published boolean DEFAULT false)")
            c.execute("CREATE TABLE processed (event_id text PRIMARY KEY)")
    with psycopg.connect(BASE + "billing", autocommit=True) as c:
        c.execute("CREATE TABLE provider_ops (id bigserial PRIMARY KEY, saga uuid, kind text, idem_key text UNIQUE)")
    with psycopg.connect(BASE + "entitlements", autocommit=True) as c:
        c.execute("CREATE TABLE entitlements (saga uuid PRIMARY KEY, status text)")
    with psycopg.connect(BASE + "accounts", autocommit=True) as c:
        c.execute("CREATE TABLE plans (saga uuid PRIMARY KEY, plan text)")
    R.delete(STREAM)
    R.xgroup_create(STREAM, "billing", id="0", mkstream=True)
    R.xgroup_create(STREAM, "entitlements", id="0")
    R.xgroup_create(STREAM, "accounts", id="0")


class Crash(Exception):
    """The process dies here. Its open transaction is gone; the message it was handling is not acknowledged."""


class Service:
    """One service: consumes the events it cares about, does a local transaction, emits the next event.
    outbox=True: the next event is written in the same transaction and published by a relay.
    outbox=False: the event is published after commit — a crash in between loses it.
    idempotent=True: each event id is recorded in `processed` in the same transaction."""
    def __init__(self, name, rnd, outbox, idempotent, crash_p, dup_p):
        self.name, self.rnd, self.outbox, self.idem, self.crash_p, self.dup_p = name, rnd, outbox, idempotent, crash_p, dup_p
        self.c = psycopg.connect(BASE + name)
        self.lost, self.crashes, self.republished = 0, 0, 0
        self.fault = random.Random(rnd.random())                # crashes and duplicates drawn separately from business outcomes

    def emit(self, events):
        for ev in events:
            ev["id"] = str(uuid.uuid4())
        if self.outbox:
            for ev in events:
                self.c.execute("INSERT INTO outbox (event) VALUES (%s)", (json.dumps(ev),))
            self.c.commit()
            if self.fault.random() < self.crash_p:              # dies after commit, before acking: the event is safe in the outbox
                raise Crash()
        else:
            self.c.commit()
            if self.fault.random() < self.crash_p:              # dies after commit, before publishing
                self.lost += len(events)                      # on redelivery the processed-event check skips it: never published
                raise Crash()
            for ev in events:
                R.xadd(STREAM, {"e": json.dumps(ev)})

    def relay(self):
        rows = self.c.execute("SELECT id, event FROM outbox WHERE NOT published ORDER BY id FOR UPDATE SKIP LOCKED").fetchall()
        for _, ev in rows:
            R.xadd(STREAM, {"e": json.dumps(ev)})
        if rows and self.fault.random() < self.crash_p:         # relay dies after publishing, before marking: all of them go out again
            self.c.rollback()
            self.crashes += 1
            self.republished += len(rows)
            return
        if rows:
            self.c.execute("UPDATE outbox SET published = true WHERE id = ANY(%s)", ([r[0] for r in rows],))
        self.c.commit()

    def handle(self, ev):
        if self.idem:
            new = self.c.execute("INSERT INTO processed VALUES (%s) ON CONFLICT DO NOTHING RETURNING 1", (ev["id"],)).fetchone()
            if not new:
                self.c.commit()
                return
        out = self.react(ev)
        if out is None:
            self.c.commit()
        else:
            self.emit(out)

    def poll(self):
        n = 0
        for start in ("0", ">"):                              # first what a crash left unacknowledged, then new messages
            got = R.xreadgroup(self.name, "w", {STREAM: start}, count=100)
            for _, msgs in got or []:
                for mid, f in msgs:
                    ev = json.loads(f[b"e"])
                    n += 1
                    try:
                        self.handle(ev)
                        if self.fault.random() < self.dup_p:    # at-least-once: the same event delivered again
                            self.handle(ev)
                    except Crash:
                        self.c.rollback()
                        self.crashes += 1
                        return n                              # restart: unacked messages are redelivered from the pending list
                    R.xack(STREAM, self.name, mid)
        return n


class Billing(Service):
    def react(self, ev):
        s = ev["saga"]
        if ev["type"] == "upgrade.requested":
            key = f"charge-{s}" if self.idem else None
            self.c.execute("INSERT INTO provider_ops (saga, kind, idem_key) VALUES (%s, 'charge', %s) ON CONFLICT DO NOTHING", (s, key))
            return [{"type": "payment.charged", "saga": s}]
        if ev["type"] in ("seats.failed", "seats.revoked"):
            key = f"refund-{s}" if self.idem else None
            self.c.execute("INSERT INTO provider_ops (saga, kind, idem_key) VALUES (%s, 'refund', %s) ON CONFLICT DO NOTHING", (s, key))
            return [{"type": "payment.refunded", "saga": s}]


class Entitlements(Service):
    def react(self, ev):
        s = ev["saga"]
        if ev["type"] == "payment.charged":
            if self.rnd.random() < 0.15:
                return [{"type": "seats.failed", "saga": s}]
            self.c.execute("INSERT INTO entitlements VALUES (%s, 'active') ON CONFLICT DO NOTHING", (s,))
            return [{"type": "seats.granted", "saga": s}]
        if ev["type"] == "plan.failed":
            self.c.execute("UPDATE entitlements SET status = 'revoked' WHERE saga = %s", (s,))
            return [{"type": "seats.revoked", "saga": s}]


class Accounts(Service):
    def react(self, ev):
        s = ev["saga"]
        if ev["type"] == "seats.granted":
            if self.rnd.random() < 0.05:
                return [{"type": "plan.failed", "saga": s}]
            self.c.execute("INSERT INTO plans VALUES (%s, 'premium') ON CONFLICT DO NOTHING", (s,))
            return [{"type": "plan.activated", "saga": s}]


def audit(sagas):
    with psycopg.connect(BASE + "billing") as b, psycopg.connect(BASE + "entitlements") as e, psycopg.connect(BASE + "accounts") as a:
        ops = b.execute("SELECT saga::text, kind FROM provider_ops").fetchall()
        ents = dict((str(k), v) for k, v in e.execute("SELECT saga, status FROM entitlements").fetchall())
        plans = {str(r[0]) for r in a.execute("SELECT saga FROM plans").fetchall()}
    charges, refunds = {}, {}
    for s, k in ops:
        (charges if k == "charge" else refunds)[s] = (charges if k == "charge" else refunds).get(s, 0) + 1
    paid_no_plan = sum(1 for s in sagas if charges.get(s, 0) > refunds.get(s, 0) and s not in plans)
    seats_no_plan = sum(1 for s in sagas if ents.get(s) == "active" and s not in plans)
    double_charged = sum(1 for s in sagas if charges.get(s, 0) > 1)
    double_refunded = sum(1 for s in sagas if refunds.get(s, 0) > 1)
    return {"completed": len(plans & set(sagas)), "charged_without_plan": paid_no_plan, "seats_without_plan": seats_no_plan,
            "double_charged": double_charged, "double_refunded": double_refunded}


def run(label, n=1000, outbox=True, idempotent=True, crash_p=0.0, dup_p=0.0, seed=1):
    setup()
    rnd = random.Random(seed)
    svcs = [Billing("billing", random.Random(seed + 1), outbox, idempotent, crash_p, dup_p),
            Entitlements("entitlements", random.Random(seed + 2), outbox, idempotent, crash_p, dup_p),
            Accounts("accounts", random.Random(seed + 3), outbox, idempotent, crash_p, dup_p)]
    sagas = [str(uuid.uuid4()) for _ in range(n)]
    for s in sagas:                                           # the UI publishes the requests
        R.xadd(STREAM, {"e": json.dumps({"type": "upgrade.requested", "saga": s, "id": str(uuid.uuid4())})})
    rounds, idle = 0, 0
    while idle < 3 and rounds < 500:
        rounds += 1
        work = 0
        for sv in svcs:
            work += sv.poll()
            if outbox:
                sv.relay()
        idle = idle + 1 if work == 0 else 0
    # where is each saga? With choreography there is no single place to ask: reconstruct from all three databases.
    out = {"label": label, **audit(sagas), "events_lost": sum(sv.lost for sv in svcs), "crashes": sum(sv.crashes for sv in svcs),
           "events_published_twice": sum(sv.republished for sv in svcs), "events_on_stream": R.xlen(STREAM)}
    print(out, flush=True)
    return out


def tracing_cost():
    """How many places must be queried to find one saga's state: orchestrated (saga table) vs choreographed (every service)."""
    return {"orchestrated_queries": 1, "choreographed_queries": len(DBS), "choreographed_note": "or one event log with a correlation id"}


if __name__ == "__main__":
    H = run("happy path")
    L = run("crash after commit, no outbox", outbox=False, crash_p=0.03)
    O = run("crash after commit, outbox", outbox=True, crash_p=0.03)
    D = run("duplicate delivery, not idempotent", idempotent=False, dup_p=0.05)
    I = run("duplicate delivery, idempotent", idempotent=True, dup_p=0.05)
    data.update({"happy": H, "lost_no_outbox": L, "outbox": O, "dups_plain": D, "dups_idempotent": I, "tracing": tracing_cost()})
    report("Choreographed saga, no failures in the plumbing", "services reacting to events reach a consistent end state",
           f"completed {H['completed']}, charged without plan {H['charged_without_plan']}, seats without plan {H['seats_without_plan']}",
           H["charged_without_plan"] == 0 and H["seats_without_plan"] == 0 and H["completed"] > 700)
    report("A service commits, then dies before publishing its event", "the next service never hears about it; the saga stops silently",
           f"{L['events_lost']} events lost → charged without plan {L['charged_without_plan']}, seats without plan {L['seats_without_plan']}",
           L["events_lost"] > 20 and L["charged_without_plan"] + L["seats_without_plan"] > 20)
    report("Same crashes with an outbox per service", "the event is committed with the change; the relay publishes it later — and sometimes twice",
           f"{O['crashes']} crashes (handlers and relays), {O['events_published_twice']} events published twice; charged without plan {O['charged_without_plan']}, "
           f"seats without plan {O['seats_without_plan']}, double charged {O['double_charged']}, completed {O['completed']}",
           O["crashes"] > 20 and O["events_published_twice"] > 20
           and O["charged_without_plan"] == 0 and O["seats_without_plan"] == 0 and O["double_charged"] == 0)
    report("Events delivered twice (5%), handlers not idempotent", "a duplicate payment.charged / seats.failed repeats the side effect",
           f"double charged {D['double_charged']}, double refunded {D['double_refunded']}",
           D["double_charged"] > 20 and D["double_refunded"] > 5)
    report("Same duplicates, idempotent handlers", "processed-event table + idempotency keys make redelivery harmless",
           f"double charged {I['double_charged']}, double refunded {I['double_refunded']}",
           I["double_charged"] == 0 and I["double_refunded"] == 0)
    with open(__file__.replace("choreography.py", "choreography_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
