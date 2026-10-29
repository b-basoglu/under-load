"""Sagas, measured for Part 31. PostgreSQL 17: three databases, one per service (billing, entitlements, accounts).

Flow "upgrade to Premium Support": 1. billing charges the card (external payment provider, simulated as a table)
2. entitlements grants premium agent seats (fails for ~15%: seat limit) 3. accounts activates the plan (fails ~5%).
Each step is a local transaction in its own database. There is no transaction across them.
"""
import json
import random
import threading
import time
import uuid

import psycopg

BASE = "host=localhost port=55432 user=postgres password=test dbname="
DBS = ("billing", "entitlements", "accounts")
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


class Crash(Exception):
    pass


class StepFailed(Exception):
    pass


class Timeout(Exception):
    pass


def setup():
    with psycopg.connect(BASE + "postgres", autocommit=True) as c:
        for d in DBS:
            c.execute(f"DROP DATABASE IF EXISTS {d} WITH (FORCE)")
            c.execute(f"CREATE DATABASE {d}")
    with psycopg.connect(BASE + "billing", autocommit=True) as c:
        c.execute("CREATE TABLE provider_ops (id bigserial PRIMARY KEY, account int, kind text, amount int, idem_key text UNIQUE)")
    with psycopg.connect(BASE + "entitlements", autocommit=True) as c:
        c.execute("CREATE TABLE entitlements (saga_id uuid PRIMARY KEY, account int, seats int, status text)")
    with psycopg.connect(BASE + "accounts", autocommit=True) as c:
        c.execute("CREATE TABLE plans (saga_id uuid PRIMARY KEY, account int, plan text)")
        c.execute("CREATE TABLE sagas (id uuid PRIMARY KEY, account int, state text, updated_at timestamptz DEFAULT now())")


class Services:
    """Each method is one local transaction in one service's database."""
    def __init__(self, rnd, idempotent=True, timeout_p=0.0):
        self.rnd, self.idem, self.timeout_p = rnd, idempotent, timeout_p
        self.c = {d: psycopg.connect(BASE + d, autocommit=True) for d in DBS}

    def charge(self, saga, account):
        key = f"charge-{saga}" if self.idem else None
        self.c["billing"].execute("INSERT INTO provider_ops (account, kind, amount, idem_key) VALUES (%s, 'charge', 4900, %s) ON CONFLICT (idem_key) DO NOTHING", (account, key))
        if self.rnd.random() < self.timeout_p:
            raise Timeout                              # the provider charged, but the response never arrived

    def refund(self, saga, account):
        key = f"refund-{saga}" if self.idem else None
        self.c["billing"].execute("INSERT INTO provider_ops (account, kind, amount, idem_key) VALUES (%s, 'refund', -4900, %s) ON CONFLICT (idem_key) DO NOTHING", (account, key))
        if self.rnd.random() < self.timeout_p:
            raise Timeout

    def grant(self, saga, account, status="active"):
        if self.rnd.random() < 0.15:
            raise StepFailed("seat limit reached")
        self.c["entitlements"].execute("INSERT INTO entitlements VALUES (%s, %s, 5, %s) ON CONFLICT (saga_id) DO NOTHING", (saga, account, status))

    def revoke(self, saga):
        self.c["entitlements"].execute("UPDATE entitlements SET status = 'revoked' WHERE saga_id = %s", (saga,))

    def confirm_grant(self, saga):
        self.c["entitlements"].execute("UPDATE entitlements SET status = 'active' WHERE saga_id = %s AND status = 'pending'", (saga,))

    def activate(self, saga, account):
        time.sleep(0.01)                               # activation calls another system: ~10 ms between grant and outcome
        if self.rnd.random() < 0.05:
            raise StepFailed("plan activation rejected")
        self.c["accounts"].execute("INSERT INTO plans VALUES (%s, %s, 'premium') ON CONFLICT DO NOTHING", (saga, account))

    def save(self, saga, account, state):
        self.c["accounts"].execute("INSERT INTO sagas (id, account, state) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET state = excluded.state, updated_at = now()",
                                   (saga, account, state))


def call(fn, *a, retries=5):
    """Retry a step on timeout (the outcome of a timed-out call is unknown)."""
    for i in range(retries):
        try:
            return fn(*a)
        except Timeout:
            continue
    raise Timeout


def run_saga(s, saga, account, mode, crash_p, rnd, pending=False):
    def maybe_crash():
        if rnd.random() < crash_p:
            raise Crash
    durable = mode == "durable"
    if mode == "no-compensation":
        call(s.charge, saga, account)
        s.grant(saga, account)
        s.activate(saga, account)
        return
    try:
        if durable: s.save(saga, account, "charging")
        maybe_crash()
        call(s.charge, saga, account)
        if durable: s.save(saga, account, "granting")
        maybe_crash()
        s.grant(saga, account, "pending" if pending else "active")
        if durable: s.save(saga, account, "activating")
        maybe_crash()
        s.activate(saga, account)
        if pending: s.confirm_grant(saga)
        if durable: s.save(saga, account, "done")
    except StepFailed:
        compensate(s, saga, account, durable, rnd, crash_p)


def compensate(s, saga, account, durable, rnd, crash_p):
    if durable: s.save(saga, account, "compensating")
    if rnd.random() < crash_p:
        raise Crash
    s.revoke(saga)
    call(s.refund, saga, account)
    if durable: s.save(saga, account, "compensated")


def recover(s):
    """After a crash, a durable orchestrator finds unfinished sagas and drives them forward or back.
    Every step is idempotent (keys / ON CONFLICT), so redoing one is safe."""
    rnd = random.Random(99)
    rows = s.c["accounts"].execute("SELECT id, account, state FROM sagas WHERE state NOT IN ('done', 'compensated')").fetchall()
    for saga, account, state in rows:
        saga = str(saga)
        try:
            if state == "compensating":
                compensate(s, saga, account, True, rnd, 0)
                continue
            if state == "charging":
                call(s.charge, saga, account)
                s.save(saga, account, "granting")
                state = "granting"
            if state == "granting":
                s.grant(saga, account)
                s.save(saga, account, "activating")
                state = "activating"
            if state == "activating":
                s.activate(saga, account)
                s.save(saga, account, "done")
        except StepFailed:
            compensate(s, saga, account, True, rnd, 0)
    return len(rows)


def audit():
    """Compare the three databases, per saga."""
    with psycopg.connect(BASE + "billing") as b, psycopg.connect(BASE + "entitlements") as e, psycopg.connect(BASE + "accounts") as a:
        money = {}
        charges, refunds = {}, {}
        for acc, kind, amt, key in b.execute("SELECT account, kind, amount, idem_key FROM provider_ops"):
            pass
        ops = b.execute("SELECT kind, idem_key, account FROM provider_ops").fetchall()
        ents = {str(r[0]): r[1] for r in e.execute("SELECT saga_id, status FROM entitlements")}
        plans = {str(r[0]) for r in a.execute("SELECT saga_id FROM plans")}
    return ops, ents, plans


def run(mode, n=1000, crash_p=0.0, timeout_p=0.0, idempotent=True, pending=False, seed=1):
    setup()
    rnd = random.Random(seed)
    s = Services(random.Random(seed + 1), idempotent, timeout_p)
    sagas = []
    crashes = 0
    seen_then_revoked = set()
    stop = threading.Event()
    reader_seen = set()

    def reader():                                       # a reporting job counting active premium seats, every 5 ms
        with psycopg.connect(BASE + "entitlements", autocommit=True) as c:
            while not stop.is_set():
                for (sid,) in c.execute("SELECT saga_id FROM entitlements WHERE status = 'active'"):
                    reader_seen.add(str(sid))
                time.sleep(0.005)
    rt = threading.Thread(target=reader, daemon=True)
    rt.start()
    for i in range(n):
        saga, account = str(uuid.uuid4()), rnd.randint(1, 300)
        sagas.append((saga, account))
        try:
            run_saga(s, saga, account, mode, crash_p, rnd, pending)
        except (Crash, Timeout):
            crashes += 1                               # the orchestrator process died mid-saga
        except StepFailed:
            pass                                       # no-compensation mode: the failure is simply left as is
    recovered = recover(s) if mode == "durable" else 0
    stop.set()
    rt.join()
    with psycopg.connect(BASE + "billing") as b, psycopg.connect(BASE + "entitlements") as e, psycopg.connect(BASE + "accounts") as a:
        ops = b.execute("SELECT kind, account FROM provider_ops").fetchall()
        ents = {str(r[0]): r[1] for r in e.execute("SELECT saga_id, status FROM entitlements")}
        plans = {str(r[0]) for r in a.execute("SELECT saga_id FROM plans")}
        stuck = a.execute("SELECT count(*) FROM sagas WHERE state NOT IN ('done','compensated')").fetchone()[0]
    # per-saga ledger: we attribute provider ops by idempotency key when present, else by order (non-idempotent runs)
    with psycopg.connect(BASE + "billing") as b:
        rows = b.execute("SELECT kind, idem_key FROM provider_ops").fetchall()
    charged_no_service = entitled_no_plan = 0
    net = {}
    if idempotent:
        for kind, key in rows:
            sid = key.split("-", 1)[1]
            net[sid] = net.get(sid, 0) + (1 if kind == "charge" else -1)
        for saga, _ in sagas:
            paid = net.get(saga, 0) > 0
            has_plan = saga in plans
            if paid and not has_plan:
                charged_no_service += 1
            if ents.get(saga) == "active" and saga not in plans:
                entitled_no_plan += 1
    charges = sum(1 for k, _ in ops if k == "charge")
    refunds = sum(1 for k, _ in ops if k == "refund")
    completed = len(plans)
    compensated = sum(1 for v in ents.values() if v == "revoked")
    dirty = len({sid for sid in reader_seen if ents.get(sid) == "revoked"})
    out = {"mode": mode, "crash_p": crash_p, "timeout_p": timeout_p, "idempotent": idempotent, "pending": pending, "sagas": n,
           "completed": completed, "charges": charges, "refunds": refunds, "crashes": crashes, "recovered": recovered, "stuck": stuck,
           "charged_without_plan": charged_no_service, "entitled_without_plan": entitled_no_plan,
           "extra_charges": charges - n if mode != "no-compensation" else None, "revoked": compensated, "reader_saw_then_revoked": dirty}
    if mode == "no-compensation":
        out["charged_without_plan"] = charges - completed
    if not idempotent:
        out["extra_charges"] = charges - len({s for s, _ in sagas})
        out["extra_refunds"] = refunds - (n - completed)
    print(out, flush=True)
    return out


if __name__ == "__main__":
    R = {
        "no-compensation": run("no-compensation"),
        "orchestrated": run("in-memory"),
        "crash-in-memory": run("in-memory", crash_p=0.03),
        "crash-durable": run("durable", crash_p=0.03),
        "timeout-no-key": run("durable", timeout_p=0.05, idempotent=False),
        "timeout-key": run("durable", timeout_p=0.05),
        "pending": run("durable", pending=True),
    }
    data.update(R)
    nc, o, cm, cd = R["no-compensation"], R["orchestrated"], R["crash-in-memory"], R["crash-durable"]
    report("Calling the three services in sequence, no compensation", "a failure after the charge leaves customers paying for nothing",
           f"{nc['charged_without_plan']} of {nc['sagas']} customers charged without the plan",
           nc["charged_without_plan"] > 100 and nc["refunds"] == 0)
    report("Orchestrated saga with compensations", "failed steps are undone: refund + revoke",
           f"completed {o['completed']}, compensated {o['revoked']} entitlement(s) and refunded {o['refunds']}; charged without plan {o['charged_without_plan']}",
           o["charged_without_plan"] == 0 and o["refunds"] > 100 and o["entitled_without_plan"] == 0)
    report("Orchestrator crashes mid-saga, state only in memory", "sagas stop half-done and nobody resumes them",
           f"{cm['crashes']} crashes → {cm['charged_without_plan']} charged without plan, {cm['entitled_without_plan']} seats granted without plan",
           cm["charged_without_plan"] > 20 and cm["crashes"] > 20 and cm["recovered"] == 0)
    report("Orchestrator with durable saga state + recovery", "unfinished sagas are found and driven forward or back",
           f"{cd['crashes']} crashes, {cd['recovered']} recovered → charged without plan {cd['charged_without_plan']}, stuck {cd['stuck']}",
           cd["charged_without_plan"] == 0 and cd["stuck"] == 0 and cd["recovered"] == cd["crashes"])
    tn, tk = R["timeout-no-key"], R["timeout-key"]
    report("Provider timeouts retried without an idempotency key", "a timed-out charge that actually succeeded is charged again",
           f"extra charges {tn['extra_charges']}, extra refunds {tn.get('extra_refunds')}",
           tn["extra_charges"] > 20)
    report("Same retries with idempotency keys", "retrying is safe", f"extra charges {tk['extra_charges']}; charged without plan {tk['charged_without_plan']}",
           tk["extra_charges"] == 0 and tk["charged_without_plan"] == 0)
    p = R["pending"]
    report("No isolation: other readers see intermediate state", "a report counts seats that are later revoked",
           f"seen as active, then revoked: {o['reader_saw_then_revoked']} (plain) vs {p['reader_saw_then_revoked']} (seats 'pending' until the saga completes)",
           o["reader_saw_then_revoked"] > 20 and p["reader_saw_then_revoked"] == 0)
    with open(__file__.replace("saga.py", "saga_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
