"""Idempotency keys for Part 14, against PostgreSQL 17.

The "API" is a function create_ticket(); a lost response is simulated by the client discarding the result
and retrying. A crash is simulated by raising between steps.
"""
import hashlib
import json
import threading
import time
import uuid

import psycopg

PG = "host=localhost port=55432 user=postgres password=test"
results, data = [], {}
emails = []          # side effect outside the database
charges = []         # a payment provider that supports idempotency keys
provider_keys = {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def db():
    return psycopg.connect(PG, autocommit=True)


def setup():
    with db() as k:
        k.execute("DROP TABLE IF EXISTS idempotency_keys, tickets")
        k.execute("CREATE TABLE tickets (id serial PRIMARY KEY, workspace_id int, subject text, created_at timestamptz DEFAULT now())")
        k.execute("""CREATE TABLE idempotency_keys (
                       workspace_id int, key text, request_hash text NOT NULL,
                       status text NOT NULL, response jsonb, created_at timestamptz DEFAULT now(),
                       PRIMARY KEY (workspace_id, key))""")
    emails.clear(); charges.clear(); provider_keys.clear()


def count_tickets(subject):
    with db() as k:
        return k.execute("SELECT count(*) FROM tickets WHERE subject = %s", (subject,)).fetchone()[0]


def h(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


# ------------------------------------------------------------------ naive endpoint
def create_naive(body):
    with db() as k:
        tid = k.execute("INSERT INTO tickets (workspace_id, subject) VALUES (7, %s) RETURNING id", (body["subject"],)).fetchone()[0]
    emails.append(tid)
    return {"id": tid}


# ------------------------------------------------------------------ idempotent endpoint
class Conflict(Exception):
    pass


class Mismatch(Exception):
    pass


def create_idempotent(body, key, crash_after_insert=False, wait_for_peer=None):
    """Claim the key, do the work and record the response in ONE transaction."""
    with db() as k:
        with k.transaction():
            claimed = k.execute("""INSERT INTO idempotency_keys (workspace_id, key, request_hash, status)
                                   VALUES (7, %s, %s, 'processing') ON CONFLICT DO NOTHING RETURNING key""",
                                (key, h(body))).fetchone()
            if not claimed:
                # someone already has (or had) this key: wait for their row lock, then read the outcome
                row = k.execute("SELECT request_hash, status, response FROM idempotency_keys WHERE workspace_id = 7 AND key = %s",
                                (key,)).fetchone()
                if row[0] != h(body):
                    raise Mismatch("same key, different request")
                if row[1] != "done":
                    raise Conflict("request with this key is still being processed")
                return row[2] | {"replayed": True}
            if wait_for_peer:
                wait_for_peer()
            tid = k.execute("INSERT INTO tickets (workspace_id, subject) VALUES (7, %s) RETURNING id", (body["subject"],)).fetchone()[0]
            if crash_after_insert:
                raise RuntimeError("process crashed")
            resp = {"id": tid}
            k.execute("UPDATE idempotency_keys SET status = 'done', response = %s WHERE workspace_id = 7 AND key = %s",
                      (json.dumps(resp), key))
    return resp


def create_two_steps(body, key, crash_between=False):
    """Anti-pattern: side effect and key record in separate transactions."""
    with db() as k:
        row = k.execute("SELECT response FROM idempotency_keys WHERE workspace_id = 7 AND key = %s", (key,)).fetchone()
        if row:
            return row[0]
        tid = k.execute("INSERT INTO tickets (workspace_id, subject) VALUES (7, %s) RETURNING id", (body["subject"],)).fetchone()[0]
        if crash_between:
            raise RuntimeError("process crashed after creating the ticket")
        k.execute("INSERT INTO idempotency_keys VALUES (7, %s, %s, 'done', %s)", (key, h(body), json.dumps({"id": tid})))
        return {"id": tid}


def check_then_insert(body, key, barrier):
    """Anti-pattern: SELECT to check the key, then INSERT the ticket — no unique claim."""
    with db() as k:
        seen = k.execute("SELECT 1 FROM idempotency_keys WHERE workspace_id = 7 AND key = %s", (key,)).fetchone()
        barrier.wait()   # both requests have checked; neither found the key
        if seen:
            return None
        tid = k.execute("INSERT INTO tickets (workspace_id, subject) VALUES (7, %s) RETURNING id", (body["subject"],)).fetchone()[0]
        k.execute("INSERT INTO idempotency_keys VALUES (7, %s, %s, 'done', %s) ON CONFLICT DO NOTHING",
                  (key, h(body), json.dumps({"id": tid})))
        return tid


# ------------------------------------------------------------------ fake payment provider
def provider_charge(amount, idem_key=None):
    if idem_key and idem_key in provider_keys:
        return provider_keys[idem_key]
    cid = f"ch_{len(charges) + 1}"
    charges.append((cid, amount))
    if idem_key:
        provider_keys[idem_key] = cid
    return cid


def tests():
    setup()
    # 1. lost response, retry, no idempotency
    body = {"subject": "Refund for order 1182"}
    create_naive(body)            # response lost on the way back
    create_naive(body)            # client retries
    n, e = count_tickets(body["subject"]), len(emails)
    data["naive_retry"] = {"tickets": n, "emails": e}
    report("Response lost, client retries, no idempotency", "the retry creates a second ticket and a second email",
           f"tickets={n}, emails={e}", n == 2 and e == 2)

    # 2. with an idempotency key
    setup()
    body = {"subject": "Refund for order 1183"}
    key = str(uuid.uuid4())
    first = create_idempotent(body, key)
    retry = create_idempotent(body, key)
    n = count_tickets(body["subject"])
    data["idempotent_retry"] = {"tickets": n, "first": first, "retry": retry}
    report("Same retry with an Idempotency-Key", "the retry gets the original response; one ticket exists",
           f"tickets={n}; first={first}; retry={retry}", n == 1 and retry["id"] == first["id"] and retry.get("replayed"))

    # 3. new key on every retry (client bug)
    setup()
    body = {"subject": "Refund for order 1184"}
    create_idempotent(body, str(uuid.uuid4())); create_idempotent(body, str(uuid.uuid4()))
    n = count_tickets(body["subject"])
    data["new_key_per_retry"] = {"tickets": n}
    report("Client generates a new key for each retry", "the server can't tell it's the same operation: two tickets",
           f"tickets={n}", n == 2)

    # 4. payload mismatch
    setup()
    key = str(uuid.uuid4())
    create_idempotent({"subject": "Printer broken"}, key)
    try:
        create_idempotent({"subject": "Printer broken, also the scanner"}, key); res = "accepted"
    except Mismatch:
        res = "rejected (422)"
    data["mismatch"] = res
    report("Same key, different request body", "rejected instead of silently returning the first response", res, res.startswith("rejected"))

    # 5. concurrent duplicates: check-then-insert vs unique claim
    setup()
    body = {"subject": "Double click 1"}
    key = str(uuid.uuid4())
    bar = threading.Barrier(2)
    ts = [threading.Thread(target=check_then_insert, args=(body, key, bar)) for _ in range(2)]
    for t in ts: t.start()
    for t in ts: t.join()
    naive_n = count_tickets(body["subject"])
    body2 = {"subject": "Double click 2"}
    key2 = str(uuid.uuid4())
    outcome = {}
    started = threading.Event()

    def slow_first():
        outcome["a"] = create_idempotent(body2, key2, wait_for_peer=lambda: (started.set(), time.sleep(0.6)))

    def second():
        started.wait()
        t0 = time.time()
        try:
            outcome["b"] = create_idempotent(body2, key2)
        except Conflict as e:
            outcome["b"] = "conflict"
        outcome["b_waited"] = round(time.time() - t0, 2)
    ta, tb = threading.Thread(target=slow_first), threading.Thread(target=second)
    ta.start(); tb.start(); ta.join(); tb.join()
    claim_n = count_tickets(body2["subject"])
    data["concurrent"] = {"check_then_insert_tickets": naive_n, "unique_claim_tickets": claim_n, "second_request": str(outcome["b"]),
                          "second_waited_s": outcome["b_waited"]}
    second_ok = outcome["b"] == "conflict" or (
        isinstance(outcome["b"], dict) and outcome["b"].get("replayed")
    ) or (isinstance(outcome["b"], dict) and outcome["b"].get("id") == outcome["a"].get("id"))
    report(
        "Two requests with the same key at the same moment",
        "check-then-insert lets both through; a unique claim (INSERT … ON CONFLICT) lets one through and replays to the other",
        f"check-then-insert: {naive_n} tickets; unique claim: {claim_n} ticket — second request waited {outcome['b_waited']} s "
        f"and got {outcome['b']}",
        naive_n == 2 and claim_n == 1 and second_ok and outcome["b_waited"] >= 0.4,
    )

    # 6. crash after the side effect
    setup()
    body = {"subject": "Crash test A"}
    key = str(uuid.uuid4())
    try:
        create_two_steps(body, key, crash_between=True)
    except RuntimeError:
        pass
    create_two_steps(body, key)  # client retries
    two_step_n = count_tickets(body["subject"])
    body = {"subject": "Crash test B"}
    key = str(uuid.uuid4())
    try:
        create_idempotent(body, key, crash_after_insert=True)
    except RuntimeError:
        pass
    create_idempotent(body, key)
    one_tx_n = count_tickets(body["subject"])
    data["crash"] = {"separate_transactions_tickets": two_step_n, "same_transaction_tickets": one_tx_n}
    report("Server crashes after creating the ticket, before recording the key",
           "with separate steps the retry creates a duplicate; in one transaction the crash rolls both back",
           f"separate steps: {two_step_n} tickets; one transaction: {one_tx_n} ticket", two_step_n == 2 and one_tx_n == 1)

    # 7. external side effect: pass the key downstream
    charges.clear(); provider_keys.clear()
    for _ in range(2):
        provider_charge(4900)                         # timeout, retry — no key
    no_key = len(charges)
    charges.clear(); provider_keys.clear()
    k = "refund-1182-attempt"
    for _ in range(2):
        provider_charge(4900, idem_key=k)            # same key passed to the provider
    with_key = len(charges)
    data["provider"] = {"charges_without_key": no_key, "charges_with_key": with_key}
    report("Calling a payment provider twice after a timeout", "without forwarding a key: two charges; with the same key: one",
           f"charges: without key={no_key}, with key={with_key}", no_key == 2 and with_key == 1)


if __name__ == "__main__":
    tests()
    with open(__file__.replace("idempotency.py", "idempotency_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
