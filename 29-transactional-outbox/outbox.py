"""Transactional outbox, measured for Part 29.

PostgreSQL 17 (wal_level=logical) + Apache Kafka 3.9 (KRaft) + Debezium 3.0 (Kafka Connect, Postgres connector,
Outbox Event Router). The ticket service creates tickets and must publish ticket.created to Kafka.
"""
import json
import random
import subprocess
import threading
import time
import urllib.request
import uuid

import psycopg
from kafka import KafkaConsumer, KafkaProducer

PG = "host=localhost port=55433 user=postgres password=test dbname=postgres"
BOOT = "localhost:59092"
CONNECT = "http://localhost:58083"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else None


def sql(q, *a):
    with psycopg.connect(PG, autocommit=True) as c:
        return c.execute(q, a).fetchall() if c.execute(q, a).description else None


def setup():
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS tickets, outbox, outbox_events")
        c.execute("CREATE TABLE tickets (id bigserial PRIMARY KEY, tenant text, subject text, status text DEFAULT 'open')")
        c.execute("""CREATE TABLE outbox (seq bigserial PRIMARY KEY, event_id uuid NOT NULL, aggregate_id text NOT NULL,
                     type text NOT NULL, payload jsonb NOT NULL, created_at timestamptz DEFAULT clock_timestamp(), published_at timestamptz)""")
        c.execute("CREATE INDEX outbox_unpublished ON outbox (seq) WHERE published_at IS NULL")
        c.execute("""CREATE TABLE outbox_events (id uuid PRIMARY KEY, aggregatetype text NOT NULL, aggregateid text NOT NULL,
                     type text NOT NULL, payload jsonb NOT NULL, created_at timestamptz DEFAULT clock_timestamp())""")


def producer():
    return KafkaProducer(bootstrap_servers=BOOT, acks="all", linger_ms=5,
                         key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())


def drain(topic, secs=6.0, until=None):
    """Read everything on a topic. Returns list of (event dict, receive time, partition, offset)."""
    c = KafkaConsumer(topic, bootstrap_servers=BOOT, auto_offset_reset="earliest", enable_auto_commit=False,
                      group_id=f"lab-{uuid.uuid4()}", consumer_timeout_ms=1500)
    out, t0 = [], time.time()
    while time.time() - t0 < secs:
        for m in c:
            v = json.loads(m.value)
            if isinstance(v, str):
                v = json.loads(v)
            out.append((v, time.time(), m.partition, m.offset))
        if until and until(out):
            break
    c.close()
    return out


class Crash(Exception):
    pass


class Collector:
    """A consumer that runs while events are produced, so receive times are real."""
    def __init__(self, topic):
        from kafka.admin import KafkaAdminClient, NewTopic
        try:
            KafkaAdminClient(bootstrap_servers=BOOT).create_topics([NewTopic(topic, 1, 1)])
        except Exception:
            pass
        self.out, self.stop = [], threading.Event()
        self.c = KafkaConsumer(topic, bootstrap_servers=BOOT, auto_offset_reset="earliest", enable_auto_commit=False,
                               group_id=f"lab-{uuid.uuid4()}", consumer_timeout_ms=200)
        while not self.c.assignment():
            self.c.poll(200)
        self.t = threading.Thread(target=self.run)
        self.t.start()

    def run(self):
        while not self.stop.is_set():
            for m in self.c:
                v = json.loads(m.value)
                if isinstance(v, str):
                    v = json.loads(v)
                self.out.append((v, time.time(), m.partition, m.offset))

    def wait(self, pred, secs):
        t0 = time.time()
        while time.time() - t0 < secs and not pred(self.out):
            time.sleep(0.2)
        return self.out

    def close(self):
        self.stop.set()
        self.t.join()
        self.c.close()


# ---- A. Dual write ------------------------------------------------------------------------------------
def dual_write(order, n=1000, p=0.02, seed=1):
    rnd = random.Random(seed)
    topic = f"dual.{order}.{uuid.uuid4().hex[:6]}"
    pr = producer()
    with psycopg.connect(PG) as c:
        c.execute("TRUNCATE tickets")
        c.commit()
        for i in range(n):
            try:
                if order == "db-then-publish":
                    tid = c.execute("INSERT INTO tickets (tenant, subject) VALUES ('halcyon', %s) RETURNING id", (f"t{i}",)).fetchone()[0]
                    c.commit()
                    if rnd.random() < p:
                        raise Crash                         # process dies after commit, before publish
                    pr.send(topic, key=str(tid), value={"ticket_id": tid})
                else:
                    tid = c.execute("INSERT INTO tickets (tenant, subject) VALUES ('halcyon', %s) RETURNING id", (f"t{i}",)).fetchone()[0]
                    pr.send(topic, key=str(tid), value={"ticket_id": tid}).get()
                    if rnd.random() < p:
                        raise Crash                         # commit fails (deadlock, constraint, crash) after the publish
                    c.commit()
            except Crash:
                c.rollback()
    pr.flush()
    ev = {e["ticket_id"] for e, *_ in drain(topic, 8)}
    rows = {r[0] for r in sql("SELECT id FROM tickets")}
    return {"order": order, "tickets": len(rows), "events": len(ev), "tickets_without_event": len(rows - ev), "events_without_ticket": len(ev - rows)}


# ---- B. Outbox + polling relay ---------------------------------------------------------------------------
def create_ticket(c, i, tenant="halcyon"):
    tid = c.execute("INSERT INTO tickets (tenant, subject) VALUES (%s, %s) RETURNING id", (tenant, f"t{i}")).fetchone()[0]
    eid = str(uuid.uuid4())
    payload = {"event_id": eid, "ticket_id": tid, "created_ms": time.time() * 1000}
    c.execute("INSERT INTO outbox (event_id, aggregate_id, type, payload) VALUES (%s, %s, 'ticket.created', %s)", (eid, str(tid), json.dumps(payload)))
    return tid, eid


def relay(topic, stop, interval, crash_p=0.0, seed=2, shard=None, shards=1, stats=None, watermark=False):
    rnd = random.Random(seed)
    pr = producer()
    last = 0
    with psycopg.connect(PG) as c:
        while not stop.is_set():
            if watermark:
                rows = c.execute("SELECT seq, aggregate_id, payload FROM outbox WHERE seq > %s ORDER BY seq LIMIT 200", (last,)).fetchall()
            else:
                if shard is None:
                    rows = c.execute("SELECT seq, aggregate_id, payload FROM outbox WHERE published_at IS NULL"
                                     " ORDER BY seq LIMIT 200 FOR UPDATE SKIP LOCKED").fetchall()
                else:                                       # each worker owns a hash shard of aggregates
                    rows = c.execute("SELECT seq, aggregate_id, payload FROM outbox WHERE published_at IS NULL"
                                     " AND abs(hashtext(aggregate_id)) %% %s = %s ORDER BY seq LIMIT 200 FOR UPDATE SKIP LOCKED",
                                     (shards, shard)).fetchall()
            if stats is not None:
                stats["polls"] += 1
            if rows:
                for seq, agg, payload in rows:
                    pr.send(topic, key=agg, value=payload)
                pr.flush()
                if rnd.random() < crash_p:                  # crash after publishing, before marking: the batch will be sent again
                    c.rollback()
                    if stats is not None:
                        stats["crashes"] += 1
                    continue
                if watermark:
                    last = rows[-1][0]
                else:
                    c.execute("UPDATE outbox SET published_at = now() WHERE seq = ANY(%s)", ([r[0] for r in rows],))
            c.commit()
            if len(rows) < 200:
                time.sleep(interval)


def polling(interval, n=1500, crash_p=0.0, rate=300):
    setup()
    topic = f"outbox.poll.{uuid.uuid4().hex[:6]}"
    col = Collector(topic)
    stop, stats = threading.Event(), {"polls": 0, "crashes": 0}
    t = threading.Thread(target=relay, args=(topic, stop, interval, crash_p), kwargs={"stats": stats})
    t.start()
    t0 = time.time()
    with psycopg.connect(PG) as c:
        for i in range(n):
            create_ticket(c, i)
            c.commit()
            d = t0 + (i + 1) / rate - time.time()
            if d > 0:
                time.sleep(d)
    got = col.wait(lambda o: len({e["event_id"] for e, *_ in o}) >= n, 20)
    time.sleep(1)
    stop.set()
    t.join()
    col.close()
    ids = [e["event_id"] for e, *_ in got]
    lat = [rt * 1000 - e["created_ms"] for e, rt, *_ in got]
    return {"interval_ms": int(interval * 1000), "created": n, "received": len(ids), "unique": len(set(ids)), "duplicates": len(ids) - len(set(ids)),
            "lost": n - len(set(ids)), "relay_crashes": stats["crashes"], "polls_per_s": round(stats["polls"] / (time.time() - t0), 1),
            "latency_p50_ms": round(pct(lat, .5)), "latency_p99_ms": round(pct(lat, .99))}


def watermark_bug(use_status, n_writers=8, per_writer=150):
    """Writers hold their transaction open a random 0–300 ms after inserting the outbox row (other work in the same tx).
    A sequence value is taken at INSERT time, but the row becomes visible at COMMIT — out of seq order."""
    setup()
    topic = f"outbox.wm.{uuid.uuid4().hex[:6]}"
    stop = threading.Event()
    t = threading.Thread(target=relay, args=(topic, stop, 0.05), kwargs={"watermark": not use_status})
    t.start()

    def writer(w):
        rnd = random.Random(w)
        with psycopg.connect(PG) as c:
            for i in range(per_writer):
                create_ticket(c, i)
                time.sleep(rnd.uniform(0, 0.3) if rnd.random() < 0.2 else 0)
                c.commit()
    ws = [threading.Thread(target=writer, args=(w,)) for w in range(n_writers)]
    for w in ws:
        w.start()
    for w in ws:
        w.join()
    time.sleep(1.5)
    stop.set()
    t.join()
    got = {e["event_id"] for e, *_ in drain(topic, 8)}
    created = {r[0] for r in sql("SELECT event_id::text FROM outbox")}
    return {"relay": "status column" if use_status else "seq > last_seen", "created": len(created), "lost": len(created - got)}


def ordering(workers, sharded, aggregates=200, per=5):
    """Each ticket gets 5 events (versions 1..5). Relay workers publish with key = ticket id (same Kafka partition)."""
    setup()
    topic = f"outbox.ord.{uuid.uuid4().hex[:6]}"
    with psycopg.connect(PG) as c:
        for v in range(1, per + 1):
            for a in range(aggregates):
                payload = {"event_id": str(uuid.uuid4()), "ticket_id": a, "version": v, "created_ms": time.time() * 1000}
                c.execute("INSERT INTO outbox (event_id, aggregate_id, type, payload) VALUES (%s, %s, 'ticket.updated', %s)",
                          (payload["event_id"], str(a), json.dumps(payload)))
        c.commit()
    stop = threading.Event()
    ts = [threading.Thread(target=relay, args=(topic, stop, 0.01), kwargs={"seed": w, "shard": w if sharded else None, "shards": workers})
          for w in range(workers)]
    for t in ts:
        t.start()
    got = drain(topic, 15, until=lambda o: len(o) >= aggregates * per)
    stop.set()
    for t in ts:
        t.join()
    seen, bad = {}, set()
    for e, _, part, off in sorted(got, key=lambda x: (x[2], x[3])):
        a = e["ticket_id"]
        if e["version"] < seen.get(a, 0):
            bad.add(a)
        seen[a] = max(seen.get(a, 0), e["version"])
    return {"workers": workers, "sharded": sharded, "events": len(got), "tickets_out_of_order": len(bad)}


# ---- C. Debezium -------------------------------------------------------------------------------------------
def connect_req(method, path, body=None):
    req = urllib.request.Request(CONNECT + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return {"error": e.code, "body": e.read().decode()[:300]}
    except Exception:
        return None


def register(name, slot):
    connect_req("DELETE", f"/connectors/{name}")
    time.sleep(1)
    cfg = {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "plugin.name": "pgoutput",
           "database.hostname": "ul-pgcdc", "database.port": "5432", "database.user": "postgres", "database.password": "test",
           "database.dbname": "postgres", "topic.prefix": "halcyon", "slot.name": slot, "publication.name": f"pub_{slot}",
           "publication.autocreate.mode": "filtered", "table.include.list": "public.outbox_events", "snapshot.mode": "no_data",
           "tombstones.on.delete": "false",
           "transforms": "outbox", "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
           "transforms.outbox.route.topic.replacement": "outbox.event.${routedByValue}",
           "key.converter": "org.apache.kafka.connect.storage.StringConverter",
           "value.converter": "org.apache.kafka.connect.storage.StringConverter"}
    r = connect_req("POST", "/connectors", {"name": name, "config": cfg})
    for _ in range(60):
        s = connect_req("GET", f"/connectors/{name}/status")
        if s and s.get("tasks") and s["tasks"][0]["state"] == "RUNNING":
            return True
        time.sleep(1)
    print("connector status:", s, r)
    return False


def debezium_write(c, i, delete_after=False):
    tid = c.execute("INSERT INTO tickets (tenant, subject) VALUES ('halcyon', %s) RETURNING id", (f"t{i}",)).fetchone()[0]
    eid = str(uuid.uuid4())
    payload = {"event_id": eid, "ticket_id": tid, "created_ms": time.time() * 1000}
    c.execute("INSERT INTO outbox_events (id, aggregatetype, aggregateid, type, payload) VALUES (%s, 'Ticket', %s, 'ticket.created', %s)",
              (eid, str(tid), json.dumps(payload)))
    if delete_after:
        c.execute("DELETE FROM outbox_events WHERE id = %s", (eid,))     # the WAL keeps the insert; the table stays empty
    return eid


def slot_lag_mb(slot):
    r = sql("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots WHERE slot_name = %s", slot)
    return round(float(r[0][0]) / 1024 / 1024, 1) if r and r[0][0] is not None else None


def debezium(n=1500, rate=300):
    setup()
    slot = f"dbz_{uuid.uuid4().hex[:6]}"
    connect_req("DELETE", "/connectors/ul-outbox")
    time.sleep(3)
    sql("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'dbz_%%' AND NOT active")
    ok = register("ul-outbox", slot)
    topic = "outbox.event.Ticket"
    base = len(drain(topic, 3))
    out = {"registered": ok}
    col = Collector(topic)
    col.wait(lambda o: len(o) >= base, 10)
    # 1) steady flow: latency and loss, with rows deleted in the same transaction
    t0 = time.time()
    ids = []
    with psycopg.connect(PG) as c:
        for i in range(n):
            ids.append(debezium_write(c, i, delete_after=True))
            c.commit()
            d = t0 + (i + 1) / rate - time.time()
            if d > 0:
                time.sleep(d)
    want = set(ids)
    got = [g for g in col.wait(lambda o: len({e["event_id"] for e, *_ in o} & want) >= len(want), 30) if g[0]["event_id"] in want]
    col.close()
    lat = [rt * 1000 - e["created_ms"] for e, rt, *_ in got]
    rec = {e["event_id"] for e, *_ in got}
    out["steady"] = {"created": n, "received": len(got), "lost": len(set(ids) - rec), "latency_p50_ms": round(pct(lat, .5)),
                     "latency_p99_ms": round(pct(lat, .99)), "outbox_rows_left": sql("SELECT count(*) FROM outbox_events")[0][0]}
    # 2) connector down: WAL retained by the replication slot grows
    subprocess.run(["docker", "pause", "ul-connect"], capture_output=True)
    lag_series = [{"t": 0, "mb": slot_lag_mb(slot)}]
    ids2 = []
    t1 = time.time()
    with psycopg.connect(PG) as c:
        for i in range(6000):
            ids2.append(debezium_write(c, i, delete_after=True))
            c.commit()
            if i % 1000 == 999:
                lag_series.append({"t": round(time.time() - t1, 1), "mb": slot_lag_mb(slot)})
    for k in range(5):                                   # other tables' writes: the slot retains their WAL too
        sql("CREATE TABLE IF NOT EXISTS ticket_attachments_log (id bigserial, body text)")
        sql("INSERT INTO ticket_attachments_log (body) SELECT repeat('x', 1000) FROM generate_series(1, 20000)")
        lag_series.append({"t": round(time.time() - t1, 1), "mb": slot_lag_mb(slot)})
    sql("CHECKPOINT")
    out["paused"] = {"events_while_down": len(ids2), "wal_retained_mb": slot_lag_mb(slot), "series": lag_series}
    subprocess.run(["docker", "unpause", "ul-connect"], capture_output=True)
    w2 = set(ids2)
    got2 = [g for g in drain(topic, 90, until=lambda o: len({e["event_id"] for e, *_ in o} & w2) >= len(w2)) if g[0]["event_id"] in w2]
    rec2 = {e["event_id"] for e, *_ in got2}
    time.sleep(20)                                       # outbox is quiet now; the other table's WAL is still pinned
    quiet = slot_lag_mb(slot)
    hb_series = []
    t2 = time.time()
    with psycopg.connect(PG) as c:                      # a small periodic write to a captured table — what heartbeat.action.query automates
        for _ in range(60):
            debezium_write(c, 0, delete_after=True)
            c.commit()
            time.sleep(1)
            hb_series.append({"t": round(time.time() - t2), "mb": slot_lag_mb(slot)})
            if (slot_lag_mb(slot) or 999) < 5:
                break
    out["after_resume"] = {"received": len(got2), "lost": len(set(ids2) - rec2), "wal_retained_mb_quiet": quiet,
                           "wal_retained_mb_heartbeat": slot_lag_mb(slot), "heartbeat_seconds": hb_series[-1]["t"] if hb_series else None}
    # 3) kill the Connect worker mid-stream: events after the last committed offset are sent again
    ids3 = []
    stop = threading.Event()

    def writer():
        with psycopg.connect(PG) as c:
            i = 0
            while not stop.is_set():
                ids3.append(debezium_write(c, i, delete_after=True))
                c.commit()
                i += 1
                time.sleep(1 / rate)
    w = threading.Thread(target=writer)
    w.start()
    time.sleep(4)
    subprocess.run(["docker", "kill", "ul-connect"], capture_output=True)
    time.sleep(3)
    stop.set()
    w.join()
    subprocess.run(["docker", "run", "-d", "--rm", "--name", "ul-connect", "--network", "ul-net", "-p", "58083:8083",
                    "-e", "BOOTSTRAP_SERVERS=ul-kafka:9092", "-e", "GROUP_ID=ul-connect", "-e", "CONFIG_STORAGE_TOPIC=ul_connect_configs",
                    "-e", "OFFSET_STORAGE_TOPIC=ul_connect_offsets", "-e", "STATUS_STORAGE_TOPIC=ul_connect_statuses",
                    "-e", "CONFIG_STORAGE_REPLICATION_FACTOR=1", "-e", "OFFSET_STORAGE_REPLICATION_FACTOR=1", "-e", "STATUS_STORAGE_REPLICATION_FACTOR=1",
                    "-e", "OFFSET_FLUSH_INTERVAL_MS=1000", "quay.io/debezium/connect:3.0.8.Final"], capture_output=True)
    for _ in range(90):
        s = connect_req("GET", "/connectors/ul-outbox/status")
        try:
            if s and s.get("tasks") and s["tasks"][0]["state"] == "RUNNING":
                break
        except Exception:
            pass
        time.sleep(1)
    time.sleep(5)
    allm = drain(topic, 40, until=lambda o: {e["event_id"] for e, *_ in o} >= set(ids3))
    seen3 = [e["event_id"] for e, *_ in allm if e["event_id"] in set(ids3)]
    out["kill"] = {"created": len(ids3), "received": len(seen3), "unique": len(set(seen3)), "lost": len(set(ids3) - set(seen3)),
                   "duplicates": len(seen3) - len(set(seen3))}
    return out


if __name__ == "__main__":
    setup()
    A = {o: dual_write(o) for o in ("db-then-publish", "publish-then-db")}
    for r in A.values():
        print(r, flush=True)
    B = {"1000": polling(1.0), "100": polling(0.1), "crash": polling(0.1, crash_p=0.05)}
    for r in B.values():
        print(r, flush=True)
    W = {"watermark": watermark_bug(False), "status": watermark_bug(True)}
    for r in W.values():
        print(r, flush=True)
    O = {"1": ordering(1, False), "2": ordering(2, False), "2-sharded": ordering(2, True)}
    for r in O.values():
        print(r, flush=True)
    D = debezium()
    print(json.dumps({k: v for k, v in D.items()}, default=str)[:1500], flush=True)
    data.update({"dual": A, "polling": B, "watermark": W, "ordering": O, "debezium": D})
    a, b = A["db-then-publish"], A["publish-then-db"]
    report("Commit, then publish", "a crash between the two leaves tickets nobody hears about",
           f"{a['tickets_without_event']} of {a['tickets']} tickets have no event",
           a["tickets_without_event"] > 10 and a["events_without_ticket"] == 0)
    report("Publish, then commit", "a failed commit leaves events about tickets that don't exist",
           f"{b['events_without_ticket']} events describe tickets that were rolled back",
           b["events_without_ticket"] > 10 and b["tickets_without_event"] == 0)
    s, f, cr = B["1000"], B["100"], B["crash"]
    report("Outbox + polling relay (1 s interval)", "no event lost; latency ≈ the polling interval",
           f"lost {s['lost']}, p50 {s['latency_p50_ms']} ms, p99 {s['latency_p99_ms']} ms",
           s["lost"] == 0 and s["latency_p50_ms"] > 300 and s["duplicates"] == 0)
    report("Polling every 100 ms", "lower latency, ~10× the queries", f"p50 {f['latency_p50_ms']} ms, p99 {f['latency_p99_ms']} ms; {f['polls_per_s']} polls/s vs {s['polls_per_s']}",
           f["latency_p50_ms"] < s["latency_p50_ms"] and f["lost"] == 0 and f["polls_per_s"] > s["polls_per_s"])
    report("Relay crashes after publishing, before marking", "nothing lost, some events published twice — consumers must dedupe",
           f"{cr['relay_crashes']} crashes → lost {cr['lost']}, duplicates {cr['duplicates']}",
           cr["lost"] == 0 and cr["duplicates"] > 50)
    wm, st = W["watermark"], W["status"]
    report("Relay that remembers the last seq it saw", "a transaction that commits late with an earlier seq is skipped forever",
           f"lost {wm['lost']} of {wm['created']}", wm["lost"] > 50)
    report("Relay that selects unpublished rows (status column)", "late commits are picked up", f"lost {st['lost']} of {st['created']}", st["lost"] == 0)
    o1, o2, o2s = O["1"], O["2"], O["2-sharded"]
    report("Two relay workers with SKIP LOCKED", "per-ticket order breaks on the topic",
           f"tickets out of order: 1 worker {o1['tickets_out_of_order']}, 2 workers {o2['tickets_out_of_order']}",
           o1["tickets_out_of_order"] == 0 and o2["tickets_out_of_order"] > 20)
    report("Two workers, each owning a hash shard of ticket ids", "order kept per ticket", f"out of order: {o2s['tickets_out_of_order']}", o2s["tickets_out_of_order"] == 0)
    ds = D["steady"]
    report("Debezium + Outbox Event Router", "no polling; events from the WAL, even when the row is deleted in the same transaction",
           f"lost {ds['lost']} of {ds['created']}; p50 {ds['latency_p50_ms']} ms, p99 {ds['latency_p99_ms']} ms; rows left in outbox table {ds['outbox_rows_left']}",
           ds["lost"] == 0 and ds["outbox_rows_left"] == 0 and ds["received"] == ds["created"])
    dp, dr = D["paused"], D["after_resume"]
    report("Connector down: the replication slot keeps WAL", "WAL on the database disk grows until the connector returns; nothing is lost",
           f"{dp['events_while_down']} events + other writes while down → {dp['wal_retained_mb']} MB of WAL retained; after resume lost {dr['lost']}",
           dp["wal_retained_mb"] > 50 and dr["lost"] == 0)
    report("Quiet outbox, busy database", "the slot stays pinned until a captured table changes; a heartbeat query releases it",
           f"retained 20 s after catching up: {dr['wal_retained_mb_quiet']} MB; with one captured write per second: {dr['wal_retained_mb_heartbeat']} MB within {dr['heartbeat_seconds']} s",
           dr["wal_retained_mb_quiet"] > 50 and dr["wal_retained_mb_heartbeat"] < 5)
    dk = D["kill"]
    report("Connect worker killed mid-stream", "at-least-once: nothing lost, events since the last offset flush re-sent",
           f"created {dk['created']}, lost {dk['lost']}, duplicates {dk['duplicates']}",
           dk["lost"] == 0 and dk["duplicates"] > 0)
    with open(__file__.replace("outbox.py", "outbox_results.json"), "w") as fh:
        json.dump(data, fh, default=str)
    print(f"{sum(results)}/{len(results)} passed")
