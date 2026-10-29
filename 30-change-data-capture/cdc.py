"""Change data capture, measured for Part 30.

PostgreSQL 17 (wal_level=logical) + Kafka 3.9 + Debezium 3.0 Postgres connector (pgoutput), capturing the tickets
table directly. Compared with a query-based poller (updated_at > last poll).
"""
import json
import random
import subprocess
import threading
polling = threading.Event()                      # set once the poller starts, so both sides count the same period
import time
import urllib.request
import uuid

import psycopg
from kafka import KafkaConsumer

PG = "host=localhost port=55433 user=postgres password=test dbname=postgres"
BOOT = "localhost:59092"
CONNECT = "http://localhost:58083"
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def sql(q, *a):
    with psycopg.connect(PG, autocommit=True) as c:
        cur = c.execute(q, a or None)
        return cur.fetchall() if cur.description else None


def connect_req(method, path, body=None):
    req = urllib.request.Request(CONNECT + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return {"error": e.code, "body": e.read().decode()[:400]}
    except Exception:
        return None


def task_state(name):
    s = connect_req("GET", f"/connectors/{name}/status")
    try:
        return s["tasks"][0]["state"], s["tasks"][0].get("trace", "")[:300]
    except Exception:
        return None, ""


def register(name, slot, prefix, snapshot="initial"):
    connect_req("DELETE", f"/connectors/{name}")
    time.sleep(2)
    cfg = {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "plugin.name": "pgoutput",
           "database.hostname": "ul-pgcdc", "database.port": "5432", "database.user": "postgres", "database.password": "test",
           "database.dbname": "postgres", "topic.prefix": prefix, "slot.name": slot, "publication.name": f"pub_{slot}",
           "publication.autocreate.mode": "filtered", "table.include.list": "public.tickets", "snapshot.mode": snapshot,
           "key.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "false",
           "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false"}
    connect_req("POST", "/connectors", {"name": name, "config": cfg})
    for _ in range(60):
        st, _ = task_state(name)
        if st == "RUNNING":
            return True
        time.sleep(1)
    return False


class Collector:
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
                self.out.append((json.loads(m.key) if m.key else None, json.loads(m.value) if m.value else None, time.time()))

    def wait_quiet(self, secs=8, max_secs=120):
        t0, n, last = time.time(), -1, time.time()
        while time.time() - t0 < max_secs:
            if len(self.out) != n:
                n, last = len(self.out), time.time()
            elif time.time() - last > secs:
                break
            time.sleep(0.5)
        return self.out

    def close(self):
        self.stop.set()
        self.t.join()
        self.c.close()


def apply(replica, ev):
    """A downstream copy maintained only from change events."""
    k, v, _ = ev
    if v is None:                                  # tombstone: tells log compaction the key is gone
        return
    if v["op"] in ("c", "r", "u"):
        replica[v["after"]["id"]] = v["after"]["status"]
    elif v["op"] == "d":
        replica.pop(v["before"]["id"], None)


def source_state():
    return dict(sql("SELECT id, status FROM tickets"))


def setup(rows):
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS tickets CASCADE")
        c.execute("""CREATE TABLE tickets (id bigserial PRIMARY KEY, tenant text, subject text, status text DEFAULT 'open',
                     updated_at timestamptz DEFAULT clock_timestamp())""")
        c.execute("""CREATE OR REPLACE FUNCTION touch() RETURNS trigger AS $$ BEGIN NEW.updated_at := clock_timestamp(); RETURN NEW; END $$ LANGUAGE plpgsql""")
        c.execute("CREATE TRIGGER t_touch BEFORE UPDATE ON tickets FOR EACH ROW EXECUTE FUNCTION touch()")
        c.execute("INSERT INTO tickets (tenant, subject) SELECT 'halcyon', 'ticket ' || g FROM generate_series(1, %s) g", (rows,))


def workload(stop, counts, seed=5):
    rnd = random.Random(seed)
    statuses = ["pending", "solved", "open", "on-hold"]
    with psycopg.connect(PG, autocommit=True) as c:
        while not stop.is_set():
            r = rnd.random()
            ids = [x[0] for x in c.execute("SELECT id FROM tickets TABLESAMPLE SYSTEM (1) LIMIT 5").fetchall()] or [1]
            tid = rnd.choice(ids)
            if r < 0.7:
                for _ in range(rnd.choice([1, 1, 1, 3])):           # sometimes several quick updates to the same ticket
                    cur = c.execute("UPDATE tickets SET status = %s WHERE id = %s", (rnd.choice(statuses), tid))
                    counts["updates"] += cur.rowcount > 0 and polling.is_set()     # only real changes, only while the poller runs
            elif r < 0.8:
                cur = c.execute("DELETE FROM tickets WHERE id = %s", (tid,))
                counts["deletes"] += cur.rowcount > 0 and polling.is_set()
            else:
                c.execute("INSERT INTO tickets (tenant, subject) VALUES ('halcyon', 'new')")
                counts["inserts"] += polling.is_set()
            time.sleep(0.004)


def poller(stop, seen, interval=1.0):
    """Query-based change capture: everything with updated_at newer than the last poll."""
    last = sql("SELECT now()")[0][0]
    while not stop.is_set():
        time.sleep(interval)
        with psycopg.connect(PG, autocommit=True) as c:
            now = c.execute("SELECT now()").fetchone()[0]
            rows = c.execute("SELECT id, status, updated_at FROM tickets WHERE updated_at > %s", (last,)).fetchall()
        for r in rows:
            seen["changes"] += 1
            seen["replica"][r[0]] = r[1]
        last = now


def main_flow(rows=20000, secs=12):
    setup(rows)
    for n in ("ul-outbox", "ul-cdc"):
        connect_req("DELETE", f"/connectors/{n}")
    time.sleep(3)
    sql("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active")
    prefix = f"cdc{uuid.uuid4().hex[:5]}"
    topic = f"{prefix}.public.tickets"
    col = Collector(topic)
    counts = {"updates": 0, "deletes": 0, "inserts": 0}
    stop = threading.Event()
    w = threading.Thread(target=workload, args=(stop, counts))
    w.start()                                           # changes start BEFORE and continue DURING the snapshot
    time.sleep(1)
    poll_seen = {"changes": 0, "replica": dict(source_state())}
    p = threading.Thread(target=poller, args=(stop, poll_seen))
    polling.set()
    p.start()
    slot = f"cdc_{uuid.uuid4().hex[:6]}"
    t_reg = time.time()
    ok = register("ul-cdc", slot, prefix)
    time.sleep(secs)
    stop.set()
    w.join()
    p.join()
    evs = list(col.wait_quiet())
    src = source_state()
    replica = {}
    for e in evs:
        apply(replica, e)
    ops = {}
    for _, v, _ in evs:
        o = "tombstone" if v is None else v["op"]
        ops[o] = ops.get(o, 0) + 1
    upd_before = sum(1 for _, v, _ in evs if v and v["op"] == "u" and v.get("before"))
    dels = [v["before"] for _, v, _ in evs if v and v["op"] == "d"]
    del_null_cols = sorted({k for b in dels for k, x in b.items() if x is None})
    del_example = dels[0] if dels else None
    poll_mismatch = sum(1 for k in set(src) | set(poll_seen["replica"]) if src.get(k) != poll_seen["replica"].get(k))
    cdc_mismatch = sum(1 for k in set(src) | set(replica) if src.get(k) != replica.get(k))
    total_changes = counts["updates"] + counts["deletes"] + counts["inserts"]
    out = {"registered": ok, "rows": rows, "workload": counts, "events_by_op": ops, "source_rows": len(src),
           "cdc_replica_mismatch": cdc_mismatch, "poll_changes_seen": poll_seen["changes"], "poll_replica_mismatch": poll_mismatch,
           "poll_deleted_rows_still_present": sum(1 for k in poll_seen["replica"] if k not in src), "total_changes": total_changes,
           "update_events_with_before": upd_before, "delete_before_null_columns": del_null_cols, "delete_before_example": del_example}
    # replica identity FULL → full 'before' image
    sql("ALTER TABLE tickets REPLICA IDENTITY FULL")
    tid = sql("SELECT min(id) FROM tickets")[0][0]
    sql("UPDATE tickets SET status = 'solved' WHERE id = %s", tid)
    # schema change: add a column; later events carry it
    sql("ALTER TABLE tickets ADD COLUMN priority text DEFAULT 'normal'")
    sql("UPDATE tickets SET priority = 'urgent' WHERE id = %s", tid)
    evs2 = col.wait_quiet(5)
    full = [v for _, v, _ in evs2[len(evs):] if v and v["op"] == "u"]
    out["full_before_columns"] = sorted((full[0]["before"] or {}).keys()) if full else None
    out["after_alter_columns"] = sorted(full[-1]["after"].keys()) if full else None
    lat = []
    col.close()
    return out, slot, prefix


def retention(slot, prefix):
    """Cap retained WAL, stop the connector, write more than the cap: the slot is invalidated."""
    sql("ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'")
    sql("SELECT pg_reload_conf()")
    subprocess.run(["docker", "pause", "ul-connect"], capture_output=True)
    sql("CREATE TABLE IF NOT EXISTS filler (id bigserial, body text)")
    series = []
    for k in range(8):
        sql("INSERT INTO filler (body) SELECT repeat('x', 1000) FROM generate_series(1, 20000)")
        sql("CHECKPOINT")
        r = sql("SELECT wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots WHERE slot_name = %s", slot)
        series.append({"step": k, "wal_status": r[0][0] if r else None, "retained_mb": round(float(r[0][1]) / 1048576, 1) if r and r[0][1] is not None else None})
    subprocess.run(["docker", "unpause", "ul-connect"], capture_output=True)
    col = Collector(f"{prefix}.public.tickets")
    col.wait_quiet(4)
    n0 = len(col.out)
    sql("INSERT INTO tickets (tenant, subject) VALUES ('halcyon', 'after the gap')")
    time.sleep(40)
    arrived = any(v and v.get("after") and v["after"].get("subject") == "after the gap" for _, v, _ in col.out[n0:])
    state, trace = task_state("ul-cdc")
    logs = subprocess.run(["docker", "logs", "--since", "60s", "ul-connect"], capture_output=True, text=True)
    slot_errors = (logs.stdout + logs.stderr).count("Cannot obtain valid replication slot")
    col.close()
    # recovery: drop the lost slot, start over with a fresh snapshot
    connect_req("DELETE", "/connectors/ul-cdc")
    time.sleep(3)
    sql("SELECT pg_drop_replication_slot(%s)", slot)
    prefix2 = f"cdc{uuid.uuid4().hex[:5]}"
    col2 = Collector(f"{prefix2}.public.tickets")
    t0 = time.time()
    register("ul-cdc", f"cdc_{uuid.uuid4().hex[:6]}", prefix2)
    evs = list(col2.wait_quiet(8))
    resnap = time.time() - t0
    col2.close()
    replica = {}
    for e in evs:
        apply(replica, e)
    src = source_state()
    recovered = sum(1 for k in set(src) | set(replica) if src.get(k) != replica.get(k))
    sql("ALTER SYSTEM RESET max_slot_wal_keep_size")
    sql("SELECT pg_reload_conf()")
    sql("DROP TABLE IF EXISTS filler")
    return {"series": series, "final_wal_status": series[-1]["wal_status"], "connector_state": state, "event_after_gap_arrived": arrived,
            "slot_errors_in_log_60s": slot_errors, "resnapshot_events": len(evs), "resnapshot_seconds": round(resnap, 1), "replica_mismatch_after_resnapshot": recovered}


if __name__ == "__main__":
    M, slot, prefix = main_flow()
    print(json.dumps(M, default=str), flush=True)
    RT = retention(slot, prefix)
    print(json.dumps(RT, default=str)[:800], flush=True)
    connect_req("DELETE", "/connectors/ul-cdc")
    data.update({"main": M, "retention": RT})
    ops = M["events_by_op"]
    report("Snapshot, then stream", "existing rows arrive as read events; changes during the snapshot are not lost",
           f"{ops.get('r', 0):,} snapshot events for {M['rows']:,} rows; replica built only from events differs from the table in {M['cdc_replica_mismatch']} rows",
           ops.get("r", 0) >= M["rows"] * 0.95 and M["cdc_replica_mismatch"] == 0)
    report("Query-based polling (updated_at > last poll, every 1 s)", "misses deletes and intermediate updates",
           f"{M['total_changes']:,} changes ({M['workload']['deletes']} deletes); poller saw {M['poll_changes_seen']:,} rows; "
           f"its replica differs in {M['poll_replica_mismatch']} rows, {M['poll_deleted_rows_still_present']} deleted tickets still present",
           M["poll_deleted_rows_still_present"] > 50 and M["poll_changes_seen"] < M["workload"]["updates"]
           and M["poll_replica_mismatch"] > 50)
    report("Log-based CDC sees every change", "one event per insert, update and delete",
           f"events: c {ops.get('c', 0)}, u {ops.get('u', 0)}, d {ops.get('d', 0)}, tombstones {ops.get('tombstone', 0)}",
           ops.get("d", 0) > 0 and ops.get("tombstone", 0) == ops.get("d", 0)
           and ops.get("u", 0) > 0 and ops.get("c", 0) > 0)
    report("REPLICA IDENTITY DEFAULT", "update events have no 'before'; delete events carry only the key (other columns null or column defaults)",
           f"update events with a before image: {M['update_events_with_before']}; delete 'before' example: {M['delete_before_example']}",
           M["update_events_with_before"] == 0 and "tenant" in M["delete_before_null_columns"])
    report("REPLICA IDENTITY FULL", "the full previous row is included", f"before columns: {M['full_before_columns']}",
           bool(M["full_before_columns"]) and "tenant" in M["full_before_columns"] and len(M["full_before_columns"]) >= 4)
    report("Schema change: ADD COLUMN", "later events carry the new column — consumers must tolerate it", f"after columns: {M['after_alter_columns']}",
           bool(M["after_alter_columns"]) and "priority" in M["after_alter_columns"])
    report("Connector falls behind a capped WAL retention", "the slot is invalidated; nothing flows, yet the task still reports RUNNING",
           f"slot wal_status: {RT['final_wal_status']}; task state: {RT['connector_state']}; new change arrived: {RT['event_after_gap_arrived']}; "
           f"'Cannot obtain valid replication slot' x{RT['slot_errors_in_log_60s']} in the log",
           RT["final_wal_status"] == "lost" and not RT["event_after_gap_arrived"]
           and RT["connector_state"] == "RUNNING" and RT["slot_errors_in_log_60s"] > 0)
    report("Recovery: new slot + fresh snapshot", "the downstream copy is rebuilt from scratch",
           f"{RT['resnapshot_events']:,} events in {RT['resnapshot_seconds']} s; replica mismatch {RT['replica_mismatch_after_resnapshot']}",
           RT["replica_mismatch_after_resnapshot"] == 0 and RT["resnapshot_events"] > 10000)
    with open(__file__.replace("cdc.py", "cdc_results.json"), "w") as fh:
        json.dump(data, fh, default=str)
    print(f"{sum(results)}/{len(results)} passed")
