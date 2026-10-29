"""Backpressure, measured for Part 24 (real threads, wall-clock time).

An email worker reads messages from the broker at up to 500/s and hands them to sender threads that call the email
provider, which completes 200 sends/s (4 senders × 20 ms). 15 s per scenario. Each message carries a 2 KB body.
"""
import json
import queue
import threading
import time

READ_RATE, SENDERS, SEND_TIME, DURATION = 500, 4, 0.02, 15.0
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def run(mode, cap=None):
    q = queue.Queue(maxsize=cap or 0)
    t0 = time.time()
    stats = {"read": 0, "sent": 0, "rejected": 0, "left_in_broker": 0}
    ages, ages_e2e, series = [], [], []
    stop = threading.Event()
    lock = threading.Lock()

    def reader():
        i = 0
        while time.time() - t0 < DURATION:
            at = t0 + i / READ_RATE
            d = at - time.time()
            if d > 0:
                time.sleep(d)
            msg = (at, time.time(), b"x" * 2048)    # (entered the broker, read by the worker, body)
            if mode == "unbounded":
                q.put(msg)
            elif mode == "bounded-block":
                q.put(msg)                    # blocks when full: the reader stops pulling from the broker
            elif mode == "bounded-reject":
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    with lock:
                        stats["rejected"] += 1   # nack: message stays in the broker / is retried later
                    i += 1
                    continue
            with lock:
                stats["read"] += 1
            i += 1
        with lock:
            stats["left_in_broker"] = int(DURATION * READ_RATE) - stats["read"] - stats["rejected"]

    def sender():
        while not stop.is_set():
            try:
                in_broker, read_at, _ = q.get(timeout=0.1)
            except queue.Empty:
                continue
            time.sleep(SEND_TIME)
            with lock:
                stats["sent"] += 1
                ages.append(time.time() - read_at)         # time since the worker read it
                ages_e2e.append(time.time() - in_broker)   # time since it was published: what the customer waits

    def sampler():
        while not stop.is_set():
            series.append({"t": round(time.time() - t0, 1), "buffered": q.qsize(),
                           "buffer_mb": round(q.qsize() * 2.1 / 1024, 1)})
            time.sleep(0.5)
    ts = [threading.Thread(target=sender) for _ in range(SENDERS)] + [threading.Thread(target=sampler)]
    for t in ts: t.start()
    r = threading.Thread(target=reader); r.start(); r.join()
    time.sleep(0.2)
    stop.set()
    for t in ts: t.join()
    late = sorted(ages[-200:])
    late_e2e = sorted(ages_e2e[-200:])
    return {"mode": mode, "cap": cap, **stats, "sent_per_s": round(stats["sent"] / DURATION, 1),
            "final_buffered": series[-1]["buffered"], "final_buffer_mb": series[-1]["buffer_mb"],
            "age_of_last_sent_s": round(late[-1], 2) if late else None,
            "e2e_age_of_last_sent_s": round(late_e2e[-1], 2) if late_e2e else None, "series": series}


if __name__ == "__main__":
    for mode, cap in (("unbounded", None), ("bounded-block", 200), ("bounded-reject", 200)):
        r = run(mode, cap)
        data[mode] = r
        print(f"{mode:15s} read {r['read']:5d} sent {r['sent']:5d} ({r['sent_per_s']}/s) rejected {r['rejected']:5d} "
              f"left in broker {r['left_in_broker']:5d} buffered at end {r['final_buffered']:5d} ({r['final_buffer_mb']} MB) "
              f"age in worker {r['age_of_last_sent_s']} s, end-to-end {r['e2e_age_of_last_sent_s']} s", flush=True)
    u, b, j = data["unbounded"], data["bounded-block"], data["bounded-reject"]
    report("Unbounded in-memory buffer", "the buffer grows by ~300 messages/s and the newest sends are ever older",
           f"buffered after 15 s: {u['final_buffered']:,} ({u['final_buffer_mb']} MB); time the last emails spent in the worker: {u['age_of_last_sent_s']} s",
           u["final_buffered"] > 3000 and u["age_of_last_sent_s"] > 5 and u["final_buffer_mb"] > 5)
    report("Bounded buffer (200) that blocks the reader", "the reader slows to the provider's pace; memory stays flat; the backlog stays in the broker",
           f"buffered {b['final_buffered']}, time in the worker {b['age_of_last_sent_s']} s, left in broker {b['left_in_broker']:,}",
           b["final_buffered"] <= 200 and b["age_of_last_sent_s"] < 2 and b["left_in_broker"] > 3000
           and j["rejected"] > 3000 and j["final_buffered"] <= 200)
    report("Backpressure doesn't make emails faster", "end to end, a message waits just as long — in the broker instead of in memory",
           f"end-to-end age of the last emails sent: unbounded {u['e2e_age_of_last_sent_s']} s, bounded {b['e2e_age_of_last_sent_s']} s",
           abs(u["e2e_age_of_last_sent_s"] - b["e2e_age_of_last_sent_s"]) < 0.25 * u["e2e_age_of_last_sent_s"]
           and b["e2e_age_of_last_sent_s"] > 5 * b["age_of_last_sent_s"])
    report("Same throughput either way", "the provider sends ~200/s no matter how much is buffered",
           f"sent per second: unbounded {u['sent_per_s']}, bounded {b['sent_per_s']}, reject {j['sent_per_s']}",
           abs(u["sent_per_s"] - b["sent_per_s"]) < 20 and abs(u["sent_per_s"] - j["sent_per_s"]) < 20
           and u["sent_per_s"] > 100)
    with open(__file__.replace("backpressure.py", "backpressure_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
