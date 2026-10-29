"""Load shedding, simulated for Part 25 (discrete-event, fixed seeds, pure Python).

Model: an API with 8 workers; each request takes ~20 ms of work (capacity ≈ 400 req/s).
From t = 5 s to t = 25 s, traffic jumps from 300 req/s to 1,200 req/s (3× capacity), then returns to 300.
30% of requests are interactive (agents loading tickets), 70% are bulk (integration syncs).
Clients give up after 1 s: work finished after that is wasted. "Goodput" = requests answered within 1 s.
Rejecting is cheap but not free: each rejection costs 0.5 ms of worker CPU (parse, authenticate, write the 503).
"""
import heapq
import json
import math
import random
from collections import deque

WORKERS, SVC, DEADLINE, SIM, REJECT_COST = 8, 0.020, 1.0, 40.0, 0.0005
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def rate_at(t):
    return 1200 if 5 <= t < 25 else 300


def simulate(policy, retry="none", seed=5, slow=1.0):
    rnd = random.Random(seed)
    ev, seq = [], 0

    def push(t, kind, *a):
        nonlocal seq
        seq += 1
        heapq.heappush(ev, (t, seq, kind, a))
    t = 0.0
    while t < SIM:
        t += rnd.expovariate(rate_at(t))
        push(t, "arrive", t, "interactive" if rnd.random() < 0.3 else "bulk", 0)
    queues = {"interactive": deque(), "bulk": deque()}
    busy, debt, attempts = 0, 0.0, 0
    per_s = [{"good": 0, "late": 0, "shed": 0, "good_interactive": 0, "offered": 0, "offered_interactive": 0} for _ in range(int(SIM) + 2)]
    lat_admitted = []

    def qlen():
        return len(queues["interactive"]) + len(queues["bulk"])

    def admit(now, born, cls):
        if policy == "none":
            return True
        if policy == "queue-limit":
            return qlen() < 80                         # ~200 ms of work queued
        if policy == "priority":
            limit = 80 if cls == "interactive" else 40  # bulk is shed first
            return qlen() < limit
        return True

    def next_job(now):
        order = ("interactive", "bulk") if policy == "priority" else None
        while True:
            if order:
                src = next((queues[c] for c in order if queues[c]), None)
            else:
                src = min((q for q in queues.values() if q), key=lambda q: q[0][0], default=None)
            if src is None:
                return None
            born, cls, att = src.popleft()
            if policy == "age-drop" and now - born > 0.2:       # stale: the client will likely give up — drop before working
                per_s[int(born)]["shed"] += 1
                continue
            return born, cls, att

    def start(now):
        nonlocal busy, debt
        while busy < WORKERS:
            j = next_job(now)
            if j is None:
                return
            busy += 1
            steal = min(debt, 0.02)                          # CPU spent on rejections is taken from real work
            debt -= steal
            m = slow if 5 <= now < 25 else 1.0
            push(now + rnd.lognormvariate(math.log(SVC * m), 0.3) + steal, "done", *j)

    while ev:
        now, _, kind, a = heapq.heappop(ev)
        if kind == "arrive":
            born, cls, att = a
            if 5 <= now < 25:
                attempts += 1
            s = per_s[int(born)]
            if att == 0:
                s["offered"] += 1
                s["offered_interactive"] += cls == "interactive"
            if admit(now, born, cls):
                queues[cls].append((born, cls, att))
                start(now)
            else:
                s["shed"] += 1
                debt += REJECT_COST
                delay = None
                if retry == "immediate" and att < 3:
                    delay = 0.01                                   # up to 3 retries, 10 ms apart
                elif retry == "budget" and att < 3 and rnd.random() < 0.1:
                    delay = rnd.uniform(0, 0.2 * 2 ** att)         # retry budget: ~10% of failures retried, with jittered backoff
                if delay is not None and now + delay - born < DEADLINE:
                    push(now + delay, "arrive", born, cls, att + 1)
        elif kind == "done":
            born, cls, att = a
            busy -= 1
            took = now - born
            s = per_s[int(born)]
            if took <= DEADLINE:
                s["good"] += 1
                s["good_interactive"] += cls == "interactive"
                lat_admitted.append(took)
            else:
                s["late"] += 1
            start(now)
    win = per_s[5:25]
    lat_admitted.sort()
    return {"policy": policy, "retry": retry, "slow": slow, "attempts_per_s_during": round(attempts / 20),
            "goodput_per_s_during": round(sum(x["good"] for x in win) / 20),
            "late_per_s_during": round(sum(x["late"] for x in win) / 20),
            "interactive_success_pct": round(100 * sum(x["good_interactive"] for x in win) / max(1, sum(x["offered_interactive"] for x in win)), 1),
            "p99_good_ms": round(lat_admitted[int(len(lat_admitted) * .99) - 1] * 1000) if lat_admitted else None,
            "series": [{"s": i, "good": x["good"], "late": x["late"], "shed": x["shed"]} for i, x in enumerate(per_s[:int(SIM)])]}


if __name__ == "__main__":
    runs = [simulate("none"), simulate("queue-limit"), simulate("age-drop"), simulate("priority"),
            simulate("queue-limit", retry="immediate"), simulate("queue-limit", retry="budget"),
            simulate("queue-limit", slow=3.0), simulate("age-drop", slow=3.0)]
    for r in runs:
        print(f"{r['policy']:12s} retry={r['retry']:9s} slow={r['slow']} attempts {r['attempts_per_s_during']:5d}/s goodput {r['goodput_per_s_during']:4d}/s  late {r['late_per_s_during']:4d}/s  "
              f"interactive ok {r['interactive_success_pct']:5.1f}%  p99 of good {r['p99_good_ms']} ms")
    data["runs"] = runs
    R = {(r["policy"], r["retry"], r["slow"]): r for r in runs}
    n, q, a, p = R[("none", "none", 1.0)], R[("queue-limit", "none", 1.0)], R[("age-drop", "none", 1.0)], R[("priority", "none", 1.0)]
    report("3× capacity with no shedding", "everything queues; almost nothing finishes within the client's 1 s deadline",
           f"goodput {n['goodput_per_s_during']}/s; work finished too late {n['late_per_s_during']}/s",
           n["goodput_per_s_during"] < 100 and n["late_per_s_during"] > 500)
    report("Shed when the queue holds ~200 ms of work", "goodput stays near capacity and admitted requests stay fast",
           f"goodput {q['goodput_per_s_during']}/s; P99 of answered requests {q['p99_good_ms']} ms",
           q["goodput_per_s_during"] > 300 and q["late_per_s_during"] == 0 and q["p99_good_ms"] < 500)
    report("Drop requests that waited > 200 ms before starting them", "also keeps goodput near capacity without a fixed queue limit",
           f"goodput {a['goodput_per_s_during']}/s; P99 {a['p99_good_ms']} ms",
           a["goodput_per_s_during"] > 300 and a["late_per_s_during"] == 0)
    report("Priority-aware shedding (bulk first)", "interactive requests keep succeeding while bulk absorbs the shedding",
           f"interactive success {p['interactive_success_pct']}% vs {q['interactive_success_pct']}% with a single queue limit",
           p["interactive_success_pct"] > q["interactive_success_pct"] + 20 and p["interactive_success_pct"] > 90)
    im, bo = R[("queue-limit", "immediate", 1.0)], R[("queue-limit", "budget", 1.0)]
    report("Clients retry rejected requests immediately (3 retries)", "attempts multiply and rejecting them eats capacity shedding was protecting",
           f"attempts {im['attempts_per_s_during']}/s vs {q['attempts_per_s_during']}/s; goodput {im['goodput_per_s_during']}/s vs {q['goodput_per_s_during']}/s",
           im["attempts_per_s_during"] > 2 * q["attempts_per_s_during"] and im["goodput_per_s_during"] < q["goodput_per_s_during"] * 0.9)
    report("Clients with a 10% retry budget and jittered backoff", "few extra attempts; goodput stays close to the no-retry case",
           f"attempts {bo['attempts_per_s_during']}/s; goodput {bo['goodput_per_s_during']}/s",
           bo["goodput_per_s_during"] > q["goodput_per_s_during"] * 0.9
           and bo["attempts_per_s_during"] < im["attempts_per_s_during"] * 0.5)
    qs, as_ = R[("queue-limit", "none", 3.0)], R[("age-drop", "none", 3.0)]
    report("Work gets 3× slower (slow database): queue-length limit vs queue-age limit",
           "the same 80-slot queue now means ~600 ms of waiting; an age limit keeps waits bounded",
           f"P99 of answered: queue-limit {qs['p99_good_ms']} ms, age-drop {as_['p99_good_ms']} ms; goodput {qs['goodput_per_s_during']}/s vs {as_['goodput_per_s_during']}/s",
           as_["p99_good_ms"] < qs["p99_good_ms"] * 0.6 and as_["p99_good_ms"] < 400)
    with open(__file__.replace("shedding.py", "shedding_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
