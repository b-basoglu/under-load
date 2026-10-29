"""Tail latency experiments for Part 18 (Monte Carlo + queue simulation, fixed seeds, pure Python).

Service latency model: lognormal body (median ~40 ms) plus a rare slow path (GC pause, cache miss, slow disk) —
2% of calls take 800–1,500 ms. (With exactly 1%, the P99 sits on the boundary between the two paths and jumps
between ~130 and ~830 ms from seed to seed; 2% puts it clearly inside the slow path.) Every number in the article comes from this file.
"""
import json
import math
import random

results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def pct(xs, p):
    s = sorted(xs)
    return s[min(len(s) - 1, int(math.ceil(p / 100 * len(s))) - 1)]


SLOW_P = 0.02


def service(rnd, slow_p=SLOW_P):
    if rnd.random() < slow_p:
        return rnd.uniform(800, 1500)
    return rnd.lognormvariate(math.log(40), 0.35)


def same_mean():
    rnd = random.Random(1)
    a = [service(rnd) for _ in range(200_000)]                   # mostly fast, rare 1 s outliers
    target = sum(a) / len(a)
    b = [rnd.gauss(target, 12) for _ in range(200_000)]          # consistently ~same, no outliers
    out = {k: {"mean": round(sum(v) / len(v), 1), "p50": round(pct(v, 50), 1), "p99": round(pct(v, 99), 1), "p999": round(pct(v, 99.9), 1)}
           for k, v in (("A_rare_slow_path", a), ("B_consistent", b))}
    data["same_mean"] = out
    a_, b_ = out["A_rare_slow_path"], out["B_consistent"]
    report("Two services with the same mean", "very different P99",
           f"A: mean {a_['mean']} ms, P99 {a_['p99']} ms · "
           f"B: mean {b_['mean']} ms, P99 {b_['p99']} ms",
           abs(a_["mean"] - b_["mean"]) < 2
           and a_["p99"] > 5 * b_["p99"] and a_["p99"] > 800)
    data["distribution_A"] = [round(x) for x in a[:20000]]


def fan_out():
    rnd = random.Random(2)
    rows = []
    for n in (1, 5, 10, 20, 40, 100):
        trials = 40_000
        slow = 0
        lat = []
        for _ in range(trials):
            calls = [service(rnd) for _ in range(n)]
            m = max(calls)                       # the page waits for all parallel calls
            lat.append(m)
            slow += m > 500
        rows.append({"fan_out": n, "p_at_least_one_slow": round(slow / trials, 3), "analytic": round(1 - (1 - SLOW_P) ** n, 3),
                     "page_p50": round(pct(lat, 50)), "page_p99": round(pct(lat, 99))})
        print("  ", rows[-1], flush=True)
    data["fan_out"] = rows
    r40 = next(r for r in rows if r["fan_out"] == 40)
    r1 = next(r for r in rows if r["fan_out"] == 1)
    report("A page that calls 40 services in parallel and waits for all", "most page loads hit at least one slow call",
           f"P(at least one call in the 2% slow path) = {r40['p_at_least_one_slow']:.0%} (analytic {r40['analytic']:.0%}); page P50 {r40['page_p50']} ms",
           r40["p_at_least_one_slow"] > 0.5 and abs(r40["p_at_least_one_slow"] - r40["analytic"]) < 0.02
           and r40["page_p50"] > 5 * r1["page_p50"])


def not_additive():
    rnd = random.Random(3)
    per = [[service(rnd) for _ in range(100_000)] for _ in range(5)]
    sum_p99 = sum(pct(x, 99) for x in per)
    total = [sum(per[j][i] for j in range(5)) for i in range(100_000)]
    actual = pct(total, 99)
    data["not_additive"] = {"sum_of_p99s": round(sum_p99), "actual_p99_of_sum": round(actual),
                            "each_p99": [round(pct(x, 99)) for x in per]}
    report("Five sequential calls: sum of P99s vs real end-to-end P99", "percentiles don't add up",
           f"sum of the five P99s {sum_p99:,.0f} ms vs actual P99 of the total {actual:,.0f} ms",
           sum_p99 > actual * 2 and abs(sum_p99 - actual) > 2000)


def averaging_instances():
    rnd = random.Random(4)
    good = [[service(rnd, slow_p=0.005) for _ in range(20_000)] for _ in range(9)]   # healthy instances: P99 on the fast path
    bad = [service(rnd, slow_p=0.08) for _ in range(20_000)]        # one instance with a noisy neighbour
    insts = good + [bad]
    avg_of_p99 = sum(pct(x, 99) for x in insts) / len(insts)
    true = pct([v for x in insts for v in x], 99)
    data["averaging"] = {"average_of_instance_p99s": round(avg_of_p99), "true_fleet_p99": round(true), "bad_instance_p99": round(pct(bad, 99)),
                         "good_instance_p99": round(pct(good[0], 99))}
    report("Fleet P99 computed by averaging per-instance P99s", "averaging percentiles hides the real tail",
           f"average of 10 instance P99s {avg_of_p99:,.0f} ms vs true fleet P99 {true:,.0f} ms; the bad instance alone {pct(bad, 99):,.0f} ms",
           true > avg_of_p99 * 2 and pct(bad, 99) > 800)


def sample_size():
    rnd = random.Random(5)
    out = []
    for n in (100, 1_000, 10_000, 100_000):
        ests = sorted(pct([service(rnd) for _ in range(n)], 99.9) for _ in range(30))
        out.append({"samples": n, "p999_min": round(ests[0]), "p999_max": round(ests[-1])})
    data["sample_size"] = out
    spread_small = out[0]["p999_max"] - out[0]["p999_min"]
    spread_big = out[-1]["p999_max"] - out[-1]["p999_min"]
    report("Estimating P99.9 from different sample sizes (30 repeats each)", "small samples give wildly different P99.9s",
           " · ".join(f"{o['samples']:,}: {o['p999_min']}–{o['p999_max']} ms" for o in out),
           spread_small > 3 * spread_big and spread_small > 500)


def queueing():
    """Single server with 40 ms (lognormal) service time; P99 of time in system vs utilization."""
    rows = []
    for util in (0.3, 0.5, 0.7, 0.8, 0.9, 0.95):
        rnd = random.Random(6)
        mean_service = 40 * math.exp(0.35 ** 2 / 2)
        rate = util / mean_service
        t, free_at, lat = 0.0, 0.0, []
        for _ in range(200_000):
            t += rnd.expovariate(rate)
            s = rnd.lognormvariate(math.log(40), 0.35)
            start = max(t, free_at)
            free_at = start + s
            lat.append(free_at - t)
        rows.append({"utilization": util, "p50": round(pct(lat, 50)), "p99": round(pct(lat, 99))})
        print("  ", rows[-1], flush=True)
    data["queueing"] = rows
    r = {x["utilization"]: x for x in rows}
    report("P99 vs utilization of a single server (same service time)", "the tail explodes as utilization approaches 100%",
           f"P99 at 50% busy {r[0.5]['p99']} ms · 80% {r[0.8]['p99']} ms · 95% {r[0.95]['p99']} ms",
           r[0.95]["p99"] > 5 * r[0.5]["p99"] and r[0.8]["p99"] > r[0.5]["p99"] and r[0.95]["p99"] > r[0.8]["p99"])


def seed_stability():
    """The same model with different seeds: the headline percentiles must not depend on the seed."""
    p99s = []
    for seed in range(10, 15):
        rnd = random.Random(seed)
        p99s.append(round(pct([service(rnd) for _ in range(200_000)], 99)))
    data["seed_stability"] = p99s
    report("Per-call P99 across 5 seeds", "a percentile that isn't on a boundary between modes is stable",
           f"P99 {p99s} ms", max(p99s) - min(p99s) < 0.1 * min(p99s) and min(p99s) > 800)


if __name__ == "__main__":
    seed_stability()
    same_mean()
    fan_out()
    not_additive()
    averaging_instances()
    sample_size()
    queueing()
    with open(__file__.replace("tail.py", "tail_results.json"), "w") as f:
        json.dump(data, f)
    print(f"{sum(results)}/{len(results)} passed")
