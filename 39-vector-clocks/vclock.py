"""Vector clocks, measured for Part 39 (deterministic model of three replicas: web, mobile app, region-2 server).

200 tickets are edited on the three replicas, which sync with each other only sometimes (an agent's phone offline on a
train, a regional link down). Each edit adds one tag to the ticket. Ground truth is tracked exactly: every edit has an
id, and every version knows which edits it includes. After a final full sync, an edit is lost if the converged ticket
doesn't contain it. Replica clocks: the mobile device runs 2 s slow.
"""
import json
import random

results, data = [], {}
REPLICAS = ["web", "mobile", "region2"]
SKEW = {"web": 0.0, "mobile": -2000.0, "region2": 30.0}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def vc_leq(a, b):
    return all(v <= b.get(k, 0) for k, v in a.items())


def vc_merge(a, b):
    return {k: max(a.get(k, 0), b.get(k, 0)) for k in set(a) | set(b)}


class Version:
    def __init__(self, tags, edits, ts=None, vc=None, lamport=None):
        self.tags, self.edits, self.ts, self.vc, self.lamport = frozenset(tags), frozenset(edits), ts, vc or {}, lamport


def run(mode, tickets=200, steps=6000, sync_p=0.08, seed=1):
    """mode: 'lww-wall' | 'lamport' | 'vector'"""
    rnd = random.Random(seed)
    store = {r: {t: [Version(set(), set(), 0.0, {}, (0, ""))] for t in range(tickets)} for r in REPLICAS}   # list = siblings
    lam = {r: 0 for r in REPLICAS}
    t_true, edit_id = 0.0, 0
    issued = {t: set() for t in range(tickets)}
    stats = {"edits": 0, "conflicts_detected": 0, "false_conflicts": 0, "siblings_max": 1}

    def merge_into(dst, tid, incoming):
        nonlocal stats
        cur = store[dst][tid]
        for inc in incoming:
            if mode == "lww-wall":
                if inc.ts > cur[0].ts:
                    cur = [inc]
            elif mode == "lamport":
                if inc.lamport > cur[0].lamport:
                    cur = [inc]
            else:
                if any(vc_leq(inc.vc, c.vc) for c in cur):        # we already have it, or something newer
                    continue
                keep = [c for c in cur if not vc_leq(c.vc, inc.vc)]   # drop versions the incoming one descends from
                if keep:                                          # concurrent with what we hold: keep both as siblings
                    stats["conflicts_detected"] += 1
                    if any(c.edits <= inc.edits or inc.edits <= c.edits for c in keep):
                        stats["false_conflicts"] += 1
                cur = keep + [inc]
        store[dst][tid] = cur
        stats["siblings_max"] = max(stats["siblings_max"], len(cur))

    def sync(a, b):
        for tid in range(tickets):
            merge_into(b, tid, list(store[a][tid]))
            merge_into(a, tid, list(store[b][tid]))

    for _ in range(steps):
        t_true += rnd.expovariate(1 / 50)
        if rnd.random() < sync_p:
            a, b = rnd.sample(REPLICAS, 2)
            sync(a, b)
            for r in (a, b):
                lam[r] = max(lam[a], lam[b])
            continue
        r = rnd.choice(REPLICAS)
        tid = rnd.randrange(20) if rnd.random() < 0.6 else rnd.randrange(tickets)   # some tickets are busy
        cur = store[r][tid]
        edit_id += 1
        stats["edits"] += 1
        issued[tid].add(edit_id)
        # the agent sees the merged value of all siblings (a UI shows a merged view) and adds a tag
        tags = set().union(*(c.tags for c in cur)) | {f"tag{edit_id}"}
        edits = set().union(*(c.edits for c in cur)) | {edit_id}
        vc = {}
        for c in cur:
            vc = vc_merge(vc, c.vc)
        vc[r] = vc.get(r, 0) + 1
        lam[r] = max([lam[r]] + [c.lamport[0] for c in cur]) + 1
        store[r][tid] = [Version(tags, edits, t_true + SKEW[r], vc, (lam[r], r))]     # writing resolves this replica's siblings
    # final anti-entropy: everyone syncs with everyone, twice
    for _ in range(2):
        for a in REPLICAS:
            for b in REPLICAS:
                if a != b:
                    sync(a, b)
    lost = 0
    converged = True
    for tid in range(tickets):
        views = [frozenset().union(*(c.tags for c in store[r][tid])) for r in REPLICAS]
        converged &= len(set(views)) == 1
        final = views[0]                                  # an edit is lost if its tag is missing from the converged ticket
        lost += sum(1 for e in issued[tid] if f"tag{e}" not in final)
    return {"mode": mode, **stats, "lost_edits": lost, "lost_pct": round(100 * lost / stats["edits"], 1), "converged": converged}


def ground_truth_concurrency(seed=7, n=20000):
    """How often are two edits to the same ticket on different replicas concurrent (neither saw the other)? A simple check
    of the comparison rule on random histories: vector order must match history inclusion exactly."""
    rnd = random.Random(seed)
    agree = 0
    for _ in range(n):
        a = {r: rnd.randint(0, 3) for r in REPLICAS}
        b = {r: rnd.randint(0, 3) for r in REPLICAS}
        hist_a = {(r, i) for r, v in a.items() for i in range(1, v + 1)}
        hist_b = {(r, i) for r, v in b.items() for i in range(1, v + 1)}
        rel_v = "eq" if a == b else ("lt" if vc_leq(a, b) else ("gt" if vc_leq(b, a) else "conc"))
        rel_h = "eq" if hist_a == hist_b else ("lt" if hist_a <= hist_b else ("gt" if hist_b <= hist_a else "conc"))
        agree += rel_v == rel_h
    return {"pairs": n, "agree_pct": round(100 * agree / n, 2)}


def metadata(writers):
    """Size of a per-writer vector clock on one popular ticket edited by `writers` distinct agents (8-byte id + 8-byte counter)."""
    return {"writers": writers, "entries": writers, "bytes": writers * 16}


if __name__ == "__main__":
    R = {m: run(m) for m in ("lww-wall", "lamport", "vector")}
    for v in R.values():
        print(v, flush=True)
    G = ground_truth_concurrency()
    M = [metadata(w) for w in (3, 50, 1000, 20000)]
    data.update({"runs": R, "rule_check": G, "metadata": M})
    w, l, v = R["lww-wall"], R["lamport"], R["vector"]
    report("Last-write-wins on wall-clock time (mobile 2 s slow)", "concurrent edits and later edits from the slow device vanish silently",
           f"{w['lost_edits']} of {w['edits']} edits lost ({w['lost_pct']}%)", w["lost_edits"] > 500 and w["lost_pct"] > 15)
    report("Lamport timestamps", "no clock skew problem, but concurrent edits still overwrite each other",
           f"{l['lost_edits']} lost ({l['lost_pct']}%)", 0 < l["lost_edits"] <= w["lost_edits"] and l["lost_pct"] > 10)
    report("Vector clocks with siblings, merged on read", "concurrent edits are detected, kept and merged; nothing lost",
           f"lost {v['lost_edits']}; conflicts detected {v['conflicts_detected']}; false conflicts {v['false_conflicts']}; max siblings {v['siblings_max']}; converged {v['converged']}",
           v["lost_edits"] == 0 and v["false_conflicts"] == 0 and v["conflicts_detected"] > 1000 and v["siblings_max"] >= 2 and v["converged"])
    report("The comparison rule", "a ≤ b exactly when history(a) ⊆ history(b); otherwise concurrent",
           f"vector order matched history order in {G['agree_pct']}% of {G['pairs']:,} random pairs", G["agree_pct"] == 100.0)
    report("Metadata growth with per-writer entries", "one entry per writer: 1,000 agents → 1,000 entries per ticket",
           " · ".join(f"{m['writers']} writers → {m['bytes']:,} B" for m in M),
           M[2]["entries"] == 1000 and M[3]["entries"] == 20000 and M[3]["bytes"] == 320000)
    with open(__file__.replace("vclock.py", "vclock_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
