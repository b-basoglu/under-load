"""Consistent hashing, measured for Part 36.

Placement math on 100,000 keys, plus a real cache: five Redis 7.4 nodes (ports 56380–56384) used as a client-side
sharded cache for ticket pages. Compares hash mod N, a hash ring with 1 token per node, a ring with virtual nodes,
jump consistent hash and rendezvous hashing.
"""
import bisect
import hashlib
import json
import random
import statistics

import redis

NODES = [f"cache-{i}" for i in range(5)]
CLIENTS = {f"cache-{i}": redis.Redis(port=56380 + i) for i in range(5)}
KEYS = [f"ticket:{i}" for i in range(100_000)]
results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def h64(s):
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:8], "big")


class ModN:
    def __init__(self, nodes, **_):
        self.nodes = list(nodes)

    def owner(self, key):
        return self.nodes[h64(key) % len(self.nodes)]


class Ring:
    def __init__(self, nodes, tokens=1):
        self.points = sorted((h64(f"{n}#{t}"), n) for n in nodes for t in range(tokens))
        self.hashes = [p for p, _ in self.points]

    def owner(self, key):
        i = bisect.bisect(self.hashes, h64(key)) % len(self.points)
        return self.points[i][1]

    def owners(self, key, rf):
        """Replica set: walk clockwise, skipping tokens of nodes already chosen."""
        i, out = bisect.bisect(self.hashes, h64(key)), []
        while len(out) < rf:
            n = self.points[i % len(self.points)][1]
            if n not in out:
                out.append(n)
            i += 1
        return out


class Jump:
    """Jump consistent hash (Lamping & Veach): buckets 0..N-1; only the last bucket can be added or removed."""
    def __init__(self, nodes, **_):
        self.nodes = list(nodes)

    def owner(self, key):
        k, b, j = h64(key), -1, 0
        while j < len(self.nodes):
            b = j
            k = (k * 2862933555777941757 + 1) % (1 << 64)
            j = int((b + 1) * ((1 << 31) / ((k >> 33) + 1)))
        return self.nodes[b]


class Rendezvous:
    """Highest random weight: every node scores the key; the highest score wins. O(N) per lookup."""
    def __init__(self, nodes, **_):
        self.nodes = list(nodes)

    def owner(self, key):
        return max(self.nodes, key=lambda n: h64(f"{n}|{key}"))


def placement(cls, **kw):
    before, after = cls(NODES[:4], **kw), cls(NODES, **kw)
    a = [before.owner(k) for k in KEYS]
    b = [after.owner(k) for k in KEYS]
    moved = sum(x != y for x, y in zip(a, b))
    load = [b.count(n) for n in NODES]
    return {"moved_pct": round(100 * moved / len(KEYS), 1), "max_over_mean": round(max(load) / statistics.mean(load), 2),
            "min_over_mean": round(min(load) / statistics.mean(load), 2), "load": load}


def removal(tokens):
    """Remove cache-2 from a 5-node ring: who absorbs its keys?"""
    full, less = Ring(NODES, tokens), Ring([n for n in NODES if n != "cache-2"], tokens)
    gained = {n: 0 for n in NODES if n != "cache-2"}
    before = {n: 0 for n in NODES}
    for k in KEYS:
        o = full.owner(k)
        before[o] += 1
        if o == "cache-2":
            gained[less.owner(k)] += 1
    mean_before = statistics.mean(before.values())
    worst = max(gained, key=gained.get)
    return {"tokens": tokens, "absorbed_by": gained, "worst_node_growth_pct": round(100 * gained[worst] / max(1, before[worst]), 0),
            "nodes_receiving": sum(1 for v in gained.values() if v > 0)}


def cache_experiment(cls, **kw):
    """Warm the 4-node cache, add a 5th node, read every key once with the new routing."""
    for c in CLIENTS.values():
        c.flushall()
    old = cls(NODES[:4], **kw)
    pipes = {n: CLIENTS[n].pipeline(transaction=False) for n in NODES}
    for k in KEYS:
        pipes[old.owner(k)].set(k, "rendered ticket page")
    for p in pipes.values():
        p.execute()
    new = cls(NODES, **kw)
    by_node = {n: [] for n in NODES}
    for k in KEYS:
        by_node[new.owner(k)].append(k)
    hits = 0
    for n, ks in by_node.items():
        p = CLIENTS[n].pipeline(transaction=False)
        for k in ks:
            p.exists(k)
        hits += sum(p.execute())
    return {"hit_pct_after_adding_node": round(100 * hits / len(KEYS), 1), "db_loads": len(KEYS) - hits}


def hot_key(tokens=160, requests=200_000, seed=3):
    rnd = random.Random(seed)
    ring = Ring(NODES, tokens)
    load = {n: 0 for n in NODES}
    for _ in range(requests):
        k = "ticket:42" if rnd.random() < 0.10 else rnd.choice(KEYS)       # one escalated ticket everyone is watching
        load[ring.owner(k)] += 1
    return {"share_pct": {n: round(100 * v / requests, 1) for n, v in load.items()}, "hot_owner": ring.owner("ticket:42")}


if __name__ == "__main__":
    P = {"mod N": placement(ModN), "ring, 1 token": placement(Ring, tokens=1), "ring, 10 tokens": placement(Ring, tokens=10),
         "ring, 160 tokens": placement(Ring, tokens=160), "jump hash": placement(Jump), "rendezvous": placement(Rendezvous)}
    for k, v in P.items():
        print(k, v, flush=True)
    RM = {t: removal(t) for t in (1, 160)}
    print(RM, flush=True)
    C = {"mod N": cache_experiment(ModN), "ring, 160 tokens": cache_experiment(Ring, tokens=160)}
    print(C, flush=True)
    HK = hot_key()
    print(HK, flush=True)
    rs = Ring(NODES, 160)
    replicas = [rs.owners(k, 3) for k in KEYS[:1000]]
    distinct = all(len(set(r)) == 3 for r in replicas)
    data.update({"placement": P, "removal": {str(k): v for k, v in RM.items()}, "cache": C, "hot_key": HK, "replicas_distinct": distinct})
    m, r1, r160, j, rz = P["mod N"], P["ring, 1 token"], P["ring, 160 tokens"], P["jump hash"], P["rendezvous"]
    report("hash mod N, 4 → 5 nodes", "most keys change owner", f"{m['moved_pct']}% of keys moved", m["moved_pct"] > 75)
    report("Hash ring, 4 → 5 nodes", "only the keys the new node takes over move (~1/5)",
           f"1 token: {r1['moved_pct']}% · 160 tokens: {r160['moved_pct']}% · jump {j['moved_pct']}% · rendezvous {rz['moved_pct']}%",
           r160["moved_pct"] < 25 and r160["moved_pct"] < r1["moved_pct"] and j["moved_pct"] < 25 and rz["moved_pct"] < 25)
    report("Balance with one token per node", "a few nodes produce very uneven ranges", f"busiest node holds {r1['max_over_mean']}× the mean, smallest {r1['min_over_mean']}×",
           r1["max_over_mean"] > 1.5)
    report("Balance with 160 virtual nodes per node", "many small ranges average out", f"busiest {r160['max_over_mean']}× the mean, smallest {r160['min_over_mean']}×",
           r160["max_over_mean"] < 1.15 and r160["min_over_mean"] > 0.85)
    a, b = RM[1], RM[160]
    report("Removing a node", "with one token, one neighbour takes everything; with virtual nodes the load spreads",
           f"1 token: {a['nodes_receiving']} node(s) absorb it, worst grows {a['worst_node_growth_pct']:.0f}% · 160 tokens: {b['nodes_receiving']} nodes, worst grows {b['worst_node_growth_pct']:.0f}%",
           a["nodes_receiving"] == 1 and b["nodes_receiving"] == 4)
    cm, cr = C["mod N"], C["ring, 160 tokens"]
    report("Real cache: add a 5th Redis node", "mod N turns most of the cache into misses at once",
           f"hit rate right after: mod N {cm['hit_pct_after_adding_node']}% ({cm['db_loads']:,} DB loads) vs ring {cr['hit_pct_after_adding_node']}% ({cr['db_loads']:,})",
           cm["hit_pct_after_adding_node"] < 25 and cr["hit_pct_after_adding_node"] > 75)
    report("Hot key", "consistent hashing balances keys, not traffic: the hot key's node is still overloaded",
           f"traffic share per node {HK['share_pct']}", max(HK["share_pct"].values()) > 25)
    report("Replica placement on the ring", "walking clockwise and skipping repeated nodes gives 3 distinct nodes", f"all 1,000 replica sets distinct: {distinct}", distinct)
    with open(__file__.replace("chash.py", "chash_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
