"""Continuous batching, measured for Part 50.

Step 1 (real model): Qwen2.5-0.5B-Instruct on CPU — time of one batched decode step for batch sizes 1..32, and prefill
time per prompt token. Step 2 (scheduler simulation driven by those measured costs): 200 requests arrive (Poisson);
output lengths vary widely (most short replies, some long drafts). Static batching vs continuous batching.
"""
import json
import random
import sys
import time
from pathlib import Path

import torch

_here = Path(__file__).resolve().parent
for _cand in (_here, _here.parent / "shared", _here.parents[1] / "shared", _here.parents[2] / "shared"):
    if (_cand / "llm.py").exists():
        sys.path.insert(0, str(_cand))
        break
import llm  # noqa: E402


results, data = [], {}
torch.set_num_threads(8)


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


@torch.no_grad()
def measure_steps(sizes=(1, 2, 4, 8, 16, 32), ctx=128, reps=8):
    tok, model = llm.load()
    ids = tok("The customer reported that agents cannot log in after the update. " * 40, return_tensors="pt")["input_ids"][:, :ctx]
    out = {}
    for b in sizes:
        x = ids.repeat(b, 1)
        o = model(x, use_cache=True)
        past, nxt = o.past_key_values, o.logits[:, -1:].argmax(-1)
        ts = []
        for _ in range(reps):
            t0 = time.perf_counter()
            o = model(nxt, past_key_values=past, use_cache=True)
            ts.append(time.perf_counter() - t0)
            past, nxt = o.past_key_values, o.logits[:, -1:].argmax(-1)
        out[b] = sorted(ts)[reps // 2]
    t0 = time.perf_counter()
    model(ids[:, :128])
    prefill_per_tok = (time.perf_counter() - t0) / 128
    return out, prefill_per_tok


def step_time(b, table):
    """Interpolate the measured step time for batch size b."""
    ks = sorted(table)
    if b <= ks[0]:
        return table[ks[0]]
    for lo, hi in zip(ks, ks[1:]):
        if b <= hi:
            return table[lo] + (table[hi] - table[lo]) * (b - lo) / (hi - lo)
    return table[ks[-1]] * b / ks[-1]


def workload(n=200, rate=1.2, seed=4):
    rnd = random.Random(seed)
    t, reqs = 0.0, []
    for i in range(n):
        t += rnd.expovariate(rate)
        out = rnd.randint(10, 40) if rnd.random() < 0.8 else rnd.randint(200, 400)     # short replies, some long drafts
        reqs.append({"id": i, "arrive": t, "prompt": rnd.randint(80, 200), "out": out})
    return reqs


def simulate(reqs, table, prefill_tok, mode, max_batch=16):
    clock, i, active, waiting, done = 0.0, 0, [], [], []
    useful_slots = total_slots = 0
    reqs = [dict(r) for r in reqs]
    while len(done) < len(reqs):
        while i < len(reqs) and reqs[i]["arrive"] <= clock:
            waiting.append(reqs[i])
            i += 1
        if not active and not waiting:
            clock = reqs[i]["arrive"]
            continue
        admit = []
        if mode == "continuous" or (mode == "static" and not active):
            while waiting and len(active) + len(admit) < max_batch:
                admit.append(waiting.pop(0))
        if admit:                                                     # prefill the newcomers (one batched prefill)
            clock += sum(r["prompt"] for r in admit) * prefill_tok
            for r in admit:
                r["first"] = clock
                r["left"] = r["out"] - 1
            active += admit
            if mode == "static":
                batch_size = len(active)
        b = len(active) if mode == "continuous" else batch_size
        clock += step_time(b, table)                                  # static: finished slots still occupy the batch
        total_slots += max_batch if mode == "static" else max_batch
        useful_slots += len(active)
        for r in active:
            r["left"] -= 1
            if r["left"] <= 0:
                r["end"] = clock
        finished = [r for r in active if r["left"] <= 0]
        active = [r for r in active if r["left"] > 0]
        done += finished
    lat = sorted(r["end"] - r["arrive"] for r in done)
    ttft = sorted(r["first"] - r["arrive"] for r in done)
    short = sorted(r["end"] - r["arrive"] for r in done if r["out"] <= 40)
    makespan = max(r["end"] for r in done) - reqs[0]["arrive"]
    toks = sum(r["out"] for r in done)
    return {"mode": mode, "throughput_tok_s": round(toks / makespan, 1), "ttft_p50_s": round(ttft[len(ttft) // 2], 2), "ttft_p99_s": round(ttft[int(len(ttft) * .99)], 2),
            "latency_short_p50_s": round(short[len(short) // 2], 2), "latency_p99_s": round(lat[int(len(lat) * .99)], 2), "slot_use_pct": round(100 * useful_slots / total_slots, 1),
            "makespan_s": round(makespan, 1)}


if __name__ == "__main__":
    table, pf = measure_steps()
    print({b: round(1000 * t, 1) for b, t in table.items()}, "prefill ms/token", round(1000 * pf, 2), flush=True)
    per_seq = {b: round(b / t, 1) for b, t in table.items()}
    reqs = workload()
    S = simulate(reqs, table, pf, "static")
    C = simulate(reqs, table, pf, "continuous")
    one = simulate(reqs, table, pf, "continuous", max_batch=1)
    print(S, C, one, sep="\n", flush=True)
    data.update({"step_ms": {b: round(1000 * t, 1) for b, t in table.items()}, "tokens_per_s_by_batch": per_seq, "prefill_ms_per_token": round(1000 * pf, 2),
                 "static": S, "continuous": C, "unbatched": one, "workload": {"requests": len(reqs), "short_share": 0.8}})
    report("Batching decode steps", "a step for 16 sequences costs far less than 16 single steps — decode is memory-bound",
           f"step: {data['step_ms'][1]} ms (1 seq) vs {data['step_ms'][16]} ms (16 seqs) → {per_seq[1]} vs {per_seq[16]} tokens/s", per_seq[16] > 5 * per_seq[1])
    report("Static batching", "finished sequences hold their slot until the longest one ends; newcomers wait for the whole batch",
           f"slots doing useful work {S['slot_use_pct']}%; short replies p50 {S['latency_short_p50_s']} s; TTFT p99 {S['ttft_p99_s']} s", S["slot_use_pct"] < 40)
    report("Continuous batching", "finished sequences leave and waiting ones join at every step",
           f"throughput {C['throughput_tok_s']} vs {S['throughput_tok_s']} tok/s; short replies p50 {C['latency_short_p50_s']} vs {S['latency_short_p50_s']} s; TTFT p99 {C['ttft_p99_s']} vs {S['ttft_p99_s']} s",
           C["latency_short_p50_s"] < S["latency_short_p50_s"] / 5 and C["ttft_p99_s"] < S["ttft_p99_s"] / 5 and C["throughput_tok_s"] > 1.5 * S["throughput_tok_s"])
    report("No batching at all", "the GPU (here CPU) does one sequence at a time; queues explode", f"throughput {one['throughput_tok_s']} tok/s, latency p99 {one['latency_p99_s']} s",
           one["throughput_tok_s"] < C["throughput_tok_s"] / 2 and one["latency_p99_s"] > 5 * C["latency_p99_s"])

    with open(__file__.replace("batching.py", "batching_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
