"""Re-derives Part 51's checks from the saved measurements, using the measured verification cost (verify.py).
Refined model: speedup = E[tokens per round] / (k·c + v(k+1)), with v(n) = cost of scoring n tokens in one target pass,
in target decode steps."""
import json

S = json.load(open("speculative_results.json"))
V = json.load(open("verify_results.json"))
results = []


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def tokens(a, k):
    return (1 - a ** (k + 1)) / (1 - a)


def v(dev, n):
    row = {int(k): x for k, x in V[dev].items()}
    ks = sorted(row)
    near = min(ks, key=lambda x: abs(x - n))
    return row[near] / row[1]


C, R, K = S["costs"], S["tasks"], S["k_sweep"]
c = C["cost_ratio"]
refined = {n: round(tokens(r["acceptance_0.5B"], 4) / (4 * c + v("cpu", 5)), 2) for n, r in R.items()}
refined_k = {k: round(tokens(R["explain"]["acceptance_0.5B"], int(k)) / (int(k) * c + v("cpu", int(k) + 1)), 2) for k in K}
mps_c = V["mps_draft_step_ms"] / V["mps"]["1"]
mps_pred = round(tokens(R["explain"]["acceptance_0.5B"], 4) / (4 * mps_c + v("mps", 5)), 2)
S["refined"] = {"cpu_verify_5_steps": round(v("cpu", 5), 2), "mps_verify_5_steps": round(v("mps", 5), 2), "mps_cost_ratio": round(mps_c, 2),
                "by_task": refined, "by_k": refined_k, "mps_predicted": mps_pred}
json.dump(S, open("speculative_results.json", "w"))

report("Speculative output is exactly the target's", "verification keeps every token the target would have produced",
       f"identical greedy output on all tasks and methods: {all(r[k] for r in R.values() for k in r if k.endswith('_identical'))}",
       all(r[k] for r in R.values() for k in r if k.endswith("_identical")))
report("Acceptance rate decides the gain", "a draft that disagrees with the target makes generation much slower",
       " · ".join(f"{n}: 0.5B agrees {r['acceptance_0.5B']:.0%} → {r['draft_0.5B_k4_speedup']}×, 6-layer {r['acceptance_weak']:.0%} → {r['weak_draft_k4_speedup']}×" for n, r in R.items()),
       all(r["weak_draft_k4_speedup"] < 0.5 * r["draft_0.5B_k4_speedup"] and r["acceptance_0.5B"] > 0.7 for r in R.values()))
report("The naive model assumes verification costs one step", "it predicts a speedup this laptop doesn't deliver",
       " · ".join(f"{n}: naive {r['predicted_0.5B_k4']}× vs measured {r['draft_0.5B_k4_speedup']}×" for n, r in R.items()),
       all(r["predicted_0.5B_k4"] > 1.5 * r["draft_0.5B_k4_speedup"] for r in R.values()))
report("Measured verification cost explains most of the gap", "the refined prediction is closer to the measurement than the naive one, for every task",
       f"verify 5 tokens: CPU {S['refined']['cpu_verify_5_steps']} steps, MPS {S['refined']['mps_verify_5_steps']} steps; refined prediction " +
       " · ".join(f"{n} {refined[n]}× vs {R[n]['draft_0.5B_k4_speedup']}×" for n in R),
       all(abs(refined[n] - R[n]["draft_0.5B_k4_speedup"]) < abs(R[n]["predicted_0.5B_k4"] - R[n]["draft_0.5B_k4_speedup"]) for n in R)
       and S["refined"]["cpu_verify_5_steps"] > 2)
report("On the GPU, the draft is too expensive relative to the target", "0.5B costs about half a 1.5B step at batch 1",
       f"MPS: draft/target step {S['refined']['mps_cost_ratio']}, refined prediction {mps_pred}×, measured {S['mps']['speedup']}×",
       mps_pred < 1.2 and S["mps"]["speedup"] < 1.0 and 0.4 < S["refined"]["mps_cost_ratio"] < 0.7)
h, e = R["handoff"], R["explain"]
report("Prompt lookup favours output that copies the input", "no draft model; more drafts accepted when the output repeats the prompt",
       f"handoff note {h['prompt_lookup_speedup']}× vs explanation {e['prompt_lookup_speedup']}× (both limited by the CPU verification cost)",
       h["prompt_lookup_speedup"] > e["prompt_lookup_speedup"] and h["prompt_lookup_speedup"] >= 1.0)
ks = sorted(K, key=int)
report("Draft length on this CPU", "with costly verification, longer drafts amortize it: speedup rises with k (no sweet spot up to k=10)",
       " · ".join(f"k={k}: {K[k]['speedup']}×" for k in ks),
       K[ks[-1]]["speedup"] > K[ks[0]]["speedup"] and K[ks[-1]]["speedup"] > 0.9)
report("Where it pays off", "big target, tiny draft, GPU-like verification: predicted speedup", json.dumps(S["projection"]),
       S["projection"]["1B → 70B"]["alpha 0.8"] > 2.5)

print(f"{sum(results)}/{len(results)} passed")
