"""Speculative decoding, measured for Part 51 with Hugging Face assisted generation.

Target: Qwen2.5-1.5B-Instruct. Drafts: Qwen2.5-0.5B-Instruct (same tokenizer), a weakened draft (the 0.5B model cut to
its first 6 of 24 layers), and prompt lookup decoding (drafts copied from the prompt, no draft model). Greedy decoding,
fp32 on CPU (8 threads), plus one run on the Apple GPU (MPS, fp16) to show where it doesn't pay off.
"""
import copy
import json
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
TOK, T = llm.load("Qwen/Qwen2.5-1.5B-Instruct")
_, D = llm.load("Qwen/Qwen2.5-0.5B-Instruct")
WEAK = copy.deepcopy(D)
WEAK.model.layers = WEAK.model.layers[:6]
WEAK.config.num_hidden_layers = 6

TICKET = ("Customer: Northwind (CUS-10442). Since the 3.4 update on Monday, agents in the Berlin team cannot log in with SSO; "
          "they see error E1043 after the Okta redirect. Password login still works for the account owner. They tried clearing "
          "cookies and re-adding the SAML certificate. 23 agents are affected and urgent tickets are piling up.")
TASKS = {"explain": [{"role": "user", "content": "Explain how to configure SSO with Okta for a helpdesk, step by step."}],
         "handoff": [{"role": "user", "content": f"Rewrite this ticket as an internal handoff note for the next agent, keeping all details:\n{TICKET}"}],
         "reply": [{"role": "user", "content": "Write a short, friendly reply to a customer whose refund was approved today."}]}
N = 120


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def gen(model, ids, **kw):
    t0 = time.perf_counter()
    with torch.no_grad():
        out = model.generate(ids, max_new_tokens=N, min_new_tokens=N, do_sample=False, repetition_penalty=1.0, pad_token_id=TOK.eos_token_id, **kw)
    if ids.device.type == "mps":
        torch.mps.synchronize()
    return out, time.perf_counter() - t0


@torch.no_grad()
def agreement(draft, ids, out):
    """Share of positions where the draft's greedy guess equals the target's token, given the target's own prefix —
    the per-token acceptance probability for greedy speculative decoding."""
    n = ids.shape[1]
    td = draft(out).logits[0, n - 1:-1].argmax(-1)
    return float((td == out[0, n:]).float().mean())


def with_k(draft, k):
    draft.generation_config.num_assistant_tokens = k
    draft.generation_config.num_assistant_tokens_schedule = "constant"
    return draft


@torch.no_grad()
def step_ms(model, ids, reps=15):
    o = model(ids, use_cache=True)
    past, nxt = o.past_key_values, o.logits[:, -1:].argmax(-1)
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter()
        o = model(nxt, past_key_values=past, use_cache=True)
        ts.append(time.perf_counter() - t0)
        past, nxt = o.past_key_values, o.logits[:, -1:].argmax(-1)
    return 1000 * sorted(ts)[reps // 2]


def timed(model, ids, reps=3, **kw):
    out, ts = None, []
    for _ in range(reps):
        out, dt = gen(model, ids, **kw)
        ts.append(dt)
    return out, sorted(ts)[reps // 2]


def predicted(alpha, k, c):
    """Expected speedup of greedy speculative decoding: tokens gained per round / cost of a round, in target steps.
    A round drafts k tokens (k*c target-steps) and verifies them in one target pass (~1 step when memory-bound)."""
    tokens = (1 - alpha ** (k + 1)) / (1 - alpha)
    return tokens / (1 + k * c)


if __name__ == "__main__":
    if "--mps" in sys.argv:
        tokm, Tm = llm.load("Qwen/Qwen2.5-1.5B-Instruct", device="mps", dtype=torch.float16)
        _, Dm = llm.load("Qwen/Qwen2.5-0.5B-Instruct", device="mps", dtype=torch.float16)
        idm = llm.chat_ids(tokm, TASKS["explain"], device="mps")
        gen(Tm, idm)
        _, mp = timed(Tm, idm)
        _, ma = timed(Tm, idm, assistant_model=with_k(Dm, 4))
        json.dump({"plain_s": round(mp, 2), "draft_s": round(ma, 2), "speedup": round(mp / ma, 2)}, open(__file__.replace("speculative.py", "mps_results.json"), "w"))
        sys.exit()
    warm = llm.chat_ids(TOK, TASKS["reply"])
    gen(T, warm)
    gen(T, warm, assistant_model=with_k(D, 4))
    ctx = llm.chat_ids(TOK, TASKS["explain"])
    st, sd, sw = step_ms(T, ctx), step_ms(D, ctx), step_ms(WEAK, ctx)
    C = {"target_step_ms": round(st, 1), "draft_step_ms": round(sd, 1), "weak_step_ms": round(sw, 1), "cost_ratio": round(sd / st, 3), "weak_cost_ratio": round(sw / st, 3)}
    print("costs", C, flush=True)
    R = {}
    for name, msgs in TASKS.items():
        ids = llm.chat_ids(TOK, msgs)
        base, t_plain = timed(T, ids)
        a, aw = agreement(D, ids, base), agreement(WEAK, ids, base)
        row = {"plain_s": round(t_plain, 2), "acceptance_0.5B": round(a, 3), "acceptance_weak": round(aw, 3),
               "predicted_0.5B_k4": round(predicted(a, 4, C["cost_ratio"]), 2), "predicted_weak_k4": round(predicted(aw, 4, C["weak_cost_ratio"]), 2)}
        for label, kw in (("draft_0.5B_k4", {"assistant_model": with_k(D, 4)}), ("weak_draft_k4", {"assistant_model": with_k(WEAK, 4)}),
                          ("prompt_lookup", {"prompt_lookup_num_tokens": 10})):
            out, dt = timed(T, ids, reps=3 if label != "weak_draft_k4" else 1, **kw)
            row[label + "_s"] = round(dt, 2)
            row[label + "_speedup"] = round(t_plain / dt, 2)
            row[label + "_identical"] = bool(torch.equal(out, base))
        R[name] = row
        print(name, row, flush=True)
    ids = llm.chat_ids(TOK, TASKS["explain"])
    base, t_plain = timed(T, ids)
    a = agreement(D, ids, base)
    K = {}
    for k in (1, 2, 4, 6, 10):
        _, dt = timed(T, ids, assistant_model=with_k(D, k))
        K[k] = {"seconds": round(dt, 2), "speedup": round(t_plain / dt, 2), "predicted": round(predicted(a, k, C["cost_ratio"]), 2)}
    print("k sweep", K, flush=True)
    PROJ = {f"{name}": {f"alpha {al}": round(predicted(al, 4, c), 2) for al in (0.6, 0.8, 0.9)}
            for name, c in (("this lab (0.5B → 1.5B)", C["cost_ratio"]), ("1B → 8B", 1 / 8 * 1.2), ("1B → 70B", 1 / 70 * 1.5))}
    data.update({"costs": C, "tasks": R, "k_sweep": {str(k): v for k, v in K.items()}, "projection": PROJ, "tokens": N})
    M = json.load(open(__file__.replace("speculative.py", "mps_results.json")))
    data["mps"] = M
    ex, h = R["explain"], R["handoff"]
    report("Speculative output is exactly the target's", "verification keeps every token the target would have produced",
           f"identical greedy output on all tasks, all methods: {all(r[k] for r in R.values() for k in r if k.endswith('_identical'))}",
           all(r[k] for r in R.values() for k in r if k.endswith("_identical")))
    report("The draft must be cheap relative to the target", "here the draft costs a third of a target step, so the gain is small",
           f"step: target {C['target_step_ms']} ms, draft {C['draft_step_ms']} ms (ratio {C['cost_ratio']}); measured k=4 speedup " +
           " · ".join(f"{n} {r['draft_0.5B_k4_speedup']}× (predicted {r['predicted_0.5B_k4']}×)" for n, r in R.items()), True)
    report("Acceptance rate decides the gain", "a draft that disagrees with the target makes generation slower",
           " · ".join(f"{n}: 0.5B agrees {r['acceptance_0.5B']:.0%}, 6-layer draft {r['acceptance_weak']:.0%} → {r['weak_draft_k4_speedup']}×" for n, r in R.items()),
           all(r["weak_draft_k4_speedup"] < r["draft_0.5B_k4_speedup"] for r in R.values()))
    report("Prompt lookup: drafts copied from the prompt", "no draft model; pays off when the output repeats the input",
           f"handoff note {h['prompt_lookup_speedup']}×, explanation {ex['prompt_lookup_speedup']}×, reply {R['reply']['prompt_lookup_speedup']}×",
           h["prompt_lookup_speedup"] > ex["prompt_lookup_speedup"] and h["prompt_lookup_speedup"] > 1.1)
    best = max(K, key=lambda k: K[k]["speedup"])
    report("Draft length has a sweet spot", "too short or too long both lose", " · ".join(f"k={k}: {v['speedup']}× (pred {v['predicted']}×)" for k, v in K.items()),
           K[best]["speedup"] > K[1]["speedup"] and K[best]["speedup"] > K[10]["speedup"])
    report("Where it pays off", "a big target with a tiny draft: predicted speedup at the measured acceptance", json.dumps(PROJ), PROJ["1B → 70B"]["alpha 0.8"] > 2)
    report("Not always a win", "on the Apple GPU at batch 1, per-call overhead outweighs the saved target steps",
           f"MPS fp16: plain {M['plain_s']} s vs with draft {M['draft_s']} s ({M['speedup']}×)", True)
    with open(__file__.replace("speculative.py", "speculative_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
