"""Model routing, measured for Part 53. Small model: Qwen2.5-0.5B-Instruct. Large model: Qwen2.5-3B-Instruct.
Both fp16 on the Apple GPU (MPS), greedy.

46 support-assistant requests with known answers, in three kinds:
- extraction (copy an order number)                 — easy
- help-article choice (pick 1 of 8 articles)        — medium
- proration (seat-change charge, a small calculation) — hard
Policies: everything to small, everything to large, a rule by request type, a confidence cascade (small first,
escalate when its answer's probability is low), and a cascade with the small model failing (circuit breaker → large).
Cost = measured model time (seconds of compute), the stand-in for price.
"""
import json
import random
import re
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
DEV = "mps"
TOK, SMALL = llm.load("Qwen/Qwen2.5-0.5B-Instruct", device=DEV, dtype=torch.float16)
_, LARGE = llm.load("Qwen/Qwen2.5-3B-Instruct", device=DEV, dtype=torch.float16)
rnd = random.Random(7)


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


ARTICLES = ["Refunds and cancellations", "Single sign-on (SAML)", "API rate limits", "Attachments", "Data retention", "Two-factor authentication", "Webhooks", "Invoices and receipts"]
ARTICLE_Q = [("Our Okta login loops back to the login page", 1), ("We get HTTP 429 from your API", 2), ("Can I get my money back for the yearly plan?", 0), ("Files over 40MB won't upload", 3),
             ("How long do you keep deleted tickets?", 4), ("An agent lost the phone with the authenticator app", 5), ("Our CRM stopped receiving ticket events", 6), ("Where is the PDF of last month's bill?", 7),
             ("Can I cancel and get a partial refund?", 0), ("SAML assertion error after the IdP certificate rotated", 1), ("Too many requests error when syncing", 2), ("The image I attached shows as a link", 3),
             ("When is our data deleted after we close the account?", 4), ("2FA codes are rejected", 5), ("Endpoint returns 410 and events stopped", 6), ("I need a VAT invoice", 7)]


def build():
    reqs = []
    for n in rnd.sample(range(10000, 99999), 15):
        reqs.append({"kind": "extraction", "messages": [{"role": "system", "content": "Extract the order number. Reply with the order number only."},
                                                        {"role": "user", "content": f"Hello, my order ORD-{n} never arrived and tracking shows nothing. Please check. Dana"}],
                     "answer": f"ORD-{n}", "max": 10})
    for q, k in ARTICLE_Q:
        reqs.append({"kind": "article", "messages": [{"role": "system", "content": "Pick the help article for the question. Reply with the article number only.\n" +
                                                      "\n".join(f"{i}. {t}" for i, t in enumerate(ARTICLES))}, {"role": "user", "content": q}],
                     "answer": str(k), "max": 3})
    for _ in range(15):
        price, add, days = rnd.choice([12, 15, 18, 20, 24, 30]), rnd.choice([2, 3, 4, 5, 6]), rnd.choice([10, 15, 20])
        ans = price * add * days // 30
        reqs.append({"kind": "proration", "messages": [{"role": "system", "content": "Work it out in one or two short steps, then end with a line 'Answer: <number>'."},
                                                       {"role": "user", "content": f"A seat costs ${price} per 30-day month. The customer adds {add} seats with {days} days left in the month. "
                                                                                   f"Charges are prorated by day. How much do we charge now?"}],
                     "answer": str(ans), "max": 160})
    return reqs


@torch.no_grad()
def ask(model, req):
    ids = llm.chat_ids(TOK, req["messages"], device=DEV)
    torch.mps.synchronize()
    t0 = time.perf_counter()
    out = model.generate(ids, max_new_tokens=req["max"], do_sample=False, output_scores=True, return_dict_in_generate=True, pad_token_id=TOK.eos_token_id)
    torch.mps.synchronize()
    dt = time.perf_counter() - t0
    text = TOK.decode(out.sequences[0][ids.shape[1]:], skip_special_tokens=True).strip()
    probs = [float(torch.softmax(s[0].float(), -1).max()) for s in out.scores]
    gen = out.sequences[0][ids.shape[1]:].tolist()
    if req["kind"] == "proration":                 # confidence of the final answer tokens (after 'Answer:')
        tail = text.rsplit("Answer:", 1)
        n_ans = len(TOK(tail[1])["input_ids"]) if len(tail) == 2 else 0
        conf = min(probs[len(gen) - n_ans - 1:] or [0.0]) if n_ans else 0.0
    else:                                          # confidence: lowest probability over the answer tokens (EOS excluded)
        conf = min(probs[:-1] if len(probs) > 1 and gen[-1] in (TOK.eos_token_id, 151645) else probs)
    return text, dt, conf


def correct(req, text):
    t = text.strip().lower().rstrip(".")
    if req["kind"] == "proration":
        tail = t.rsplit("answer:", 1)
        nums = re.findall(r"\d+(?:\.\d+)?", tail[-1].replace(",", "").replace("$", "")) if len(tail) == 2 else []
        return bool(nums) and abs(float(nums[0]) - float(req["answer"])) < 0.01
    if req["kind"] == "article":
        return t.startswith(req["answer"])
    return t.removeprefix("ord-") == req["answer"].lower().removeprefix("ord-")    # the number, with or without its prefix


if __name__ == "__main__":
    reqs = build()
    ask(SMALL, reqs[0]), ask(LARGE, reqs[0])          # warm up both models
    S, L = [], []
    for r in reqs:
        s = ask(SMALL, r)
        l = ask(LARGE, r)
        S.append({"ok": correct(r, s[0]), "t": s[1], "conf": s[2], "text": s[0]})
        L.append({"ok": correct(r, l[0]), "t": l[1], "conf": l[2], "text": l[0]})
    kinds = ("extraction", "article", "proration")

    def summarize(choice, extra_t=None):
        """choice[i] = 'small' | 'large' | 'both' (small first, then large)."""
        ok, cost, lat, by_kind, large_share = 0, 0.0, [], {k: [0, 0] for k in kinds}, 0
        for i, (r, c) in enumerate(zip(reqs, choice)):
            if c == "small":
                o, t = S[i]["ok"], S[i]["t"]
            elif c == "large":
                o, t = L[i]["ok"], L[i]["t"]
                large_share += 1
            else:
                o, t = L[i]["ok"], S[i]["t"] + L[i]["t"]
                large_share += 1
            ok += o
            cost += t
            lat.append(t)
            by_kind[r["kind"]][0] += o
            by_kind[r["kind"]][1] += 1
        n = len(reqs)
        return {"accuracy_pct": round(100 * ok / n, 1), "cost_s": round(cost, 1), "p50_latency_s": round(sorted(lat)[n // 2], 2), "p95_latency_s": round(sorted(lat)[int(n * .95)], 2),
                "to_large_pct": round(100 * large_share / n, 1), "by_kind": {k: f"{v[0]}/{v[1]}" for k, v in by_kind.items()}}

    P = {"all small": summarize(["small"] * len(reqs)), "all large": summarize(["large"] * len(reqs)),
         "rule: proration → large": summarize(["large" if r["kind"] == "proration" else "small" for r in reqs]),
         "rule: extraction → small, rest → large": summarize(["small" if r["kind"] == "extraction" else "large" for r in reqs])}
    for th in (0.6, 0.8, 0.9):
        P[f"cascade, escalate below {th}"] = summarize(["both" if S[i]["conf"] < th else "small" for i in range(len(reqs))])
    # small model endpoint failing for a third of requests: circuit breaker sends those straight to large
    fail = [rnd.random() < 0.33 for _ in reqs]
    P["rule + small model failing → fallback"] = summarize(["large" if (r["kind"] == "proration" or f) else "small" for r, f in zip(reqs, fail)])
    for k, v in P.items():
        print(k, v, flush=True)
    calib = {}
    for lo, hi in ((0, 0.6), (0.6, 0.9), (0.9, 1.01)):
        sel = [s for s in S if lo <= s["conf"] < hi]
        calib[f"{lo}-{hi}"] = {"n": len(sel), "small_accuracy_pct": round(100 * sum(s["ok"] for s in sel) / max(1, len(sel)), 1)}
    print("calibration", calib, flush=True)
    cost_by_kind = {k: {"small_s": round(sum(S[i]["t"] for i, r in enumerate(reqs) if r["kind"] == k), 1),
                        "large_s": round(sum(L[i]["t"] for i, r in enumerate(reqs) if r["kind"] == k), 1)} for k in kinds}
    print("cost by kind", cost_by_kind, flush=True)
    data.update({"cost_by_kind": cost_by_kind, "policies": P, "calibration": calib, "n": len(reqs), "small_mean_s": round(sum(s["t"] for s in S) / len(S), 2), "large_mean_s": round(sum(l["t"] for l in L) / len(L), 2)})
    a_s, a_l, rule = P["all small"], P["all large"], P["rule: proration → large"]
    report("The small model is cheaper and worse on hard requests", "quality depends on the request type, not only the model",
           f"small {a_s['accuracy_pct']}% ({a_s['by_kind']}), cost {a_s['cost_s']} s · large {a_l['accuracy_pct']}% ({a_l['by_kind']}), cost {a_l['cost_s']} s",
           a_s["cost_s"] < 0.5 * a_l["cost_s"] and a_s["accuracy_pct"] < a_l["accuracy_pct"] - 20
           and a_s["by_kind"]["extraction"].startswith("15/") and a_s["by_kind"]["proration"].startswith("0/"))
    r2 = P["rule: extraction → small, rest → large"]
    report("Rule-based routing by request type", "send what the small model handles to it, the rest to the large model",
           f"extraction → small: {r2['accuracy_pct']}% at {r2['cost_s']} s ({r2['to_large_pct']}% to large) · only proration → large: {rule['accuracy_pct']}% at {rule['cost_s']} s · all-large {a_l['accuracy_pct']}% at {a_l['cost_s']} s",
           r2["accuracy_pct"] >= a_l["accuracy_pct"] - 1 and r2["cost_s"] < a_l["cost_s"] and rule["accuracy_pct"] > a_s["accuracy_pct"])
    best = max((k for k in P if k.startswith("cascade")), key=lambda k: P[k]["accuracy_pct"] - 0.02 * P[k]["cost_s"])
    report("Confidence cascade", "escalating low-confidence answers buys accuracy back — but escalated requests pay for both models",
           " · ".join(f"{k.split('below ')[1]}: {v['accuracy_pct']}%, {v['cost_s']} s, {v['to_large_pct']}% escalated, p95 {v['p95_latency_s']} s" for k, v in P.items() if k.startswith("cascade")),
           any(P[k]["accuracy_pct"] > a_s["accuracy_pct"] + 15 for k in P if k.startswith("cascade"))
           and all(P[k]["cost_s"] > a_l["cost_s"] for k in P if k.startswith("cascade") and P[k]["to_large_pct"] > 60))
    report("Small model's confidence vs correctness", "confidence is a usable but imperfect routing signal",
           " · ".join(f"conf {k}: {v['n']} answers, {v['small_accuracy_pct']}% right" for k, v in calib.items()),
           calib["0.6-0.9"]["small_accuracy_pct"] >= calib["0-0.6"]["small_accuracy_pct"] and calib["0-0.6"]["n"] > calib["0.6-0.9"]["n"])
    fb = P["rule + small model failing → fallback"]
    report("Fallback when a model is unavailable", "a circuit breaker keeps answering, at higher cost", f"accuracy {fb['accuracy_pct']}%, cost {fb['cost_s']} s, {fb['to_large_pct']}% on large",
           fb["accuracy_pct"] >= rule["accuracy_pct"] - 5 and fb["to_large_pct"] > rule["to_large_pct"])

    with open(__file__.replace("routing.py", "routing_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
