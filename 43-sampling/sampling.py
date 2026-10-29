"""Sampling, measured for Part 43 with a real model: Qwen2.5-0.5B-Instruct (see ../../shared/llm.py).

1. The next-token distribution for a ticket-triage prompt, reshaped by temperature; what top-k and top-p keep.
2. Ticket priority classification (40 labelled tickets): accuracy and invalid answers vs temperature.
3. Extracting an order number: exact matches vs temperature.
4. Subject-line suggestions: how different the samples are vs temperature.
"""
import json
import math
import re
import sys
from pathlib import Path

import torch

_here = Path(__file__).resolve().parent
for _cand in (_here, _here.parent / "shared", _here.parents[1] / "shared", _here.parents[2] / "shared"):
    if (_cand / "llm.py").exists():
        sys.path.insert(0, str(_cand))
        break
import llm  # noqa: E402

results, data = [], {}
torch.manual_seed(0)


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


TICKETS = [
    ("Nobody in our company can log in since 9am, the whole support team is blocked", "urgent"),
    ("Production API returns 500 for every request, customers cannot check out", "urgent"),
    ("We were charged twice this month and our finance team needs it fixed today", "high"),
    ("The SSO integration stopped working for new employees after your update", "high"),
    ("Can you change the color of the button on our help center?", "low"),
    ("How do I export last month's tickets to CSV?", "medium"),
    ("Typo on the pricing page: 'anual' instead of 'annual'", "low"),
    ("Our data seems to be visible to another customer, please investigate immediately", "urgent"),
    ("Email notifications are delayed by about an hour for some agents", "high"),
    ("Please add a dark mode to the agent workspace", "low"),
    ("Search results are slow, around 5 seconds, but they do load", "medium"),
    ("The mobile app crashes when attaching a photo on Android 14", "high"),
    ("Where can I find the invoice for March?", "medium"),
    ("All tickets disappeared from our dashboard after the deployment", "urgent"),
    ("Could you translate the widget into Portuguese?", "low"),
    ("Two agents cannot reset their passwords, the reset email never arrives", "high"),
    ("I would like a demo of the analytics features", "low"),
    ("Webhooks to our CRM failed for the last 3 hours, we are losing leads", "urgent"),
    ("The CSV import skips rows with accents in the name", "medium"),
    ("Is there an API rate limit and what is it?", "medium"),
    ("Payment page is down, customers cannot pay their invoices", "urgent"),
    ("Small alignment issue in the ticket sidebar on Safari", "low"),
    ("Agents in Germany see times in the wrong time zone", "medium"),
    ("Our account was suspended by mistake and nobody can work", "urgent"),
    ("Can we get a copy of your security whitepaper?", "low"),
    ("Macros stopped applying tags since this morning", "high"),
    ("The report total does not match the number of tickets", "medium"),
    ("Please update our billing address", "low"),
    ("Attachments over 10 MB fail to upload for everyone", "high"),
    ("Customer replies are not being threaded into existing tickets", "high"),
    ("What is the difference between the Pro and Enterprise plans?", "low"),
    ("SLA timers show negative values on some tickets", "medium"),
    ("Data export contains other tenants' email addresses", "urgent"),
    ("The chatbot answers in English to Spanish customers", "medium"),
    ("Two-factor authentication codes are rejected for all users", "urgent"),
    ("Please add an emoji picker to replies", "low"),
    ("Ticket assignment rules route everything to one agent", "high"),
    ("Can I rename a custom field without losing data?", "medium"),
    ("Our status page integration shows an outage that is not real", "medium"),
    ("Logo looks blurry on retina screens", "low"),
]
LABELS = ("low", "medium", "high", "urgent")


def classify_messages(text):
    return [{"role": "system", "content": "You triage support tickets. Reply with exactly one word: low, medium, high or urgent.\n"
             "urgent = many or all users blocked, payments down, or customer data exposed.\n"
             "high = an important feature is broken for some users.\n"
             "medium = a question, or a minor bug with a workaround.\n"
             "low = cosmetic issue, feature request, or general inquiry."},
            {"role": "user", "content": "Ticket: Checkout page is down for everyone\nPriority:"}, {"role": "assistant", "content": "urgent"},
            {"role": "user", "content": "Ticket: Please add more emoji to the editor\nPriority:"}, {"role": "assistant", "content": "low"},
            {"role": "user", "content": f"Ticket: {text}\nPriority:"}]


def next_token_distribution(tok, model):
    ids = llm.chat_ids(tok, classify_messages("Our data seems to be visible to another customer, please investigate immediately"))
    with torch.no_grad():
        logits = model(ids).logits[0, -1].float()
    out = {}
    for T in (0.2, 0.7, 1.0, 1.5):
        p = torch.softmax(logits / T, -1)
        top = torch.topk(p, 8)
        ent = float(-(p * torch.log(p + 1e-12)).sum())
        sorted_p = torch.sort(p, descending=True).values
        kept_p90 = int((torch.cumsum(sorted_p, 0) < 0.9).sum()) + 1
        out[str(T)] = {"top": [(tok.decode([int(i)]).strip() or repr(tok.decode([int(i)])), round(float(v), 4)) for v, i in zip(top.values, top.indices)],
                       "entropy_bits": round(ent / math.log(2), 2), "top_p_0.9_keeps": kept_p90, "top_k_40_tail_mass": round(float(1 - sorted_p[:40].sum()), 6)}
    return out


def adaptivity(tok, model):
    """How many tokens top-p=0.9 keeps at a confident step vs an open-ended step; top-k=40 always keeps 40."""
    confident = llm.chat_ids(tok, [{"role": "user", "content": "What is the capital of France? Answer with one word."}])
    open_ended = llm.chat_ids(tok, [{"role": "user", "content": "Suggest a friendly opening word for an email to a customer."}])
    out = {}
    for name, ids in (("confident", confident), ("open-ended", open_ended)):
        with torch.no_grad():
            p = torch.softmax(model(ids).logits[0, -1].float(), -1)
        s = torch.sort(p, descending=True).values
        out[name] = {"top1": round(float(s[0]), 3), "top_p_0.9_keeps": int((torch.cumsum(s, 0) < 0.9).sum()) + 1, "top_k_40_keeps": 40}
    return out


def run_classification(tok, model, T, samples):
    correct = invalid = n = 0
    for text, label in TICKETS:
        for s in range(samples):
            kw = {"do_sample": False} if T == 0 else {"do_sample": True, "temperature": T, "top_p": 1.0, "top_k": 0}
            out = llm.generate(tok, model, classify_messages(text), max_new_tokens=4, **kw).strip().lower()
            word = re.sub(r"[^a-z]", "", out.split()[0]) if out.split() else ""
            n += 1
            invalid += word not in LABELS
            correct += word == label
    return {"temperature": T, "answers": n, "accuracy_pct": round(100 * correct / n, 1), "invalid_pct": round(100 * invalid / n, 1)}


def run_extraction(tok, model, T, samples=3):
    texts = [f"Hi, my order ORD-{n} arrived damaged, the box was open. Can you send a replacement? Thanks, Dana" for n in
             (48213, 90071, 11532, 66310, 25894, 73019, 40457, 88124, 19376, 52608)]
    exact = n = 0
    for t in texts:
        want = re.search(r"ORD-\d+", t).group(0)
        for _ in range(samples):
            kw = {"do_sample": False} if T == 0 else {"do_sample": True, "temperature": T, "top_p": 1.0, "top_k": 0}
            out = llm.generate(tok, model, [{"role": "system", "content": "Extract the order number. Reply with the order number only."},
                                             {"role": "user", "content": t}], max_new_tokens=10, **kw).strip()
            n += 1
            exact += out.upper().removeprefix("ORD-") == want.removeprefix("ORD-")   # the number, with or without its prefix
    return {"temperature": T, "exact_pct": round(100 * exact / n, 1)}


def run_brainstorm(tok, model, T, k=8):
    outs = []
    for _ in range(k):
        kw = {"do_sample": False} if T == 0 else {"do_sample": True, "temperature": T, "top_p": 0.95}
        outs.append(llm.generate(tok, model, [{"role": "user", "content": "Write one short, friendly subject line for an email telling a customer their ticket is solved."}],
                                 max_new_tokens=20, **kw).strip().strip('"'))
    return {"temperature": T, "distinct": len(set(outs)), "of": k, "examples": outs[:4]}


if __name__ == "__main__":
    tok, model = llm.load()
    ND = next_token_distribution(tok, model)
    for T, v in ND.items():
        print(T, v, flush=True)
    AD = adaptivity(tok, model)
    print(AD, flush=True)
    CL = [run_classification(tok, model, 0, 1)] + [run_classification(tok, model, T, 3) for T in (0.7, 1.0, 1.5)]
    for v in CL:
        print(v, flush=True)
    EX = [run_extraction(tok, model, T) for T in (0, 1.0, 1.5)]
    print(EX, flush=True)
    BR = [run_brainstorm(tok, model, T) for T in (0, 0.7, 1.2)]
    for v in BR:
        print(v, flush=True)
    data.update({"next_token": ND, "adaptivity": AD, "classification": CL, "extraction": EX, "brainstorm": BR})
    report("Temperature reshapes the next-token distribution", "low T concentrates probability on the top token; high T spreads it",
           f"P(top token): T=0.2 {ND['0.2']['top'][0][1]} · T=1.0 {ND['1.0']['top'][0][1]} · T=1.5 {ND['1.5']['top'][0][1]}; entropy {ND['0.2']['entropy_bits']} → {ND['1.5']['entropy_bits']} bits",
           ND["0.2"]["top"][0][1] > ND["1.0"]["top"][0][1] > ND["1.5"]["top"][0][1] and ND["1.5"]["entropy_bits"] > ND["0.2"]["entropy_bits"] + 0.5)
    report("Top-p adapts; top-k doesn't", "a confident step keeps one or two tokens under top-p, an open-ended step keeps many; top-k always keeps k",
           f"top-p 0.9 keeps {AD['confident']['top_p_0.9_keeps']} (confident) vs {AD['open-ended']['top_p_0.9_keeps']} (open-ended); top-k keeps 40 in both",
           AD["confident"]["top_p_0.9_keeps"] <= 2 and AD["open-ended"]["top_p_0.9_keeps"] >= 5 and AD["confident"]["top_k_40_keeps"] == AD["open-ended"]["top_k_40_keeps"] == 40)
    g, hi = CL[0], CL[-1]
    report("Classification gets worse with temperature", "higher temperature means more wrong and more invalid answers, not better reasoning",
           " · ".join(f"T={c['temperature']}: {c['accuracy_pct']}% right, {c['invalid_pct']}% invalid" for c in CL),
           hi["accuracy_pct"] <= g["accuracy_pct"] and hi["invalid_pct"] > g["invalid_pct"])
    report("Extraction gets worse with temperature", "copying a value out of the text has one right answer",
           " · ".join(f"T={e['temperature']}: {e['exact_pct']}% exact" for e in EX), EX[-1]["exact_pct"] < EX[0]["exact_pct"])
    report("Diversity comes from temperature", "greedy repeats itself; sampling gives variety for open-ended tasks",
           " · ".join(f"T={b['temperature']}: {b['distinct']}/{b['of']} distinct" for b in BR), BR[0]["distinct"] == 1 and BR[-1]["distinct"] > 4)
    with open(__file__.replace("sampling.py", "sampling_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
