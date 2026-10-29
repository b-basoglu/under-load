"""Structured outputs and constrained decoding, measured for Part 45 with Qwen2.5-0.5B-Instruct (greedy).

Task: turn an inbound support email into arguments for create_ticket(title, severity, customer_id).
Constrained decoding is implemented here directly: at every step, only tokens that keep the output a valid prefix of
the schema's language (a regular expression derived from the JSON schema) are allowed.
"""
import json
import re
import sys
import time
from pathlib import Path

import regex
import torch

_here = Path(__file__).resolve().parent
for _cand in (_here, _here.parent / "shared", _here.parents[1] / "shared", _here.parents[2] / "shared"):
    if (_cand / "llm.py").exists():
        sys.path.insert(0, str(_cand))
        break
import llm  # noqa: E402

results, data = [], {}


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


EMAILS = [
    ("Hi, I'm with Northwind (customer CUS-10442). None of our agents can log in since this morning.", "urgent", "CUS-10442"),
    ("Customer ID CUS-22871 here. The CSV export skips rows that contain accents.", "medium", "CUS-22871"),
    ("Hello! Could you add a dark mode? We're CUS-30915.", "low", "CUS-30915"),
    ("Account CUS-41207: we were billed twice for October, please fix asap.", "high", "CUS-41207"),
    ("Our webhooks to the CRM have failed for 3 hours. Account: CUS-55310.", "urgent", "CUS-55310"),
    ("CUS-60028 — small typo on the pricing page, 'anual'.", "low", "CUS-60028"),
    ("We (CUS-71654) see times in the wrong time zone for German agents.", "medium", "CUS-71654"),
    ("Payment page is down for all our customers. CUS-82219.", "urgent", "CUS-82219"),
    ("Attachments over 10 MB fail to upload. Customer CUS-93047.", "high", "CUS-93047"),
    ("CUS-14590 asking: how do I rename a custom field?", "medium", "CUS-14590"),
    ("Two of our agents can't reset passwords, the email never arrives. CUS-25613", "high", "CUS-25613"),
    ("Please update our billing address. Customer number CUS-36782.", "low", "CUS-36782"),
    ("Data export includes other companies' emails!! CUS-47821", "urgent", "CUS-47821"),
    ("Macros stopped applying tags since today. We are CUS-58039.", "high", "CUS-58039"),
    ("The report total doesn't match the ticket count. CUS-69154", "medium", "CUS-69154"),
    ("Logo is blurry on retina screens (CUS-70263).", "low", "CUS-70263"),
    ("SSO broke for new employees after your update — CUS-81372", "high", "CUS-81372"),
    ("Can we get your security whitepaper? CUS-92481", "low", "CUS-92481"),
    ("2FA codes are rejected for every user in our account CUS-13590.", "urgent", "CUS-13590"),
    ("Search takes about 5 seconds but works. CUS-24609", "medium", "CUS-24609"),
    ("Our whole team cannot see any tickets after your deploy. CUS-35718", "urgent", "CUS-35718"),
    ("The chatbot replies in English to Spanish customers. CUS-46827", "medium", "CUS-46827"),
    ("Ticket assignment sends everything to one agent. CUS-57936", "high", "CUS-57936"),
    ("Please add an emoji picker. CUS-68045", "low", "CUS-68045"),
    ("Mobile app crashes when attaching photos on Android 14. CUS-79154", "high", "CUS-79154"),
    ("Hi, nobody can log in to our helpdesk right now, please help!", "urgent", None),
    ("How do I export tickets to CSV?", "medium", None),
    ("Could you translate the widget into Portuguese?", "low", None),
    ("We were charged twice this month.", "high", None),
    ("Our customers can't pay invoices, the payment page errors.", "urgent", None),
]
SEVERITIES = ("low", "medium", "high", "urgent")
SYSTEM = ("Turn the support email into arguments for create_ticket. Return only JSON with exactly these keys: "
          '"title" (short string), "severity" (one of "low", "medium", "high", "urgent"), "customer_id" (string like "CUS-12345").')
STR = r'"[^"\\\n]{3,80}"'
WS = r'[ \n]{0,2}'                 # JSON allows whitespace; forcing exactly one space fights the tokenizer
SCHEMA_RE = (r'\{' + WS + r'"title":' + WS + STR + r',' + WS + r'"severity":' + WS + r'"(low|medium|high|urgent)",' + WS
             + r'"customer_id":' + WS + r'"CUS-[0-9]{5}"' + WS + r'\}')
SYSTEM_NULLABLE = SYSTEM.replace('"customer_id" (string like "CUS-12345").', '"customer_id" (string like "CUS-12345", or null if the email contains no customer id).')
SCHEMA_RE_NULLABLE = SCHEMA_RE.replace(r'"CUS-[0-9]{5}"', r'("CUS-[0-9]{5}"|null)')


def validate(text, strict=False):
    """Returns (parsed_ok, schema_ok, obj). strict: the whole output must be JSON (what json.loads in production sees);
    lenient: the first {...} block is cut out of surrounding text such as a ```json fence."""
    m = None if strict else re.search(r"\{.*\}", text, re.S)
    try:
        obj = json.loads(m.group(0) if m else text.strip())
    except Exception:
        return False, False, None
    ok = (isinstance(obj, dict) and set(obj) == {"title", "severity", "customer_id"} and isinstance(obj["title"], str) and obj["severity"] in SEVERITIES
          and (obj["customer_id"] is None or (isinstance(obj["customer_id"], str) and re.fullmatch(r"CUS-\d{5}", obj["customer_id"]))))
    return True, bool(ok), obj


def free(tok, model, email):
    t0 = time.perf_counter()
    out = llm.generate(tok, model, [{"role": "system", "content": SYSTEM}, {"role": "user", "content": email}], max_new_tokens=80, do_sample=False)
    return out, time.perf_counter() - t0


def constrained(tok, model, email, pattern, max_steps=80, system=None):
    """Greedy decoding restricted to tokens that keep the text a valid prefix of `pattern`."""
    t0 = time.perf_counter()
    ids = llm.chat_ids(tok, [{"role": "system", "content": system or SYSTEM}, {"role": "user", "content": email}])
    rx = regex.compile(pattern)
    text, past, inp, checked = "", None, ids, 0
    for _ in range(max_steps):
        with torch.no_grad():
            o = model(inp, past_key_values=past, use_cache=True)
        past = o.past_key_values
        order = torch.argsort(o.logits[0, -1], descending=True)
        chosen = None
        for tid in order.tolist():
            piece = tok.decode([tid])
            if not piece:
                continue
            checked += 1
            if rx.fullmatch(text + piece, partial=True):
                chosen = tid
                break
        text += tok.decode([chosen])
        inp = torch.tensor([[chosen]])
        if rx.fullmatch(text):
            break
    return text, time.perf_counter() - t0, checked


def semantic(obj, sev, cid):
    """Valid JSON can still be wrong."""
    return {"wrong_id": obj["customer_id"] != cid, "wrong_severity": obj["severity"] != sev, "invented_id": cid is None and obj["customer_id"] is not None}


def run(tok, model, mode):
    rows, lat = [], []
    calls = 0
    for email, sev, cid in EMAILS:
        if mode == "prompt":
            out, dt = free(tok, model, email)
            calls += 1
        elif mode == "prompt+retry":
            dt = 0
            for attempt in range(3):
                out, d = free(tok, model, email + ("" if attempt == 0 else "\n\nReturn ONLY the JSON object, nothing else."))
                dt += d
                calls += 1
                if validate(out, strict=True)[1]:          # retry on what production's json.loads would reject
                    break
        elif mode == "constrained":
            out, dt, _ = constrained(tok, model, email, SCHEMA_RE)
            calls += 1
        else:
            out, dt, _ = constrained(tok, model, email, SCHEMA_RE_NULLABLE, system=SYSTEM_NULLABLE)
            calls += 1
        lat.append(dt)
        p, s, obj = validate(out)
        row = {"out": out[:160], "parsed": p, "schema_ok": s, "strict_schema_ok": validate(out, strict=True)[1], "has_id": cid is not None}
        if s:
            row.update(semantic(obj, sev, cid))
            row["null_id"] = obj["customer_id"] is None
        rows.append(row)
    ok = [r for r in rows if r["schema_ok"]]
    return {"mode": mode, "n": len(rows), "parse_pct": round(100 * sum(r["parsed"] for r in rows) / len(rows), 1),
            "schema_valid_pct": round(100 * len(ok) / len(rows), 1), "strict_schema_valid_pct": round(100 * sum(r["strict_schema_ok"] for r in rows) / len(rows), 1),
            "null_on_id_emails": sum(r.get("null_id", False) for r in ok if r["has_id"]),
            "wrong_digits_on_id_emails": sum(r.get("wrong_id", False) and not r.get("null_id", False) for r in ok if r["has_id"]),
            "id_emails": sum(1 for e in EMAILS if e[2]), "rows": rows, "calls": calls, "mean_latency_s": round(sum(lat) / len(lat), 2),
            "wrong_id": sum(r.get("wrong_id", False) for r in ok), "wrong_severity": sum(r.get("wrong_severity", False) for r in ok),
            "invented_id": sum(r.get("invented_id", False) for r in ok), "no_id_emails": sum(1 for e in EMAILS if e[2] is None),
            "example_fail": next((r["out"] for r in rows if not r["schema_ok"]), None), "example_ok": next((r["out"] for r in rows if r["schema_ok"]), None)}


if __name__ == "__main__":
    tok, model = llm.load()
    R = {m: run(tok, model, m) for m in ("prompt", "prompt+retry", "constrained", "constrained-nullable")}
    for v in R.values():
        print({k: x for k, x in v.items() if k != "rows"}, flush=True)
    data.update(R)
    p, r, c, n = R["prompt"], R["prompt+retry"], R["constrained"], R["constrained-nullable"]
    report("'Return only JSON' in the prompt", "most outputs parse; not all match the schema",
           f"strict json.loads: schema-valid {p['strict_schema_valid_pct']}% · after cutting the JSON out of fences: parsed {p['parse_pct']}%, schema-valid {p['schema_valid_pct']}%",
           p["strict_schema_valid_pct"] < 50 and p["parse_pct"] >= 90)
    report("Prompt + validate + retry (up to 3 calls)", "more valid outputs, at the cost of extra calls",
           f"strict schema-valid {r['strict_schema_valid_pct']}% (was {p['strict_schema_valid_pct']}%) using {r['calls']} calls for {r['n']} emails",
           r["strict_schema_valid_pct"] > p["strict_schema_valid_pct"] and r["calls"] > r["n"])
    report("Constrained decoding", "every output matches the schema, in one call",
           f"schema-valid {c['schema_valid_pct']}%; {c['calls']} calls; mean {c['mean_latency_s']} s vs {p['mean_latency_s']} s unconstrained",
           c["schema_valid_pct"] == 100.0 and c["strict_schema_valid_pct"] == 100.0 and c["calls"] == c["n"])
    report("Valid is not correct", "schema-valid outputs still carry wrong values",
           f"among constrained outputs: wrong severity {c['wrong_severity']}, wrong customer id {c['wrong_id']}",
           c["wrong_severity"] >= 10 and c["wrong_id"] >= 1)
    report("A rigid schema forces an answer that doesn't exist", "emails without a customer id get an invented one; a nullable field allows 'none'",
           f"invented ids for the {c['no_id_emails']} emails with no id: rigid {c['invented_id']}, nullable {n['invented_id']}",
           c["invented_id"] == c["no_id_emails"] and n["invented_id"] < c["invented_id"])
    report("A nullable field has a cost", "the escape hatch is also taken when the answer exists — measured on emails that do have an id",
           f"of {n['id_emails']} emails with an id: rigid wrong {c['wrong_id'] - c['invented_id']} · nullable null {n['null_on_id_emails']}, wrong digits {n['wrong_digits_on_id_emails']}",
           n["null_on_id_emails"] >= 1 and n["null_on_id_emails"] + n["wrong_digits_on_id_emails"] > c["wrong_id"] - c["invented_id"])
    with open(__file__.replace("structured.py", "structured_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
