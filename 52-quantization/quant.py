"""Quantization, measured for Part 52 on Qwen2.5-0.5B-Instruct (CPU).

Formats: fp32 (baseline), bf16, int8 dynamic quantization of all Linear layers (torch.ao), and simulated weight-only
int4 / int3 (quantize → dequantize, so quality is real but speed isn't), per-tensor vs group-wise (group of 128) scales.
Quality: perplexity on support-style text, agreement of greedy outputs with fp32, order-number extraction accuracy.
"""
import copy
import json
import math
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
torch.set_num_threads(8)
torch.backends.quantized.engine = "qnnpack"      # the int8 kernel backend available on Apple silicon
TOK, BASE = llm.load()

TEXT = ("Hi team, since this morning none of our agents can log in with single sign-on. After the Okta redirect they see "
        "an error page with code E1043. Password login still works for the account owner. We already cleared cookies and "
        "uploaded the SAML certificate again. Twenty-three agents are affected and urgent tickets are piling up in the queue. "
        "Could you please look into this as soon as possible and let us know if there is a workaround? Thanks, Dana. "
        "Reply from support: Thanks for the details, Dana. Error E1043 means the account header refers to a closed account, "
        "which usually happens when the identity provider still points to an old tenant. Please check the ACS URL in Okta "
        "and make sure it matches the one shown in Settings, Security, Single sign-on. I have raised the priority of this ticket.") * 2
PROMPTS = [f"Hi, my order ORD-{n} arrived damaged, the box was open. Can you send a replacement? Thanks" for n in (48213, 90071, 11532, 66310, 25894, 73019, 40457, 88124)]


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def fake_quant(model, bits, group):
    """Weight-only symmetric quantization of every Linear weight (except the output head), then dequantize."""
    m = copy.deepcopy(model)
    qmax = 2 ** (bits - 1) - 1
    for name, mod in m.named_modules():
        if isinstance(mod, torch.nn.Linear) and "lm_head" not in name:
            w = mod.weight.data
            if group:
                g = w.reshape(-1, group)
                s = g.abs().amax(1, keepdim=True).clamp(min=1e-8) / qmax
                mod.weight.data = (torch.round(g / s).clamp(-qmax, qmax) * s).reshape(w.shape)
            else:
                s = w.abs().max().clamp(min=1e-8) / qmax
                mod.weight.data = torch.round(w / s).clamp(-qmax, qmax) * s
    return m


@torch.no_grad()
def perplexity(model, dtype=torch.float32):
    ids = TOK(TEXT, return_tensors="pt")["input_ids"][:, :400]
    logits = model(ids).logits.float()
    loss = torch.nn.functional.cross_entropy(logits[0, :-1], ids[0, 1:])
    return round(math.exp(float(loss)), 3)


def norm(s):
    return s.strip().upper().removeprefix("ORD-")          # the order number, with or without its prefix


@torch.no_grad()
def outputs(model):
    outs = []
    for p in PROMPTS:
        ids = llm.chat_ids(TOK, [{"role": "system", "content": "Extract the order number. Reply with the order number only."}, {"role": "user", "content": p}])
        o = model.generate(ids, max_new_tokens=12, do_sample=False, pad_token_id=TOK.eos_token_id)
        outs.append(TOK.decode(o[0][ids.shape[1]:], skip_special_tokens=True).strip())
    return outs


@torch.no_grad()
def decode_speed(model, steps=40):
    ids = TOK(TEXT, return_tensors="pt")["input_ids"][:, :128]
    o = model(ids, use_cache=True)
    past, nxt = o.past_key_values, o.logits[:, -1:].argmax(-1)
    ts = []
    for _ in range(steps):
        t0 = time.perf_counter()
        o = model(nxt, past_key_values=past, use_cache=True)
        ts.append(time.perf_counter() - t0)
        past, nxt = o.past_key_values, o.logits[:, -1:].argmax(-1)
    return round(1000 * sorted(ts)[steps // 2], 1)


def weight_mb(model, bits_linear=None):
    tot = 0
    for name, p in model.named_parameters():
        tot += p.numel() * p.element_size()
    for mod in model.modules():                       # dynamic-quantized Linear layers keep packed int8 weights outside parameters
        if type(mod).__name__ == "Linear" and hasattr(mod, "_packed_params"):
            w, b = mod._packed_params._weight_bias()
            tot += w.numel() + (b.numel() * 4 if b is not None else 0)
    return round(tot / 2**20)


def linear_share(model):
    lin = sum(m.weight.numel() for n, m in model.named_modules() if isinstance(m, torch.nn.Linear) and "lm_head" not in n)
    return lin / sum(p.numel() for p in model.parameters())


if __name__ == "__main__":
    ref_out = outputs(BASE)
    want = [re.search(r"ORD-\d+", p).group(0) for p in PROMPTS]
    rows = {}

    def row(name, model, mb, speed=True):
        ppl = perplexity(model)
        outs = outputs(model)
        r = {"weights_mb": mb, "perplexity": ppl, "same_as_fp32": sum(a == b for a, b in zip(outs, ref_out)), "exact": sum(norm(a) == norm(w) for a, w in zip(outs, want)),
             "decode_ms": decode_speed(model) if speed else None}
        rows[name] = r
        print(name, r, flush=True)

    row("fp32", BASE, weight_mb(BASE))
    bf = copy.deepcopy(BASE).to(torch.bfloat16)
    row("bf16", bf, weight_mb(bf))
    del bf
    q8 = torch.ao.quantization.quantize_dynamic(copy.deepcopy(BASE), {torch.nn.Linear}, dtype=torch.qint8)
    row("int8 dynamic", q8, weight_mb(q8))
    del q8
    # control: the same, but the output head (lm_head, tied to the embeddings) left in fp32 like the simulated formats
    names = {n for n, mod in BASE.named_modules() if isinstance(mod, torch.nn.Linear) and n != "lm_head"}
    q8b = torch.ao.quantization.quantize_dynamic(copy.deepcopy(BASE), {n: torch.ao.quantization.default_dynamic_qconfig for n in names}, dtype=torch.qint8)
    row("int8 dynamic, lm_head fp32", q8b, weight_mb(q8b))
    del q8b
    share = linear_share(BASE)
    fp32_mb = weight_mb(BASE)
    for bits, group in ((8, 0), (8, 128), (4, 128), (4, 0), (3, 128)):
        m = fake_quant(BASE, bits, group)
        est = round(fp32_mb * (1 - share) + fp32_mb * share * (bits / 32) * (1 + (16 / (bits * group) if group else 0)))
        row(f"int{bits} {'group-128' if group else 'per-tensor'} (simulated)", m, est, speed=False)
        del m
    # activation outliers: per-channel max |activation| entering a mid-network MLP down-projection
    acts = {}
    layer = BASE.model.layers[12].mlp.down_proj
    def grab(m, i, o):
        acts.setdefault("x", i[0].detach())          # returns None, so the layer's output is untouched
    h = layer.register_forward_hook(grab)
    with torch.no_grad():
        BASE(TOK(TEXT, return_tensors="pt")["input_ids"][:, :256])
    h.remove()
    a = acts["x"][0].abs()
    per_ch = a.amax(0)
    w = BASE.model.layers[12].mlp.down_proj.weight.abs()
    OUT = {"act_max": round(float(a.max()), 1), "act_median": round(float(a.median()), 3), "act_ratio": round(float(a.max() / a.median())),
           "channels_over_20x_median_pct": round(100 * float((per_ch > 20 * a.median()).float().mean()), 2),
           "weight_max": round(float(w.max()), 3), "weight_median": round(float(w.median()), 4), "weight_ratio": round(float(w.max() / w.median()))}
    print("outliers", OUT, flush=True)
    emb_mb = round(BASE.model.embed_tokens.weight.numel() * 4 / 2**20)
    data.update({"rows": rows, "linear_share": round(share, 3), "params_m": round(sum(p.numel() for p in BASE.parameters()) / 1e6), "outliers": OUT, "embedding_mb_fp32": emb_mb})
    f, b, q, qb = rows["fp32"], rows["bf16"], rows["int8 dynamic"], rows["int8 dynamic, lm_head fp32"]
    w8, g8, g4, t4, g3 = (rows[k] for k in ("int8 per-tensor (simulated)", "int8 group-128 (simulated)", "int4 group-128 (simulated)", "int4 per-tensor (simulated)", "int3 group-128 (simulated)"))
    report("Lower precision shrinks only the tensors you quantize", "bf16 halves everything; int8 of Linear layers leaves the fp32 embeddings untouched",
           f"fp32 {f['weights_mb']} MB → bf16 {b['weights_mb']} MB; int8 Linear layers {q['weights_mb']} MB (embedding table alone: {emb_mb} MB fp32); int4 group-128 ≈{g4['weights_mb']} MB",
           b["weights_mb"] < f["weights_mb"] * 0.55 and q["weights_mb"] < f["weights_mb"] * 0.55 and emb_mb > 400)
    report("16-bit and 8-bit weights keep quality", "bf16 and int8 weight-only stay close to fp32",
           f"perplexity fp32 {f['perplexity']}, bf16 {b['perplexity']}, int8 weights per-tensor {w8['perplexity']}, group-128 {g8['perplexity']}; identical text to fp32: {b['same_as_fp32']}/8, {w8['same_as_fp32']}/8, {g8['same_as_fp32']}/8; right order number: fp32 {f['exact']}/8, bf16 {b['exact']}/8, group-128 {g8['exact']}/8",
           abs(g8["perplexity"] - f["perplexity"]) / f["perplexity"] < 0.01 and abs(b["perplexity"] - f["perplexity"]) / f["perplexity"] < 0.01
           and b["exact"] == f["exact"] and g8["exact"] == f["exact"] and g8["same_as_fp32"] == 8)
    report("Quantizing activations naively breaks it", "activation outliers dominate a per-tensor scale (control: same per-tensor int8 weights without activation quantization)",
           f"int8 dynamic, all Linear: perplexity {q['perplexity']}, {q['same_as_fp32']}/8 · lm_head kept fp32: {qb['perplexity']}, {qb['same_as_fp32']}/8 · "
           f"weights-only per-tensor int8: {w8['perplexity']}, {w8['same_as_fp32']}/8; activations reach {OUT['act_ratio']}× their median vs weights {OUT['weight_ratio']}×",
           qb["perplexity"] > 1.7 * w8["perplexity"] and OUT["act_ratio"] > 2 * OUT["weight_ratio"] and q["same_as_fp32"] == 0)

    report("4-bit needs group-wise scales", "one scale per tensor lets outliers crush everything else",
           f"int4 perplexity: group-128 {g4['perplexity']} vs per-tensor {t4['perplexity']:.3g}; extraction exact {g4['exact']}/8 vs {t4['exact']}/8",
           t4["perplexity"] > 1000 * g4["perplexity"] and g4["exact"] == 8 and t4["exact"] == 0)
    report("Going lower costs quality", "3-bit loses far more than 4-bit", f"perplexity int4 {g4['perplexity']} vs int3 {g3['perplexity']} (fp32 {f['perplexity']})",
           g3["perplexity"] > 10 * g4["perplexity"] and g3["exact"] == 0)
    report("Fewer bits ≠ proportionally faster", "speed depends on kernels and hardware, not bit-width",
           f"decode step: fp32 {f['decode_ms']} ms, bf16 {b['decode_ms']} ms, int8 {q['decode_ms']} ms (CPU, PyTorch kernels)",
           q["decode_ms"] > 0.8 * b["decode_ms"] and b["decode_ms"] < f["decode_ms"])

    with open(__file__.replace("quant.py", "quant_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
