"""KV cache, measured for Part 48 on Qwen2.5-0.5B-Instruct (float32, CPU), with PyTorch + Transformers.

1. Decoding with vs without the KV cache: time per token as the sequence grows.
2. Prefill vs decode: time to first token vs time per later token, for different prompt lengths.
3. KV cache size: measured from the cache tensors, and computed for larger models (GQA vs full multi-head attention).
4. Attention over a long cache still costs: decode step time vs context length.
"""
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
torch.manual_seed(0)
torch.set_num_threads(8)
TOK, MODEL = llm.load()
CFG = MODEL.config


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def prompt_ids(n):
    base = TOK("Ticket history: the customer reported that agents cannot log in after the update. " * 700, return_tensors="pt")["input_ids"]
    return base[:, :n]


@torch.no_grad()
def decode(ids, steps, use_cache):
    """Greedy decode `steps` tokens; returns per-step times (s). Without the cache, every step re-reads the whole sequence."""
    times = []
    if use_cache:
        t0 = time.perf_counter()
        out = MODEL(ids, use_cache=True)
        times.append(time.perf_counter() - t0)
        past, nxt = out.past_key_values, out.logits[:, -1:].argmax(-1)
        for _ in range(steps - 1):
            t0 = time.perf_counter()
            out = MODEL(nxt, past_key_values=past, use_cache=True)
            times.append(time.perf_counter() - t0)
            past, nxt = out.past_key_values, out.logits[:, -1:].argmax(-1)
        return times, past
    seq = ids
    for _ in range(steps):
        t0 = time.perf_counter()
        out = MODEL(seq, use_cache=False)
        times.append(time.perf_counter() - t0)
        seq = torch.cat([seq, out.logits[:, -1:].argmax(-1)], 1)
    return times, None


def cache_bytes(past):
    total = 0
    for layer in past.layers if hasattr(past, "layers") else past:
        k, v = (layer.keys, layer.values) if hasattr(layer, "keys") else layer
        total += k.numel() * k.element_size() + v.numel() * v.element_size()
    return total


def kv_formula(layers, kv_heads, head_dim, tokens, bytes_per=2):
    return 2 * layers * kv_heads * head_dim * tokens * bytes_per


if __name__ == "__main__":
    # 1. with vs without cache
    ids = prompt_ids(200)
    t_nc, _ = decode(ids, 100, False)
    t_c, _ = decode(ids, 100, True)
    W = {"no_cache_ms": [round(1000 * t, 1) for t in t_nc], "cache_ms": [round(1000 * t, 1) for t in t_c],
         "no_cache_total_s": round(sum(t_nc), 2), "cache_total_s": round(sum(t_c), 2)}
    print("100 tokens after a 200-token prompt:", W["no_cache_total_s"], "s vs", W["cache_total_s"], "s", flush=True)
    # 2. prefill vs decode for prompt lengths
    PD = []
    for n in (100, 500, 1000, 2000):
        t, past = decode(prompt_ids(n), 20, True)
        PD.append({"prompt_tokens": n, "ttft_ms": round(1000 * t[0]), "decode_ms": round(1000 * sum(t[1:]) / len(t[1:]), 1), "cache_mb": round(cache_bytes(past) / 2**20, 1)})
        print(PD[-1], flush=True)
    # 3. size: measured vs formula, and larger models
    measured = PD[-1]["cache_mb"]
    hd = CFG.hidden_size // CFG.num_attention_heads
    formula = kv_formula(CFG.num_hidden_layers, CFG.num_key_value_heads, hd, 2000 + 19, 4) / 2**20
    big = {"Qwen2.5-0.5B (24 layers, 2 KV heads × 64)": kv_formula(24, 2, 64, 1, 2), "8B, GQA (32 layers, 8 KV heads × 128)": kv_formula(32, 8, 128, 1, 2),
           "8B, full MHA (32 layers, 32 heads × 128)": kv_formula(32, 32, 128, 1, 2), "70B, GQA (80 layers, 8 KV heads × 128)": kv_formula(80, 8, 128, 1, 2)}
    per_token_kb = {k: round(v / 1024, 1) for k, v in big.items()}
    users = {k: round((80 - 16) * 2**30 / (v * 8192)) for k, v in big.items() if "8B" in k}    # 80 GB GPU, 16 GB weights, 8k-token conversations
    SZ = {"config": {"layers": CFG.num_hidden_layers, "q_heads": CFG.num_attention_heads, "kv_heads": CFG.num_key_value_heads, "head_dim": hd},
          "measured_mb_2019_tokens": measured, "formula_mb": round(formula, 1), "per_token_kb_fp16": per_token_kb, "users_8k_on_80gb": users}
    print(SZ, flush=True)
    # 4. decode cost vs context: median of 15 decode steps after prompts of increasing length
    DC = []
    for n in (256, 2048, 8192):
        t, _ = decode(prompt_ids(n), 16, True)
        DC.append({"context": n, "decode_ms": round(1000 * sorted(t[1:])[len(t[1:]) // 2], 1), "ttft_ms": round(1000 * t[0])})
        print(DC[-1], flush=True)
    data.update({"with_without": W, "prefill_decode": PD, "size": SZ, "decode_vs_context": DC})
    report("Decoding without the KV cache", "each new token re-processes the whole sequence; step time grows",
           f"100 tokens: {W['no_cache_total_s']} s without cache vs {W['cache_total_s']} s with; last step {W['no_cache_ms'][-1]} ms vs {W['cache_ms'][-1]} ms",
           W["no_cache_total_s"] > 3 * W["cache_total_s"])
    report("Prefill vs decode", "time to first token grows with the prompt; each later token is much cheaper",
           " · ".join(f"{p['prompt_tokens']} tok: TTFT {p['ttft_ms']} ms, then {p['decode_ms']} ms/token" for p in PD), PD[-1]["ttft_ms"] > 5 * PD[-1]["decode_ms"])
    report("Cache size matches 2 × layers × KV heads × head dim × tokens × bytes", "it's predictable from the model config",
           f"measured {measured} MB vs formula {SZ['formula_mb']} MB for 2,019 tokens (fp32)", abs(measured - SZ["formula_mb"]) < 1)
    report("GQA shrinks the cache", "fewer KV heads → fewer bytes per token → more concurrent users",
           f"8B per token: full MHA {per_token_kb['8B, full MHA (32 layers, 32 heads × 128)']} KB vs GQA {per_token_kb['8B, GQA (32 layers, 8 KV heads × 128)']} KB; 8k-token users on an 80 GB GPU: {users}",
           per_token_kb["8B, GQA (32 layers, 8 KV heads × 128)"] * 4 == per_token_kb["8B, full MHA (32 layers, 32 heads × 128)"])
    report("Long context isn't free even with the cache", "each decode step still reads the whole cache",
           " · ".join(f"{d['context']} tok: {d['decode_ms']} ms" for d in DC), DC[-1]["decode_ms"] > 1.2 * DC[0]["decode_ms"])
    with open(__file__.replace("kvcache.py", "kvcache_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
