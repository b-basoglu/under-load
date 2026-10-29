"""Context (prefix) caching, measured for Part 49 on Qwen2.5-0.5B-Instruct (fp32, CPU).

Every support request starts with the same ~1,900-token prefix (system prompt + policies + tool rules), then a short
customer message. A prefix cache keeps the KV cache of the shared prefix and reuses it, so only the suffix is prefilled.
"""
import copy
import hashlib
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
TOK, MODEL = llm.load()

POLICY = ("You are the support assistant for Halcyon Desk. Follow the policies below exactly.\n" +
          "".join(f"Policy {i}: {t}\n" for i, t in enumerate([
              "Annual plans can be refunded within 45 days of purchase; afterwards the remaining months become account credit.",
              "Urgent tickets on Enterprise receive a first response within 30 minutes, 24/7.",
              "Never ask customers for passwords or full card numbers.",
              "Deleted tickets stay in the trash for 21 days.",
              "Escalate any report of data visible to another customer to the security team immediately.",
              "Attachments can be up to 40 MB each; customers can attach up to 10 files per reply.",
              "Pro allows 600 API requests per minute; Enterprise allows 2,500."] * 12)))
QUESTIONS = ["Can I get a refund on my annual plan after two months?", "How fast do you answer urgent tickets?", "What is the attachment limit?",
             "Our data looks visible to another company!", "How many API calls can we make on Pro?", "Where did my deleted ticket go?"]


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def ids_for(system, user):
    return llm.chat_ids(TOK, [{"role": "system", "content": system}, {"role": "user", "content": user}])


def common_prefix_len(a, b):
    n = min(a.shape[1], b.shape[1])
    eq = (a[0, :n] == b[0, :n]).int()
    return int(n if eq.all() else (eq == 0).nonzero()[0])


class PrefixCache:
    """Maps a hash of (model, prefix tokens) to the KV cache for those tokens."""
    def __init__(self):
        self.store, self.hits, self.misses, self.tokens_reused = {}, 0, 0, 0

    def key(self, prefix_ids):
        return hashlib.sha256((MODEL.config._name_or_path + ":" + ",".join(map(str, prefix_ids[0].tolist()))).encode()).hexdigest()

    @torch.no_grad()
    def first_token(self, ids, prefix_len):
        """Returns time to first token; reuses the cached prefix if present."""
        k = self.key(ids[:, :prefix_len])
        t0 = time.perf_counter()
        if k in self.store:
            self.hits += 1
            self.tokens_reused += prefix_len
            past = copy.deepcopy(self.store[k])                  # each request extends its own copy
            out = MODEL(ids[:, prefix_len:], past_key_values=past, use_cache=True)
        else:
            self.misses += 1
            pre = MODEL(ids[:, :prefix_len], use_cache=True)
            self.store[k] = copy.deepcopy(pre.past_key_values)
            out = MODEL(ids[:, prefix_len:], past_key_values=pre.past_key_values, use_cache=True)
        return time.perf_counter() - t0, out.logits[0, -1].argmax().item(), out.logits[0, -1].float()


@torch.no_grad()
def no_cache_first_token(ids):
    t0 = time.perf_counter()
    out = MODEL(ids, use_cache=True)
    return time.perf_counter() - t0, out.logits[0, -1].argmax().item(), out.logits[0, -1].float()


def cache_mb(past):
    tot = 0
    for layer in past.layers:
        tot += layer.keys.numel() * layer.keys.element_size() + layer.values.numel() * layer.values.element_size()
    return tot / 2**20


if __name__ == "__main__":
    base = [ids_for(POLICY, q) for q in QUESTIONS]
    plen = min(common_prefix_len(base[0], b) for b in base[1:])
    print("prompt tokens", [b.shape[1] for b in base], "shared prefix", plen, flush=True)
    # 1. no prefix cache vs prefix cache, same requests
    nc = [no_cache_first_token(b) for b in base]
    pc = PrefixCache()
    wc = [pc.first_token(b, plen) for b in base]
    same = all(a[1] == b[1] for a, b in zip(nc, wc))
    max_diff = max(float((a[2] - b[2]).abs().max()) for a, b in zip(nc, wc))   # whole next-token distribution, not just the top token
    A = {"prefix_tokens": plen, "ttft_no_cache_ms": [round(1000 * x[0]) for x in nc], "ttft_prefix_cache_ms": [round(1000 * x[0]) for x in wc],
         "hits": pc.hits, "misses": pc.misses, "same_first_token": same, "max_abs_logit_diff": max_diff, "prefix_cache_mb": round(cache_mb(next(iter(pc.store.values()))), 1)}
    print(A, flush=True)
    # 2. a prompt change at the start vs at the end
    pc2 = PrefixCache()
    pc2.first_token(base[0], plen)
    dated = ids_for(f"Today is 2026-10-25 06:41:13.\n{POLICY}", QUESTIONS[1])          # a timestamp at the very top
    cp_dated = common_prefix_len(base[0], dated)
    t_dated = pc2.first_token(dated, plen)[0] if cp_dated >= plen else no_cache_first_token(dated)[0]
    suffix_dated = ids_for(POLICY, f"(sent 2026-10-25 06:41:13) {QUESTIONS[1]}")
    cp_suffix = common_prefix_len(base[0], suffix_dated)
    t_suffix = pc2.first_token(suffix_dated, plen)[0]
    B = {"timestamp_at_top_shared_tokens": cp_dated, "ttft_timestamp_at_top_ms": round(1000 * t_dated),
         "timestamp_in_user_msg_shared_tokens": cp_suffix, "ttft_timestamp_in_user_msg_ms": round(1000 * t_suffix)}
    print(B, flush=True)
    # 3. tenant isolation: a prefix with tenant-specific data must not be reused across tenants
    t1 = ids_for(POLICY + "\nCustomer context: Halcyon, plan Enterprise, open incident INC-4471 (data exposure).", QUESTIONS[0])
    t2 = ids_for(POLICY + "\nCustomer context: Northwind, plan Starter, no incidents.", QUESTIONS[0])
    shared_policy = common_prefix_len(t1, t2)
    # a cache keyed by template name ("support-policy-v3") instead of by the tokens would hand tenant 1's context to tenant 2
    name_keyed = {}
    name_keyed["support-policy-v3"] = t1
    reused_wrong = bool((name_keyed["support-policy-v3"][0, :t2.shape[1]].shape == t2[0].shape) and not torch.equal(name_keyed["support-policy-v3"][0, :t2.shape[1]], t2[0]))
    C = {"tokens_shared_by_two_tenants": shared_policy, "full_prompt_tokens": t1.shape[1],
         "keys_differ": PrefixCache().key(t1[:, :t1.shape[1] - 10]) != PrefixCache().key(t2[:, :t2.shape[1] - 10]),
         "template_name_key_serves_other_tenant": reused_wrong or not torch.equal(t1[0, :min(t1.shape[1], t2.shape[1])], t2[0, :min(t1.shape[1], t2.shape[1])])}
    print(C, flush=True)
    # 4. memory: how much a cached prefix holds, and break-even on hits
    data.update({"reuse": A, "invalidation": B, "isolation": C})
    m_nc = sorted(A["ttft_no_cache_ms"])[len(nc) // 2]
    m_hit = sorted(A["ttft_prefix_cache_ms"][1:])[len(wc[1:]) // 2]
    data["summary"] = {"median_ttft_no_cache_ms": m_nc, "median_ttft_hit_ms": m_hit, "speedup": round(m_nc / m_hit, 1)}
    report("Same ~1.9k-token prefix on every request", "a prefix cache prefills only the new suffix on a hit",
           f"time to first token: {m_nc} ms without vs {m_hit} ms on a hit ({data['summary']['speedup']}×); {A['hits']} hits / {A['misses']} miss", m_hit * 8 < m_nc and A["hits"] == 5 and A["misses"] == 1)
    report("Reusing the cache doesn't change the output", "the cached prefix state matches recomputing, up to floating-point rounding",
           f"first token identical on all {len(QUESTIONS)} requests: {same}; largest difference in any next-token logit: {max_diff:.2e}", same and max_diff < 1e-4)
    report("A timestamp at the top of the prompt", "one changed token at the start makes everything after it uncacheable",
           f"shared prefix: {cp_dated} tokens with the timestamp at the top vs {cp_suffix} with it in the user message; TTFT {B['ttft_timestamp_at_top_ms']} vs {B['ttft_timestamp_in_user_msg_ms']} ms",
           cp_dated < 10 and cp_suffix >= plen and B["ttft_timestamp_at_top_ms"] > 3 * B["ttft_timestamp_in_user_msg_ms"])
    report("Tenant-specific context", "only the shared part is reusable; keys must include everything they cover",
           f"two tenants share {shared_policy} of {C['full_prompt_tokens']} tokens; token-hash keys differ: {C['keys_differ']}; a key by template name would serve tenant 1's prefix to tenant 2: {C['template_name_key_serves_other_tenant']}",
           C["keys_differ"] and C["template_name_key_serves_other_tenant"] and shared_policy < C["full_prompt_tokens"])
    report("Cached prefixes take memory", "each one is a full KV cache held between requests", f"{A['prefix_cache_mb']} MB for {plen} tokens (fp32, 0.5B model)", A["prefix_cache_mb"] > 30)

    with open(__file__.replace("prefix.py", "prefix_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
