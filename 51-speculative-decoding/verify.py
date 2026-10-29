"""How much does it cost the target to score n new tokens in one pass? (Part 51)
Speculative decoding assumes verifying k+1 tokens costs about one decode step. Measured on CPU (fp32) and MPS (fp16)."""
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


torch.set_num_threads(8)
out = {}
for dev, dt in (("cpu", torch.float32), ("mps", torch.float16)):
    tok, m = llm.load("Qwen/Qwen2.5-1.5B-Instruct", device=dev, dtype=dt)
    ids = llm.chat_ids(tok, [{"role": "user", "content": "Explain how to configure SSO with Okta for a helpdesk, step by step."}], device=dev)
    row = {}
    with torch.no_grad():
        for n in (1, 2, 3, 5, 7, 11):
            ts = []
            for _ in range(15):
                o = m(ids, use_cache=True)
                new = torch.full((1, n), 1000, dtype=ids.dtype, device=dev)
                if dev == "mps":
                    torch.mps.synchronize()
                t0 = time.perf_counter()
                m(new, past_key_values=o.past_key_values, use_cache=True)
                if dev == "mps":
                    torch.mps.synchronize()
                ts.append(time.perf_counter() - t0)
            row[n] = round(1000 * sorted(ts)[7], 1)               # median of 15
    out[dev] = row
    print(dev, row, flush=True)
    del m
# the draft's own decode step on the GPU (the CPU draft cost is measured in speculative.py)
tok, d = llm.load("Qwen/Qwen2.5-0.5B-Instruct", device="mps", dtype=torch.float16)
ids = llm.chat_ids(tok, [{"role": "user", "content": "Explain how to configure SSO with Okta for a helpdesk, step by step."}], device="mps")
ts = []
with torch.no_grad():
    for _ in range(9):
        o = d(ids, use_cache=True)
        new = torch.full((1, 1), 1000, dtype=ids.dtype, device="mps")
        torch.mps.synchronize()
        t0 = time.perf_counter()
        d(new, past_key_values=o.past_key_values, use_cache=True)
        torch.mps.synchronize()
        ts.append(time.perf_counter() - t0)
out["mps_draft_step_ms"] = round(1000 * sorted(ts)[4], 1)
print("mps draft step", out["mps_draft_step_ms"], flush=True)
json.dump(out, open(__file__.replace("verify.py", "verify_results.json"), "w"))
