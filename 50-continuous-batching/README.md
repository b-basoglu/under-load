# Part 50 lab: continuous batching

`batching.py` — measures one batched decode step of Qwen2.5-0.5B-Instruct (CPU) for batch sizes 1–32 and the prefill cost
per token, then drives a scheduler simulation with those measured costs: 200 requests at 1.2/s, 80% short replies,
20% long drafts. Compares one-at-a-time, static batches of 16, and continuous (per-step) batching with up to 16 slots.

Results in `batching_results.json`. Last run: 4/4 passed. ~1 min on an M2 Pro (CPU).

```bash
HF_HOME=./hf llmenv/bin/python batching.py     # venv as in ../../43-sampling/lab/README.md
```
