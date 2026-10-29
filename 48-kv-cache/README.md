# Part 48 lab: KV cache

`kvcache.py` — Qwen2.5-0.5B-Instruct, fp32, CPU (helper: `../../shared/llm.py`). Measures: 100 decode steps with and
without the KV cache; time to first token and per-token decode time for prompts of 100–8,192 tokens (median of 15
steps for the 256/2,048/8,192 runs); cache size measured from the cache tensors vs the formula; per-token cache size and
concurrent-user capacity computed for larger model shapes (GQA vs full MHA).

Results in `kvcache_results.json`. Last run: 5/5 passed. ~3 min on an M2 Pro (CPU).

```bash
HF_HOME=./hf llmenv/bin/python kvcache.py     # venv as in ../../43-sampling/lab/README.md
```
