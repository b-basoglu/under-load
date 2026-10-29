# Part 49 lab: context (prefix) caching

`prefix.py` — Qwen2.5-0.5B-Instruct, fp32, CPU. Six support questions sharing a ~1,935-token system prompt + policies.
A prefix cache (implemented here) keyed on a hash of model name + exact token IDs stores the prefix KV cache and reuses
it. Measures TTFT with and without the cache, output equality, a timestamp at the top vs in the user message, tenant
prefixes and cache keys, and the memory of a cached prefix.

Results in `prefix_results.json`. Last run (after the 2026-10-25 audit): 5/5 passed. ~1 min on an M2 Pro (CPU).

```bash
HF_HOME=./hf llmenv/bin/python prefix.py     # venv as in ../../43-sampling/lab/README.md
```
