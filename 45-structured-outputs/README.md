# Part 45 lab: structured outputs and constrained decoding

`structured.py` — Qwen2.5-0.5B-Instruct, greedy, local (helper: `../../shared/llm.py`). 30 support emails → arguments
for create_ticket(title, severity, customer_id); 5 emails contain no customer id. Modes: prompt only; prompt + validate +
up to 2 retries; constrained decoding (implemented here: at each step, only tokens that keep the output a valid prefix
of a regex derived from the schema, via `regex` partial matching); constrained with a nullable customer_id.
Measures parse and schema-valid rates, calls, latency, wrong values and invented ids.

Results in `structured_results.json`. Last run (after the 2026-10-25 audit): 6/6 passed. ~5 min on an M2 Pro (CPU).

```bash
HF_HOME=./hf llmenv/bin/python structured.py     # venv as in ../../43-sampling/lab/README.md
```
