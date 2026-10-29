# Part 43 lab: sampling

`sampling.py` — Qwen2.5-0.5B-Instruct, run locally with PyTorch + Transformers (helper: `../../shared/llm.py`).
Measures: the next-token distribution for a triage prompt at T = 0.2/0.7/1.0/1.5 and what top-p keeps; top-p vs top-k
at a confident vs an open-ended step; priority classification of 40 labelled tickets (greedy vs sampled) — accuracy
and invalid answers; order-number extraction exact-match; distinct subject lines across 8 samples.

Results in `sampling_results.json`. Last run (after the 2026-10-25 audit): 5/5 passed. ~4 min on an M2 Pro (CPU).

```bash
python3.12 -m venv llmenv && llmenv/bin/pip install torch transformers accelerate
HF_HOME=./hf llmenv/bin/python sampling.py      # downloads the model (~1 GB) on first run
```
