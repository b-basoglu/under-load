# Part 53 lab: model routing

`routing.py` — Qwen2.5-0.5B-Instruct (small) and Qwen2.5-3B-Instruct (large), both fp16 on the Apple GPU (MPS), greedy.
46 support requests with known answers: 15 order-number extractions, 16 help-article choices (1 of 8), 15 seat-proration
calculations. Every request is sent to both models once; policies are then scored from those answers and their measured
compute time: all small, all large, two type rules, confidence cascades at 0.6 / 0.8 / 0.9, and a type rule with the small
model failing for a third of requests (circuit breaker → large). Also reports the small model's confidence calibration
and cost by request kind.

Results in `routing_results.json`. Last run (after the 2026-10-25 audit): 5/5 passed. ~3 min on an M2 Pro.

```bash
HF_HOME=./hf llmenv/bin/python routing.py     # venv as in ../../43-sampling/lab/README.md
```
