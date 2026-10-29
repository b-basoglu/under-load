# Part 44 lab: hallucinations and grounding

`grounding.py` — Qwen2.5-0.5B-Instruct, greedy, local (helper: `../../shared/llm.py`). 12 questions about the product's
own (fictional) policies. Modes: closed book; closed book with "say you don't know"; grounded in the right policy text;
grounded in the wrong policy text. Also: a prompt injection inside a retrieved page (plain vs delimited), a programmatic
claim check (every number in the answer must appear in the source), and the same model judging its own answers.

Results in `grounding_results.json`. Last run (after the 2026-10-25 audit): 7/7 passed. ~3 min on an M2 Pro (CPU).

```bash
HF_HOME=./hf llmenv/bin/python grounding.py     # venv as in ../../43-sampling/lab/README.md
```
