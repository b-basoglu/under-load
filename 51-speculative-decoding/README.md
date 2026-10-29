# Part 51 lab: speculative decoding

- `speculative.py` — Qwen2.5-1.5B-Instruct (target) with Qwen2.5-0.5B-Instruct (draft), a 6-layer cut-down draft, and
  prompt lookup decoding, via Hugging Face assisted generation; greedy, 120 tokens, three tasks, median of 3 runs, fp32 CPU.
  Also measures per-step costs, draft/target agreement (acceptance), a draft-length sweep. `--mps` runs the Apple GPU
  comparison (fp16) and writes `mps_results.json` (run it first).
- `verify.py` — cost of scoring 1–11 new tokens in one target pass, CPU and MPS (`verify_results.json`).
- `report.py` — derives the checks from the saved results, including the prediction with measured verification cost.

Last run: 8/8 passed (`report.py`). ~45 min total on an M2 Pro.

```bash
HF_HOME=./hf llmenv/bin/python speculative.py --mps
HF_HOME=./hf llmenv/bin/python speculative.py
HF_HOME=./hf llmenv/bin/python verify.py
python3 report.py
```
