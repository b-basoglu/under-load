# Part 52 lab: quantization

`quant.py` — Qwen2.5-0.5B-Instruct on CPU. Formats: fp32, bf16, int8 dynamic (weights + activations, PyTorch qnnpack
kernels), and simulated weight-only int8 / int4 / int3 (quantize → dequantize) with per-tensor vs group-128 scales.
Measures weight memory, perplexity on support-style text, 8 extraction answers vs fp32, decode step time (real kernels
only), and activation vs weight outliers in one MLP layer.

Results in `quant_results.json`. Last run (after the 2026-10-25 audit): 6/6 passed. ~5 min on an M2 Pro.

```bash
HF_HOME=./hf llmenv/bin/python quant.py     # venv as in ../../43-sampling/lab/README.md
```
