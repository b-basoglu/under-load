# Part 46 lab: RAG — chunking and retrieval

`rag.py` + `corpus.py` — a 20-article help center (fictional facts, near-identical error-code lists, an outdated refund
article) and 47 labelled questions (paraphrases, exact terms, answers spanning sentences). Chunking: sentences, 40-word
windows with and without 15-word overlap, whole articles. Retrieval: BM25 (implemented here), dense (all-MiniLM-L6-v2),
hybrid (reciprocal rank fusion), dense + cross-encoder reranking (ms-marco-MiniLM-L-6-v2). Top-K 1/3/8. A deprecated-
article metadata filter. Answers by Qwen2.5-0.5B-Instruct (greedy). Metrics: recall@K, MRR, context precision, tokens,
answer correctness, latency.

Results in `rag_results.json`. Last run (after the 2026-10-25 audit): 6/6 passed. ~6 min on an M2 Pro (CPU).

```bash
llmenv/bin/pip install sentence-transformers
HF_HOME=./hf llmenv/bin/python rag.py     # venv as in ../../43-sampling/lab/README.md
```
