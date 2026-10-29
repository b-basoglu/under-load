"""RAG chunking and retrieval, measured for Part 46.

Corpus: 18 help-center articles (corpus.py), one of them an outdated version of the refund policy. 40 labelled
questions. Retrieval: dense (all-MiniLM-L6-v2 embeddings), BM25 (implemented here), hybrid (reciprocal rank fusion),
and dense + cross-encoder reranking (ms-marco-MiniLM-L-6-v2). Generation: Qwen2.5-0.5B-Instruct, greedy.
"""
import json
import math
import re
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
from sentence_transformers import CrossEncoder, SentenceTransformer

_here = Path(__file__).resolve().parent
for _cand in (_here, _here.parent / "shared", _here.parents[1] / "shared", _here.parents[2] / "shared"):
    if (_cand / "llm.py").exists():
        sys.path.insert(0, str(_cand))
        break
import llm  # noqa: E402
from corpus import ARTICLES, QUESTIONS  # noqa: E402

results, data = [], {}
EMB = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
RER = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
TOK, MODEL = llm.load()


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


def words(s):
    # keep decimals and versions ("1.5", "v2.1"), drop sentence punctuation ("hours." must match "hours")
    return [w.rstrip(".") for w in re.findall(r"[A-Za-z0-9$%.>-]+", s) if w.rstrip(".")]


def chunk(strategy, size=40, overlap=0):
    """Returns list of (chunk_id, article_id, deprecated, text)."""
    out = []
    for aid, title, dep, text in ARTICLES:
        if strategy == "article":
            parts = [text]
        elif strategy == "sentence":
            parts = re.split(r"(?<=[.!?])\s+", text)
        else:                                            # fixed-size word windows
            w = text.split()
            step = size - overlap
            parts = [" ".join(w[i:i + size]) for i in range(0, max(1, len(w) - overlap), step)]
        for k, p in enumerate(parts):
            out.append((f"{aid}#{k}", aid, dep, f"{title}: {p}"))
    return out


class BM25:
    def __init__(self, docs, k1=1.5, b=0.75):
        self.toks = [[t.lower() for t in words(d)] for d in docs]
        self.avg = sum(map(len, self.toks)) / len(self.toks)
        self.df = Counter(t for d in self.toks for t in set(d))
        self.N, self.k1, self.b = len(docs), k1, b

    def scores(self, q):
        qt = [t.lower() for t in words(q)]
        out = []
        for d in self.toks:
            tf = Counter(d)
            s = 0.0
            for t in qt:
                if t in tf:
                    idf = math.log(1 + (self.N - self.df[t] + 0.5) / (self.df[t] + 0.5))
                    s += idf * tf[t] * (self.k1 + 1) / (tf[t] + self.k1 * (1 - self.b + self.b * len(d) / self.avg))
            out.append(s)
        return np.array(out)


class Index:
    def __init__(self, chunks):
        self.chunks = chunks
        self.texts = [c[3] for c in chunks]
        self.vecs = EMB.encode(self.texts, normalize_embeddings=True)
        self.bm25 = BM25(self.texts)

    def search(self, q, method, k, exclude_deprecated=False):
        dense = self.vecs @ EMB.encode([q], normalize_embeddings=True)[0]
        if method == "dense":
            order = list(np.argsort(-dense))
        elif method == "bm25":
            order = list(np.argsort(-self.bm25.scores(q)))
        elif method == "hybrid":                         # reciprocal rank fusion of dense and BM25
            rd = {int(i): r for r, i in enumerate(np.argsort(-dense))}
            rb = {int(i): r for r, i in enumerate(np.argsort(-self.bm25.scores(q)))}
            order = sorted(range(len(self.texts)), key=lambda i: -(1 / (60 + rd[i]) + 1 / (60 + rb[i])))
        elif method == "rerank":                         # dense top-20, then a cross-encoder reads query and chunk together
            cand = list(np.argsort(-dense)[:20])
            sc = RER.predict([(q, self.texts[i]) for i in cand])
            order = [cand[i] for i in np.argsort(-sc)]
        if exclude_deprecated:
            order = [i for i in order if not self.chunks[i][2]]
        return [int(i) for i in order[:k]]


def evaluate(index, method, k, exclude_deprecated=False, qs=QUESTIONS):
    hit, rr, tokens, precise, right_tokens = 0, 0.0, 0, 0, 0
    for q, aid, fact, kind in qs:
        got = index.search(q, method, max(k, 10), exclude_deprecated)
        rank = next((r for r, i in enumerate(got) if fact.lower() in index.texts[i].lower()), None)
        top = got[:k]
        hit += rank is not None and rank < k
        rr += 1 / (rank + 1) if rank is not None else 0
        tokens += sum(len(TOK(index.texts[i])["input_ids"]) for i in top)
        right_tokens += sum(len(TOK(index.texts[i])["input_ids"]) for i in top if index.chunks[i][1] == aid)
        precise += sum(1 for i in top if index.chunks[i][1] == aid)
    n = len(qs)
    return {"recall_at_k": round(100 * hit / n, 1), "mrr": round(rr / n, 3), "tokens_per_query": round(tokens / n), "context_precision": round(100 * precise / (n * k), 1),
            "token_precision": round(100 * right_tokens / max(1, tokens), 1)}   # share of context tokens from the right article


def answer(index, q, method, k, exclude_deprecated=False):
    ctx = "\n\n".join(index.texts[i] for i in index.search(q, method, k, exclude_deprecated))
    return llm.generate(TOK, MODEL, [{"role": "system", "content": "Answer the customer's question in one sentence using only the help-center excerpts. If they don't contain the answer, say you don't know."},
                                     {"role": "user", "content": f"Excerpts:\n{ctx}\n\nQuestion: {q}"}], max_new_tokens=50, do_sample=False)


def answer_accuracy(index, method, k, exclude_deprecated=False):
    ok, lat = 0, []
    for q, aid, fact, kind in QUESTIONS:
        t0 = time.perf_counter()
        a = answer(index, q, method, k, exclude_deprecated)
        lat.append(time.perf_counter() - t0)
        key = re.findall(r"[\d.$%]+|[A-Z][a-z]+", fact) or [fact]
        ok += all(x.lower() in a.lower() for x in key[:2])
    return {"answer_pct": round(100 * ok / len(QUESTIONS), 1), "latency_s": round(sum(lat) / len(lat), 2)}


if __name__ == "__main__":
    IDX = {"sentence": Index(chunk("sentence")), "window-40": Index(chunk("window", 40, 0)), "window-40-overlap-15": Index(chunk("window", 40, 15)), "article": Index(chunk("article"))}
    CH = {name: evaluate(ix, "dense", 3) for name, ix in IDX.items()}
    for k, v in CH.items():
        print("chunking", k, v, len(IDX[k].chunks), flush=True)
    span = [q for q in QUESTIONS if q[3] == "span"]
    SP = {name: evaluate(IDX[name], "dense", 3, qs=span) for name in ("window-40", "window-40-overlap-15")}
    print("span", SP, flush=True)
    ix = IDX["window-40-overlap-15"]
    kw = [q for q in QUESTIONS if q[3] == "keyword"]
    para = [q for q in QUESTIONS if q[3] == "paraphrase"]
    ME = {m: {"all": evaluate(ix, m, 3), "keyword": evaluate(ix, m, 3, qs=kw), "paraphrase": evaluate(ix, m, 3, qs=para)} for m in ("bm25", "dense", "hybrid", "rerank")}
    for m, v in ME.items():
        print("method", m, v, flush=True)
    TK = {k: evaluate(ix, "hybrid", k) for k in (1, 3, 8)}
    print("topk", TK, flush=True)
    refund_q = [q for q in QUESTIONS if q[1] == "billing-refunds" and "45" in q[2]]
    dep_hits = sum(1 for q in refund_q if any(ix.chunks[i][2] for i in ix.search(q[0], "dense", 3)))
    dep_hits_filtered = sum(1 for q in refund_q if any(ix.chunks[i][2] for i in ix.search(q[0], "dense", 3, exclude_deprecated=True)))
    DEP = {"refund_questions": len(refund_q), "outdated_article_in_top3": dep_hits, "outdated_in_top3_filtered": dep_hits_filtered,
           "answers_with_30_days": sum("30 days" in answer(ix, q[0], "dense", 3) for q in refund_q),
           "answers_with_30_days_filtered": sum("30 days" in answer(ix, q[0], "dense", 3, exclude_deprecated=True) for q in refund_q)}
    print("deprecated", DEP, flush=True)
    AN = {"hybrid@3": answer_accuracy(ix, "hybrid", 3, True), "hybrid@8": answer_accuracy(ix, "hybrid", 8, True), "rerank@3": answer_accuracy(ix, "rerank", 3, True)}
    print("answers", AN, flush=True)
    data.update({"chunking": CH, "chunk_counts": {k: len(v.chunks) for k, v in IDX.items()}, "span": SP, "methods": ME, "topk": TK, "deprecated": DEP, "answers": AN})
    report("Chunk size", "sentences lose context, whole articles cost tokens; mid-sized chunks balance both",
           " · ".join(f"{k}: recall@3 {v['recall_at_k']}%, MRR {v['mrr']}, {v['token_precision']}% of context tokens from the right article, {v['tokens_per_query']} tok" for k, v in CH.items()),
           CH["article"]["token_precision"] < CH["window-40-overlap-15"]["token_precision"] and CH["article"]["tokens_per_query"] > CH["window-40-overlap-15"]["tokens_per_query"])
    report("Answers split across a chunk boundary", "overlap keeps the two halves together, so the answering chunk ranks higher",
           f"span questions — MRR: no overlap {SP['window-40']['mrr']}, overlap {SP['window-40-overlap-15']['mrr']}",
           SP["window-40-overlap-15"]["mrr"] > SP["window-40"]["mrr"])
    report("BM25 vs dense", "keywords and IDs favour BM25; paraphrases favour dense",
           f"keyword questions: BM25 {ME['bm25']['keyword']['recall_at_k']}% vs dense {ME['dense']['keyword']['recall_at_k']}% · paraphrases: BM25 {ME['bm25']['paraphrase']['recall_at_k']}% vs dense {ME['dense']['paraphrase']['recall_at_k']}%",
           ME["bm25"]["keyword"]["recall_at_k"] >= ME["dense"]["keyword"]["recall_at_k"] and ME["dense"]["paraphrase"]["recall_at_k"] >= ME["bm25"]["paraphrase"]["recall_at_k"])
    report("Hybrid and reranking", "reranking puts the right chunk first; hybrid covers both kinds of questions",
           f"recall@3 all questions: BM25 {ME['bm25']['all']['recall_at_k']}%, dense {ME['dense']['all']['recall_at_k']}%, hybrid {ME['hybrid']['all']['recall_at_k']}%, rerank {ME['rerank']['all']['recall_at_k']}% (MRR {ME['rerank']['all']['mrr']})",
           ME["rerank"]["all"]["mrr"] >= ME["dense"]["all"]["mrr"] and ME["hybrid"]["keyword"]["recall_at_k"] >= ME["dense"]["keyword"]["recall_at_k"])
    report("Top-K", "more chunks raise recall and cost, and lower precision",
           " · ".join(f"K={k}: recall {v['recall_at_k']}%, precision {v['context_precision']}%, {v['tokens_per_query']} tokens" for k, v in TK.items()),
           TK[8]["recall_at_k"] >= TK[1]["recall_at_k"] and TK[8]["context_precision"] < TK[1]["context_precision"])
    report("Similar is not correct: an outdated article", "the 2023 policy is retrieved next to the current one; a metadata filter keeps it out",
           f"outdated article in top-3 for {DEP['outdated_article_in_top3']}/{DEP['refund_questions']} refund questions ({DEP['outdated_in_top3_filtered']} with the filter); answers citing the old 30 days: {DEP['answers_with_30_days']}",
           DEP["outdated_article_in_top3"] > 0 and DEP["outdated_in_top3_filtered"] == 0)
    with open(__file__.replace("rag.py", "rag_results.json"), "w") as fh:
        json.dump(data, fh)
    print(f"{sum(results)}/{len(results)} passed")
