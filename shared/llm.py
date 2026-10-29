"""Shared helpers for the LLM labs (Parts 43–53): one small open model, run locally with PyTorch + Transformers.

Model: Qwen/Qwen2.5-0.5B-Instruct (0.5B parameters), float32 on CPU or MPS. Small enough for a laptop, real enough to
show how sampling, decoding and inference behave. Set HF_HOME to cache the weights.
"""
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL = "Qwen/Qwen2.5-0.5B-Instruct"
_cache = {}


def load(name=MODEL, device="cpu", dtype=torch.float32):
    key = (name, device, dtype)
    if key not in _cache:
        tok = AutoTokenizer.from_pretrained(name)
        model = AutoModelForCausalLM.from_pretrained(name, dtype=dtype).to(device).eval()
        # The checkpoint's generation_config sets repetition_penalty 1.05–1.1, top_k 20, top_p 0.8 and do_sample.
        # Those defaults would silently apply to every generate() call (greedy included), so reset to neutral:
        # each lab passes the decoding settings it means to test.
        gc = model.generation_config
        gc.do_sample, gc.temperature, gc.top_p, gc.top_k, gc.repetition_penalty = False, 1.0, 1.0, None, 1.0
        _cache[key] = (tok, model)
    return _cache[key]


def chat_ids(tok, messages, device="cpu", tools=None):
    enc = tok.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt", return_dict=True, tools=tools)
    return enc["input_ids"].to(device)


def generate(tok, model, messages, max_new_tokens=60, device="cpu", **kw):
    ids = chat_ids(tok, messages, device)
    with torch.no_grad():
        out = model.generate(ids, max_new_tokens=max_new_tokens, pad_token_id=tok.eos_token_id, **kw)
    return tok.decode(out[0][ids.shape[1]:], skip_special_tokens=True)
