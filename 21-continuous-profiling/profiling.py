"""A tiny sampling profiler and a CPU regression, for Part 21. Pure Python, single process.

render_ticket() is a simplified ticket-page handler. Release v2 adds a "harmless" change: PII masking now builds its
regular expressions inside the function (on every call) and deep-copies the ticket "for safety". A background thread
samples the worker thread's stack at a fixed rate (sys._current_frames), the way production sampling profilers do.
"""
import collections
import copy
import json
import re
import sys
import threading
import time

results, data = [], {}
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PHONE = re.compile(r"\+?\d[\d\s().-]{7,}\d")


def report(name, claim, observed, ok):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}\n       claim:    {claim}\n       observed: {observed}\n", flush=True)


TICKET = {"id": 48213, "subject": "Refund for order 1182",
          "comments": [{"author": f"agent{i}", "body": ("Customer jane.doe@example.com called from +1 (555) 010-2233 about the refund. " * 6)}
                       for i in range(25)]}


def load_ticket():
    return TICKET


def render_markdown(text):
    out = []
    for line in text.split(". "):
        out.append("<p>" + line.replace("refund", "<b>refund</b>") + "</p>")
    return "".join(out)


def mask_pii_v1(text):
    return PHONE.sub("[phone]", EMAIL.sub("[email]", text))


def mask_pii_v2(text):
    email = re.compile(r"(?:[\w.+-]+)@(?:[\w-]+)\.(?:[\w.]+)", re.UNICODE)
    phone = re.compile(r"\+?\d(?:[\d\s().-]){7,}\d", re.UNICODE)
    for _ in range(3):   # "make sure we got everything"
        text = phone.sub("[phone]", email.sub("[email]", text))
    return text


def serialize(page):
    return json.dumps(page)


def render_ticket(version):
    t = load_ticket()
    if version == 2:
        t = copy.deepcopy(t)
    mask = mask_pii_v1 if version == 1 else mask_pii_v2
    comments = [{"author": c["author"], "html": render_markdown(mask(c["body"]))} for c in t["comments"]]
    return serialize({"id": t["id"], "subject": t["subject"], "comments": comments})


class Sampler:
    def __init__(self, thread_id, hz):
        self.tid, self.interval, self.stacks, self.running = thread_id, 1.0 / hz, collections.Counter(), True
        self.t = threading.Thread(target=self.loop, daemon=True)
        self.taken, self.t0 = 0, time.time()

    def loop(self):
        while self.running:
            f = sys._current_frames().get(self.tid)
            stack = []
            while f is not None:
                stack.append(f.f_code.co_name)
                f = f.f_back
            self.stacks[";".join(reversed(stack))] += 1
            self.taken += 1
            time.sleep(self.interval)

    def start(self):
        self.t.start(); return self

    def stop(self):
        self.running = False; self.t.join()
        self.achieved_hz = round(self.taken / (time.time() - self.t0), 1)   # sleep + GIL waits make it lower than asked


def workload(version, seconds, hz=None):
    out = {}

    def worker():
        n, t_end = 0, time.time() + seconds
        cpu0 = time.thread_time()
        while time.time() < t_end:
            render_ticket(version); n += 1
        out["n"], out["cpu"] = n, time.thread_time() - cpu0
    w = threading.Thread(target=worker)
    w.start()
    s = Sampler(w.ident, hz).start() if hz else None
    w.join()
    if s:
        s.stop()
    if s:
        s.stacks["__achieved_hz__"] = 0
        out["hz"] = s.achieved_hz
    return out["n"] / seconds, out["cpu"] / out["n"] * 1000, s.stacks if s else None, out.get("hz")


def summarize(stacks):
    stacks = {k: v for k, v in stacks.items() if k != "__achieved_hz__"}
    total = sum(stacks.values())
    own, incl = collections.Counter(), collections.Counter()
    for st, c in stacks.items():
        frames = [f for f in st.split(";") if f not in ("<module>", "_bootstrap", "_bootstrap_inner", "run", "worker")]
        if not frames:
            continue
        own[frames[-1]] += c
        for f in set(frames):
            incl[f] += c
    return total, {k: round(100 * v / total, 2) for k, v in incl.items()}, {k: round(100 * v / total, 1) for k, v in own.most_common(8)}


def folded(stacks):
    out = collections.Counter()
    for st, c in stacks.items():
        if st == "__achieved_hz__":
            continue
        frames = [f for f in st.split(";") if f not in ("<module>", "_bootstrap", "_bootstrap_inner", "run", "worker")]
        if frames and frames[0] == "render_ticket":
            out[";".join(frames[:4])] += c
    return dict(out.most_common(40))


if __name__ == "__main__":
    # 1. overhead of sampling
    import statistics
    ov, achieved, bases = {}, {}, []
    for rep_ in range(3):                                   # interleaved repeats; the median damps noise
        bases.append(workload(1, 4)[0])
        for hz in (19, 99, 999):
            r, _, _, got = workload(1, 4, hz)
            ov.setdefault(hz, []).append(100 * (bases[-1] - r) / bases[-1])
            achieved.setdefault(hz, []).append(got)
    ov = {hz: round(statistics.median(v), 1) for hz, v in ov.items()}
    achieved = {hz: round(statistics.median(v)) for hz, v in achieved.items()}
    base = statistics.median(bases)
    data["overhead_pct"], data["achieved_hz"], data["baseline_rps"] = ov, achieved, round(base)
    report("Sampling overhead (median of 3 interleaved runs)", "low sampling rates cost a few percent at most; very high rates cost more",
           " · ".join(f"asked {hz} Hz, got {achieved[hz]} Hz: {ov[hz]}% throughput" for hz in ov),
           ov[99] < 5 and ov[19] < 5 and achieved[99] > 20)
    # 2. the regression
    r1, c1, s1, hz1 = workload(1, 10, 99)
    r2, c2, s2, hz2 = workload(2, 10, 99)
    data["releases"] = {"v1": {"rps": round(r1), "cpu_ms_per_request": round(c1, 3)}, "v2": {"rps": round(r2), "cpu_ms_per_request": round(c2, 3)}}
    n1, inc1, own1 = summarize(s1)
    n2, inc2, own2 = summarize(s2)
    top = lambda inc: dict(sorted(inc.items(), key=lambda kv: -kv[1])[:12])
    data["profile_v1"] = {"samples": n1, "achieved_hz": hz1, "inclusive": top(inc1), "self": own1, "folded": folded(s1)}
    data["profile_v2"] = {"samples": n2, "achieved_hz": hz2, "inclusive": top(inc2), "self": own2, "folded": folded(s2)}
    report("Release v2: CPU per request", "the 'harmless' change makes each request much more expensive",
           f"v1 {c1:.3f} ms CPU/request ({r1:,.0f} req/s) → v2 {c2:.3f} ms ({r2:,.0f} req/s): ×{c2 / c1:.1f}",
           c2 > 1.5 * c1 and r1 > 1.5 * r2)
    ms = lambda inc, k, c: round(inc.get(k, 0) / 100 * c, 3)
    diff = {"mask_pii": (ms(inc1, "mask_pii_v1", c1), ms(inc2, "mask_pii_v2", c2)),
            "render_markdown": (ms(inc1, "render_markdown", c1), ms(inc2, "render_markdown", c2)),
            "serialize": (ms(inc1, "serialize", c1), ms(inc2, "serialize", c2)),
            "deepcopy": (0.0, ms(inc2, "deepcopy", c2)), "re.compile": (ms(inc1, "compile", c1), ms(inc2, "compile", c2))}
    data["diff_ms_per_request"] = diff
    report("Comparing the two profiles in CPU ms per request (share × CPU per request)",
           "percent shares barely move; absolute time per request shows the regression, and it's not where the guess was",
           f"share of samples in masking: {inc1.get('mask_pii_v1', 0)}% → {inc2.get('mask_pii_v2', 0)}%; ms/request: "
           + ", ".join(f"{k} {a} → {b}" for k, (a, b) in diff.items()),
           diff["mask_pii"][1] > 2 * diff["mask_pii"][0]
           and diff["re.compile"][1] < 0.1 * diff["mask_pii"][1]
           and abs(inc1.get("mask_pii_v1", 0) - inc2.get("mask_pii_v2", 0)) < 15)
    # 3. a rare event: sampling vs a function that uses ~0.2% of CPU
    data["rare"] = {"rule": "at 99 Hz for 60 s there are ~5,940 samples; a function using 0.2% of CPU gets ~12, one using 0.01% gets ~0.6"}
    with open(__file__.replace("profiling.py", "profiling_results.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"{sum(results)}/{len(results)} passed")
