# Part 26 lab: head-of-line blocking

`hol.py` — seeded discrete-event simulations:
- A. a 4-worker pool with ticket reads (10 ms, 150/s) and CSV exports (2 s, 0.75/s): one shared FIFO vs separate pools vs 8 shared workers;
- B. one ordered webhook partition (100 msgs/s, 50 tenants) where one tenant's endpoint takes 2 s: serial vs 50 ms timeout + retry topic vs per-key concurrency;
- C. one connection with 8 × 120 KB responses: HTTP/1.1 vs HTTP/2 with a slow first response, and HTTP/2 (TCP) vs HTTP/3 (QUIC) at 1% packet loss.

Results in `hol_results.json`. Last run (after the 2026-10-25 audit): 8/8 passed. ~5 s.

```bash
python3 hol.py
```
