# Part 36 lab: consistent hashing

`chash.py` — 100,000 keys placed with hash mod N, a hash ring (1, 10, 160 tokens per node), jump consistent hash and
rendezvous hashing; key movement from 4 to 5 nodes, balance, and node removal. A real cache: five Redis 7.4 nodes
(ports 56380–56384) warmed through 4-node routing, then read through 5-node routing (hit rate). Also a hot key taking
10% of reads, and replica-set placement.

Results in `chash_results.json`. Last run: 8/8 passed. ~1 min.

```bash
for i in 0 1 2 3 4; do docker run -d --rm --name ul-c$i -p 5638$i:6379 redis:7.4 --save "" --appendonly no; done
pip install redis
python3 chash.py
```
