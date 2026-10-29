# Part 38 lab: clock skew

`clocks.py` — containers share the host clock, so each server's clock is modelled explicitly (true time + offset, or a
step). Last-write-wins updates to five busy tickets from two servers are applied to a real PostgreSQL 17 table, comparing
wall-clock timestamps (0, 50, 300 ms skew), a hybrid logical clock, and a database-assigned version. Also: merged log
ordering under skew, a 5 s timeout across a ±2 s NTP step (wall vs monotonic), and lease overlap with 500 ms skew,
with and without a safety margin.

Results in `clocks_results.json`. Last run (after the 2026-10-25 audit): 7/7 passed. ~30 s.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install "psycopg[binary]"
python3 clocks.py
```
