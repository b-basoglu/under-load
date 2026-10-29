# Part 10 lab: write-through vs write-back

`write_paths.py` — PostgreSQL 17 + Redis 7.4: view counting with write-through vs write-back (Redis HINCRBY + 1 s batch
flush), database lag, Redis killed before a flush with no persistence / AOF everysec / AOF always (own container
`ul-wb` on port 56380, restarted by the script), and out-of-order flushes with and without a version guard.
Results in `write_paths_results.json`. Last run: 4/4 passed (twice after flusher drain + stronger asserts).
Killing the container is a process crash, not a power cut — AOF everysec still keeps the buffer here.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install "psycopg[binary]" redis
python3 write_paths.py
```
