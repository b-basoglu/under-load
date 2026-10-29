# Part 28 lab: dead letter queues

`dlq.py` — Redis 7.4 Streams + PostgreSQL 17.
- Poison message on an ordered stream: retried forever vs 5 attempts then DLQ.
- Failure classification on 2,000 messages (98 transient, 14 permanent): no retries vs retry-all vs classified.
- Replay of 250 partially processed messages through a non-idempotent vs idempotent path.
- Replay speed: 1,500 DLQ messages replayed at once vs throttled to 60/s, with live traffic at 150/s on ~240/s of consumers.

Results in `dlq_results.json`. Last run: 9/9 passed. ~60 s.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install "psycopg[binary]" redis
python3 dlq.py
```
