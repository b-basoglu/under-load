# Part 27 lab: message queues and delivery guarantees

`mq.py` — Redis 7.4 Streams (consumer group, XREADGROUP / XACK / XAUTOCLAIM) + PostgreSQL 17.
- 2,000 `ticket.created` events; the consumer crashes at a random point on 2% of messages. Compares ack-before-work
  (at-most-once), ack-after-work (at-least-once), an idempotent consumer (processed-messages table in the same
  transaction), and the same with an idempotency key passed to the (simulated) email provider.
- Ordering: 300 tickets × 3 status events, 10% transient failures redelivered later; last-write-wins vs version check.
- Lag: a producer at 400 msgs/s with 1, 2 and 4 consumers.

Results in `mq_results.json`. Last run: 7/7 passed. ~60 s.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install "psycopg[binary]" redis
python3 mq.py
```
