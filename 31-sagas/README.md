# Part 31 lab: sagas

`saga.py` — PostgreSQL 17 with three databases (billing, entitlements, accounts), one local transaction per step.
1,000 "upgrade to Premium" flows: charge (simulated payment provider) → grant seats (15% fail) → activate plan (5% fail).
Compares: no compensation; an orchestrated saga with compensations; orchestrator crashes (3% of steps) with in-memory
vs durable saga state + recovery; provider timeouts (5%) retried with and without idempotency keys; a concurrent
report reading intermediate state with and without a 'pending' semantic lock.

Results in `saga_results.json`. Last run: 7/7 passed. ~2 min.

`choreography.py` — the same flow with no orchestrator: billing, entitlements and accounts react to events on a Redis 7.4
stream, each with its own PostgreSQL database. Scenarios: happy path; a service crashing after commit (3%) — the
unacked message is redelivered from the consumer group's pending list — without an outbox (event never published) and
with a per-service outbox (handlers crash after commit, relays crash after publishing and before marking, so events go
out twice); 5% duplicate deliveries with plain vs idempotent handlers. Faults use their own RNG so business outcomes match the happy path.
Results in `choreography_results.json`. Last run (after the 2026-10-25 audit): 5/5 passed. ~1 min.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
docker run -d --rm --name ul-redis -p 56379:6379 redis:7.4
pip install "psycopg[binary]" redis
python3 saga.py
python3 choreography.py
```
