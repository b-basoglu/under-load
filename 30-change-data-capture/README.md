# Part 30 lab: change data capture

`cdc.py` — PostgreSQL 17 (`wal_level=logical`) + Kafka 3.9 + Debezium 3.0 Postgres connector (`pgoutput`) capturing the
`tickets` table directly (JSON converter, schemas disabled). Uses the same containers as Part 29.
- A 20,000-row table under a live workload (updates, deletes, inserts) while the connector snapshots and streams; a
  copy rebuilt only from events is compared with the table. A 1-second `updated_at` poller runs alongside.
- Before images with `REPLICA IDENTITY DEFAULT` vs `FULL`; an `ADD COLUMN` schema change.
- `max_slot_wal_keep_size = 64MB`, connector paused while 160 MB is written: slot invalidated; then recovery with a new
  slot and a fresh snapshot.

Results in `cdc_results.json`. Last run: 8/8 passed. ~3 min.

```bash
# containers as in ../../29-transactional-outbox/lab/README.md
pip install "psycopg[binary]" kafka-python
python3 cdc.py
```
