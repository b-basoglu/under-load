# Part 29 lab: transactional outbox (polling relay and Debezium)

`outbox.py` — PostgreSQL 17 (`wal_level=logical`) + Apache Kafka 3.9 (KRaft) + Debezium 3.0 on Kafka Connect.
- Dual write: commit-then-publish vs publish-then-commit with a failure between them on 2% of 1,000 requests.
- Polling relay (`FOR UPDATE SKIP LOCKED`, `published_at` status column): 1 s vs 100 ms interval, relay crashes
  between publish and mark, the `seq > last_seen` watermark bug with late-committing transactions, and ordering with
  1 worker, 2 workers, and 2 hash-sharded workers.
- Debezium Postgres connector + Outbox Event Router: latency and loss with rows deleted in the same transaction,
  WAL retained by the replication slot while Connect is paused, a quiet outbox pinning WAL, and a Connect worker
  killed mid-stream.

Results in `outbox_results.json`. Last run: 13/13 passed. ~5 min.

```bash
docker network create ul-net
docker run -d --rm --name ul-kafka --network ul-net -p 59092:59092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:59092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://ul-kafka:9092,EXTERNAL://localhost:59092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@ul-kafka:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 apache/kafka:3.9.1
docker run -d --rm --name ul-pgcdc --network ul-net -p 55433:5432 -e POSTGRES_PASSWORD=test postgres:17 -c wal_level=logical
docker run -d --rm --name ul-connect --network ul-net -p 58083:8083 -e BOOTSTRAP_SERVERS=ul-kafka:9092 \
  -e GROUP_ID=ul-connect -e CONFIG_STORAGE_TOPIC=ul_connect_configs -e OFFSET_STORAGE_TOPIC=ul_connect_offsets \
  -e STATUS_STORAGE_TOPIC=ul_connect_statuses -e CONFIG_STORAGE_REPLICATION_FACTOR=1 -e OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e STATUS_STORAGE_REPLICATION_FACTOR=1 -e OFFSET_FLUSH_INTERVAL_MS=1000 quay.io/debezium/connect:3.0.8.Final
pip install "psycopg[binary]" kafka-python
python3 outbox.py
```
