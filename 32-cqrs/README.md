# Part 32 lab: CQRS and materialized projections

`cqrs.py` — PostgreSQL 17. Write model: 200,000 tickets, 1M comments, tags, users, 50 tenants (Halcyon = 40,000 tickets).
Read models: `ticket_list_view` (one row per ticket) and `tenant_status_counts`, maintained by a projector from outbox
events written in the same transaction as each command.
- Agent queue query from the write model vs the projection; dashboard counts via GROUP BY vs projection.
- Projection lag and read-after-write staleness at 300 commands/s.
- Dashboard counts updated synchronously in the command vs by the projector, 16 concurrent writers on one tenant.
- 15% duplicate event delivery with a plain increment vs a version-aware handler; divergence check.
- Rebuild into a new table and swap.

Results in `cqrs_results.json`. Last run (after the 2026-10-25 audit): 7/7 passed. ~1 min.

```bash
docker run -d --rm --name ul-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
pip install "psycopg[binary]"
python3 cqrs.py
```
