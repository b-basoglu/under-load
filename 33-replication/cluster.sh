#!/bin/zsh
# PostgreSQL 17 primary (55440) + two streaming replicas r1 (55441), r2 (55442). Recreates everything.
docker network create ul-rep 2>/dev/null
docker rm -f ul-pgp ul-pgr1 ul-pgr2 >/dev/null 2>&1
docker run -d --name ul-pgp --network ul-rep -p 55440:5432 -e POSTGRES_PASSWORD=test postgres:17 -c wal_level=replica -c max_wal_senders=10 >/dev/null
sleep 6
docker exec ul-pgp psql -U postgres -qc "CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD 'repl'"
docker exec ul-pgp bash -c "echo 'host replication repl all md5' >> /var/lib/postgresql/data/pg_hba.conf"
docker exec ul-pgp psql -U postgres -qc "SELECT pg_reload_conf()" >/dev/null
for r in 1 2; do
  docker run -d --name ul-pgr$r --network ul-rep -p 5544$r:5432 -e PGPASSWORD=repl --entrypoint bash postgres:17 -c "
    rm -rf /var/lib/postgresql/data/*; until pg_basebackup -h ul-pgp -U repl -D /var/lib/postgresql/data -R -X stream -C -S slot$r -c fast; do sleep 1; done
    echo \"primary_conninfo = 'host=ul-pgp user=repl password=repl application_name=r$r'\" >> /var/lib/postgresql/data/postgresql.auto.conf
    chown -R postgres:postgres /var/lib/postgresql/data; chmod 700 /var/lib/postgresql/data
    exec gosu postgres postgres -D /var/lib/postgresql/data -c hot_standby=on" >/dev/null
done
sleep 12
docker exec ul-pgp psql -U postgres -c "SELECT application_name, state, sync_state FROM pg_stat_replication"
