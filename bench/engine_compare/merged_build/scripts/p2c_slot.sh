#!/bin/bash
# does a caught-up, idle mirror's slot keep up with WAL that is not for it?  (merged build, default settings)
Q=/tmp/qsb; S=$Q/scripts_merged; R=$Q/results_merged; PG=/usr/lib/postgresql/17/bin
psq() { su postgres -c "$PG/psql -X -h $Q/sock -p 5440 -d app -Atc \"$1\""; }
slot() { echo "$(date -u +%T) $(psq "select 'restart='||restart_lsn||' confirmed='||confirmed_flush_lsn||' head='||pg_current_wal_lsn()||' retained_MB='||((pg_current_wal_lsn()-restart_lsn)/1048576)::int||' unconfirmed_MB='||((pg_current_wal_lsn()-confirmed_flush_lsn)/1048576)::int||' active='||active from pg_replication_slots where slot_name='qs_bench_bench_1'")  readyz=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9199/readyz)"; }
psq "SELECT pg_drop_replication_slot('qs_bench')" >/dev/null
QS_M=$Q/mirror_new QS_BIN=$Q/bin_merged bash $S/qs_start.sh slot_new >/dev/null 2>&1 </dev/null
bash $S/qs_wait_ready.sh slot_new; sleep 20; slot
echo "mirror applied_lsn: $(python3 -c "import json;print(json.load(open('$Q/mirror_new/public.orders/state.json'))['applied_lsn'])")"
psq "CREATE TABLE public.junk(g int, t text); ALTER TABLE public.junk ALTER COLUMN t SET STORAGE EXTERNAL" >/dev/null
psq "select 'junk in publication: '||count(*) from pg_publication_tables where pubname='qs_bench' and tablename='junk'"
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
for i in 1 2 3 4 5 6; do
  psq "INSERT INTO junk SELECT g, repeat(md5(g::text), 30) FROM generate_series(1,700000) g" >/dev/null
  sleep 16; slot
  grep -q "dropped this mirror's replication slot\|could not drop" $R/qs_mirror_slot_new.log && break
done
echo "--- sidecar log (no slow-tick lines)"; grep -v "slow tick" $R/qs_mirror_slot_new.log | cut -c1-520 | tail -8
sleep 3; psq "select slot_name||' restart='||restart_lsn||' active='||active from pg_replication_slots"
kill "$(cat $Q/qs.pid)"; sleep 2
psq "DROP TABLE public.junk" >/dev/null
echo P2C_DONE
