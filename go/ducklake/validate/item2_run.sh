#!/bin/bash
# Item 2: rebuilds with publishing on. A reader queries the lake every 100 ms while a
# writer inserts; (a) a REBUILD file in the table's mirror directory and a restart;
# (b) the replication slot dropped on the primary, so the next start re-snapshots.
# Output: out/item2_reader.txt (every result), out/item2_writer.txt, out/item2.log,
# out/item2_report.txt (the distinct sequence of results, checked by item2_report.py).
. "$(dirname "$0")/qs2_env.sh"
export QS_VERIFY_BIN=$BIN/qs-verify
MIR=$BASE/mirror/item2
TAG=item2
TABLE=public.rb_ledger
SLOT=qs2_item2_item2
PUB=qs2_rb
RUN=${ITEM2_RUN:-150}     # seconds the reader and writer run in total
PHASE=${ITEM2_PHASE:-35}  # seconds between the steps

sc_stop $TAG
psq $PPORT postgres "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'qs2_item2%' AND NOT active" >/dev/null 2>&1
psq $PPORT app "DROP PUBLICATION IF EXISTS $PUB" >/dev/null 2>&1
psq $PPORT app "DROP TABLE IF EXISTS $TABLE" >/dev/null || die "drop table"
psq $PPORT app "CREATE TABLE $TABLE (id bigint PRIMARY KEY, amount int NOT NULL, note text)" >/dev/null || die "create"
psq $PPORT app "INSERT INTO $TABLE SELECT g, g % 1000, 'seed' FROM generate_series(1, 2000) g" >/dev/null || die "seed"
rm -rf "$MIR"

TABLES=$TABLE sc_start $TAG $TAG QS_DUCKLAKE=on QS_SLOW_TICK=1ms QS_PUBLICATION=$PUB || die start
sc_ready || die "not ready"
echo "bootstrapped: $(psq $PPORT app "SELECT count(*) FROM $TABLE")"

python3 "$HERE/item2_writer.py" $((RUN)) "$OUT/item2_writer.txt" &
WPID=$!
python3 "$HERE/item2_reader.py" "$MIR/_ducklake/catalog.sqlite" public rb_ledger "$OUT/item2_reader.txt" $((RUN)) &
RPID=$!
echo "$(date +%T.%N | cut -c1-12) writer and reader started"

sleep "$PHASE"
# (a) halt the table first: an unsafe schema change (a retyped column; a dropped one is
# evolved in place) halts it, and a REBUILD
# request is honoured only for a halted table (a request for a healthy one is discarded).
echo "$(date +%T.%N | cut -c1-12) (a) ALTER COLUMN amount TYPE bigint: the table halts"
psq $PPORT app "ALTER TABLE $TABLE ALTER COLUMN amount TYPE bigint" >/dev/null || die "retype column"
for _ in $(seq 300); do grep -q "halting public.rb_ledger" "$OUT/$TAG.log" 2>/dev/null && break; sleep 0.2; done
grep "halting public.rb_ledger" "$OUT/$TAG.log" | tail -1 | cut -c1-200
sleep 10
echo "$(date +%T.%N | cut -c1-12) (a) REBUILD marker written in $MIR/$TABLE and sidecar restarted"
touch "$MIR/$TABLE/REBUILD"
sc_stop $TAG
sc_start_again() { TABLES=$TABLE sc_start $TAG $TAG QS_DUCKLAKE=on QS_SLOW_TICK=1ms QS_PUBLICATION=$PUB; }
sc_start_again >/dev/null || die start
sc_ready || die "not ready after rebuild"
echo "$(date +%T.%N | cut -c1-12) (a) sidecar ready again"

sleep "$PHASE"
echo "$(date +%T.%N | cut -c1-12) (b) sidecar stopped, slot dropped on the primary, sidecar restarted (full re-snapshot)"
sc_stop $TAG
psq $PPORT postgres "SELECT pg_drop_replication_slot('$SLOT')" 2>&1 | tail -1
TABLES=$TABLE sc_start $TAG $TAG QS_DUCKLAKE=on QS_SLOW_TICK=1ms QS_PUBLICATION=$PUB >/dev/null || die start
sc_ready || die "not ready after re-snapshot"
echo "$(date +%T.%N | cut -c1-12) (b) sidecar ready again"

wait $WPID
wait $RPID
sc_stop $TAG
python3 "$HERE/item2_report.py" "$OUT/item2_reader.txt" "$OUT/item2_writer.txt" | tee "$OUT/item2_report.txt"
