#!/bin/bash
# cross-check bench/scripts/lib_slotname.sh against the names the real sidecar derives, then tear part 1 down
. "$(dirname "$0")/lib.sh"
SCR=$1
{
stop_sc a
for pod in a.b Pod_X cluster-with-a-very-long-name-that-goes-past-the-limit-of-sixty-three-bytes-7; do
  start_sc x "$pod" 9303 >/dev/null; sleep 2.5; stop_sc x
  got=$(grep "pod=$pod " $OUT/sc_x.log | tail -1 | sed 's/.* slot=\([^ ]*\) .*/\1/')
  want=$(bash -c ". $SCR/lib_slotname.sh; qs_pod_slot qs_e2e '$pod'")
  echo "pod=$pod sidecar=$got lib=$want $([ "$got" = "$want" ] && echo SAME || echo DIFFERENT)"
done
slots
psq "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" >/dev/null
su postgres -c "$PG/pg_ctl -D $BASE/standby stop -m fast" | tail -1
su postgres -c "$PG/pg_ctl -D $BASE/primary stop -m fast" | tail -1
rm -rf $BASE $E/mirror_x
ls /var/lib/postgresql
} 2>&1 | tee $OUT/h.txt
