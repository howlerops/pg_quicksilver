#!/bin/bash
# part 3: apply the slot-name fix to bench/scripts (run from bench/scripts)
set -e
add() { # file base pod anchor
  sed -i "0,/^$4\$/s//&\nsource \"\$(dirname \"\$0\")\/lib_slotname.sh\"\nSLOT=\$(qs_pod_slot $2 $3)   # what the sidecar derives from QS_SLOT and QS_POD_NAME/" "$1"; }
add bootstrap_profile.sh bp_slot bp-2 'DB=app'
add e2e_mirror.sh qs_e2e e2e-2 'DB=app'
add large_scale.sh lg_slot lg-2 'DB=large'
add rendering_fidelity.sh tz_slot tz-2 'DB=tzapp'
add slot_guard.sh gd_slot gd-2 'DB=guard'
add workload_matrix.sh mx_slot mx-2 'DB=app'
drop() { sed -i "s/pg_drop_replication_slot('$2') FROM pg_replication_slots WHERE slot_name='$2'/pg_drop_replication_slot('\$SLOT') FROM pg_replication_slots WHERE slot_name='\$SLOT'/" "$1"; }
drop bootstrap_profile.sh bp_slot
drop e2e_mirror.sh qs_e2e
drop large_scale.sh lg_slot
drop rendering_fidelity.sh tz_slot
drop slot_guard.sh gd_slot
drop workload_matrix.sh mx_slot
sed -i "s/WHERE slot_name='lg_slot'\" postgres/WHERE slot_name='\$SLOT'\" postgres/" large_scale.sh
sed -i "s/WHERE slot_name='tz_slot' AND active/WHERE slot_name='\$SLOT' AND active/" rendering_fidelity.sh
sed -i "s/WHERE slot_name='gd_slot'\" postgres/WHERE slot_name='\$SLOT'\" postgres/; s/ok \"no gd_slot exists/ok \"no \$SLOT exists/; s/ok \"the current gd_slot/ok \"the current \$SLOT/; s/bad \"a slot named gd_slot/bad \"a slot named \$SLOT/" slot_guard.sh
for f in *.sh; do bash -n $f || echo "SYNTAX $f"; done; echo "bash -n done"
source ./lib_slotname.sh
qs_pod_slot qs_e2e e2e-2; qs_pod_slot quicksilver_app app-1; qs_pod_slot qs_e2e ""; qs_pod_slot qs_e2e a.b; qs_pod_slot quicksilver_ccccccccccccccccccccccccccccccccccccccccccccc cluster-with-a-long-name-10
