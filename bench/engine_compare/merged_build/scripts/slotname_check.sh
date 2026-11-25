#!/bin/bash
# prints what bench/scripts/lib_slotname.sh derives; argument: path to bench/scripts
. "$1/lib_slotname.sh"
qs_pod_slot qs_e2e ""; qs_pod_slot qs_e2e e2e-2; qs_pod_slot qs_e2e a.b; qs_pod_slot Qs.E2e A_b
qs_pod_slot quicksilver_ccccccccccccccccccccccccccccccccccccccccccccc cluster-with-a-long-name-10
