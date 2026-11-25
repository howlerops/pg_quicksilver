#!/bin/bash
# stop the bench cluster, keep results, then run the repository's own e2e (part 3) pinned to cpus 2-3
E=/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/e2e
W=$1   # worktree
Q=/tmp/qsb; PG=/usr/lib/postgresql/17/bin
pkill -x qs-mirror; sleep 1
su postgres -c "$PG/psql -X -h $Q/sock -p 5440 -d app -Atc \"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active\"" >/dev/null
su postgres -c "$PG/pg_ctl -D $Q/pg/probe stop -m fast" | tail -1
su postgres -c "$PG/pg_ctl -D $Q/pg/primary stop -m fast" | tail -1
rm -rf $Q/mirror_new $Q/mirror_old $Q/results_merged/_o.txt $Q/results_merged/_c.txt
mkdir -p $E/p2; cp -r $Q/results_merged $Q/scripts_merged $E/p2/
umount $Q && rmdir $Q && echo "unmounted /tmp/qsb"
echo "=== setup_cluster.sh"; taskset -c 2-3 bash $W/bench/scripts/setup_cluster.sh 2>&1 | tail -6
echo "=== e2e_mirror.sh  $(date -u +%T)"
taskset -c 2-3 bash $W/bench/scripts/e2e_mirror.sh > $E/out/p3_e2e_mirror.txt 2>&1 </dev/null; echo "e2e_mirror.sh exit=$? at $(date -u +%T)"
echo P3_DONE
