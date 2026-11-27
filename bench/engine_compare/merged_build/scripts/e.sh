#!/bin/bash
# scenario e: idle compaction fires once
. "$(dirname "$0")/lib.sh"
L=$OUT/sc_a.log
{
echo "--- rebuild a 200k base (reload, forces a churn compaction)"
psq "TRUNCATE items" >/dev/null
su postgres -c "$PG/psql -X -q -h $BASE/sock -p $PP -d $DB -c \"INSERT INTO items SELECT gen_random_uuid(), g, 1+g%200, 'paid', round((random()*1e6)::numeric,2), now(), jsonb_build_object('g',g), CASE WHEN g%3=0 THEN NULL ELSE 'n '||g END, g, g%2=0, current_date FROM generate_series(1,200000) g\""
wait_applied a
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
for i in $(seq 300); do tr -d '\n ' < $E/mirror_a/public.items/state.json | grep -q '"delta_files":\(null\|\[\]\)' && break; sleep 0.2; done
sleep 2
echo "state: $(tr -d '\n ' < $E/mirror_a/public.items/state.json | cut -c1-260)"
n0=$(grep -c 'compaction started' $L)
echo "--- small churn at $(date -u +%T): 5000 updates + 2000 deletes (below the churn and dead-fraction triggers)"
psq "UPDATE items SET note='churn-e', amount=amount+1 WHERE seq BETWEEN 1000 AND 5999"
psq "DELETE FROM items WHERE seq BETWEEN 100000 AND 101999"
wait_applied a && echo "applied; last write at $(date -u +%T)"
echo "state: $(tr -d '\n ' < $E/mirror_a/public.items/state.json | cut -c1-400)"
ls $E/mirror_a/public.items/dv
python3 $E/scripts/viewcheck.py $E/mirror_a
echo "--- waiting 50 s"; sleep 50
grep 'T' $L | tail -n +1 | grep -n "why=" | tail -3 | cut -c1-200
echo "new 'compaction started' lines since churn: $(( $(grep -c 'compaction started' $L) - n0 ))"
echo "state: $(tr -d '\n ' < $E/mirror_a/public.items/state.json | cut -c1-400)"
ls $E/mirror_a/public.items/dv
python3 $E/scripts/viewcheck.py $E/mirror_a
verify a | head -8
echo "--- waiting 65 s more, expecting no second rewrite"; sleep 65
echo "at $(date -u +%T): new 'compaction started' lines since churn: $(( $(grep -c 'compaction started' $L) - n0 ))"
curl -s -o /dev/null -w "readyz=%{http_code}\n" http://127.0.0.1:9301/readyz
} 2>&1 | tee $OUT/e.txt
