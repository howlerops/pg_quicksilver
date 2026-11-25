#!/bin/bash
# scenario f: a value the mirror cannot store exactly (NaN in numeric(12,2))
. "$(dirname "$0")/lib.sh"
L=$OUT/sc_a.log
probe() { for p in readyz healthz; do printf '%s -> HTTP %s body: %s\n' $p "$(curl -s -o $OUT/f_body.txt -w '%{http_code}' http://127.0.0.1:9301/$p)" "$(cut -c1-600 $OUT/f_body.txt)"; done; }
{
n0=$(wc -l < $L)
echo "--- before"; probe
echo "--- insert NaN at $(date -u +%T)"
psq "INSERT INTO items VALUES ('00000000-0000-0000-0000-00000000dead', 999999001, 1, 'nan-row', 'NaN', now(), '{}', 'nan', 1, true, current_date)"
psq "INSERT INTO items SELECT gen_random_uuid(), 999999100+g, 1, 'after-nan', 1.00, now(), '{}', 'after', g, true, current_date FROM generate_series(1,100) g"
sleep 3
echo "--- 3 s later"; probe
sleep 20
echo "--- 23 s later"; probe
echo "--- sidecar alive? $(kill -0 $(cat $OUT/sc_a.pid) 2>/dev/null && echo yes || echo no)"
echo "--- new log lines (first 6 + count)"; tail -n +$((n0+1)) $L | cut -c1-900 | head -6; echo "new log lines total: $(tail -n +$((n0+1)) $L | wc -l)"
echo "--- metrics (halt/ready/verif related)"; curl -s http://127.0.0.1:9301/metrics | grep -i "halt\|ready\|verif\|diverg\|lag" | grep -v '^#' | head -12
echo "--- mirror dir"; ls $E/mirror_a/public.items; echo "state: $(tr -d '\n ' < $E/mirror_a/public.items/state.json | cut -c1-500)"
echo "--- qs-verify while halted"; verify a | head -12
echo "--- view from qs-query while halted"; python3 $E/scripts/viewcheck.py $E/mirror_a 2>&1 | tail -7
slots
echo "--- restart sidecar without fixing anything"
stop_sc a; n1=$(wc -l < $L); start_sc a pod-a 9301; sleep 8; probe
tail -n +$((n1+1)) $L | cut -c1-700 | head -5
} 2>&1 | tee $OUT/f.txt
