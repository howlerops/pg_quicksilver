#!/usr/bin/env bash
# Scenario g: one transaction that updates 500,000 rows of a mirrored table.
# fv.accounts first grows to ~700k rows (a separate insert), then a single transaction
# updates exactly 500,000 of them. The sidecar's RSS is sampled every 0.2 s throughout.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
q $PPORT "INSERT INTO fv.accounts SELECT 'BG' || lpad(i::text, 7, '0'), (i % 100000)::numeric / 10, now(), 'bulk' FROM generate_series(1, 500000) i" && echo "grew fv.accounts"
q $PPORT "ANALYZE fv.accounts" >/dev/null
SPID=$(cat $BASE/mirror.pid)
( while kill -0 $SPID 2>/dev/null; do echo "$(date -u +%T.%N | cut -c1-12) $(awk '/VmRSS/{print $2}' /proc/$SPID/status) $(awk '/VmHWM/{print $2}' /proc/$SPID/status)"; sleep 0.2; done ) > $OUT/g_rss_samples.txt 2>&1 &
RP=$!
echo "rss before: $(awk '/VmRSS/{print $2" kB"}' /proc/$SPID/status) hwm $(awk '/VmHWM/{print $2" kB"}' /proc/$SPID/status)"
START=$(date +%s.%N)
q $PPORT "BEGIN; UPDATE fv.accounts SET label = 'bulk-' || (substr(code, 3, 7)::int % 97) WHERE code IN (SELECT code FROM fv.accounts WHERE code LIKE 'BG%' ORDER BY code LIMIT 500000); COMMIT;" | tail -1
END=$(date +%s.%N)
echo "update seconds: $(python3 -c "print(round($END-$START,2))")"
q $PPORT "SELECT count(*) FROM fv.accounts WHERE label LIKE 'bulk-%'"
echo "lag after commit:"; for _ in $(seq 1200); do curl -s http://$HEALTH/readyz | grep -q '"lag_bytes":[0-9]\{1,6\},' && break; sleep 0.2; done; curl -s http://$HEALTH/readyz | cut -c1-200; echo
sleep 1
kill $RP 2>/dev/null
echo "rss samples: $(wc -l < $OUT/g_rss_samples.txt) ; peak VmRSS kB $(awk 'BEGIN{m=0}{if($2>m)m=$2}END{print m}' $OUT/g_rss_samples.txt) ; VmHWM kB at end $(awk '/VmHWM/{print $2}' /proc/$SPID/status)"
bash $FV/scripts/check_all.sh g
