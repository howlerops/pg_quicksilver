#!/usr/bin/env bash
# check_all.sh LABEL [TABLES]: after a scenario, settle the mirror, then
#   - lake vs heap (fv_compare.py; it waits until every mirror has the primary's WAL position)
#   - qs-verify on every mirrored table (MATCH or DIVERGED)
#   - routing and equality on the standby (fv_routing.py)
# TABLES defaults to the twelve; pass a comma list to leave out a halted table.
# Raw output goes to out/LABEL_*.txt.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
L=${1:?label}
TL=${2:-$ALLT}
timeout 900 python3 -I $FV/scripts/fv_compare.py --port ${P:-$PPORT} --tables $TL --base $BASE --mirror ${MD:-$BASE/mirror} ${TSTEXT:+--ts-text} > $OUT/${L}_lake_vs_heap.txt 2>&1
echo "lake_vs_heap exit $?" >> $OUT/${L}_lake_vs_heap.txt
: > $OUT/${L}_qs_verify.txt
for t in ${TL//,/ }; do echo "$t: $(verify $t)" >> $OUT/${L}_qs_verify.txt; done
timeout 600 python3 -I $FV/scripts/fv_routing.py --port ${S:-$SPORT} > $OUT/${L}_routing.txt 2>&1
echo "routing exit $?" >> $OUT/${L}_routing.txt
echo "== $L"
echo "qs-verify: $(grep -c ': MATCH$' $OUT/${L}_qs_verify.txt) MATCH, $(grep -vc ': MATCH$' $OUT/${L}_qs_verify.txt) other"
grep -v ': MATCH$' $OUT/${L}_qs_verify.txt | sed 's/^/  not match: /'
grep -E 'tables|exit' $OUT/${L}_lake_vs_heap.txt
grep -E 'different' $OUT/${L}_routing.txt
grep -E 'routed_to_mirror=True' $OUT/${L}_routing.txt | sed 's/^/  /'
