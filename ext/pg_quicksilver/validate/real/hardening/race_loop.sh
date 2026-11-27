#!/usr/bin/env bash
# race_loop.sh SEQ N OUT: runs race_SEQ.sql N times, each in a fresh backend (psql), against the
# cluster that env.sh describes (PPORT, PG, LD_LIBRARY_PATH). EXPECT is the forced-mirror answer
# (default 'mirror'). One line per iteration goes to OUT:
#   <iter> | <side> | <reason> | <detail> | answer=<value> [| error]
# and the summary prints the iterations whose answer was not EXPECT.
# Run as root: psql runs as the postgres user, as setup.sh does. The SQL goes in on stdin.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
seq=$1; n=$2; out=$3
f=$HERE/race_$seq.sql
: "${PPORT:?set PPORT}"; : "${PG:?set PG}"; EXPECT=${EXPECT:-mirror}
: > "$out"
for i in $(seq 1 "$n"); do
  r=$(su postgres -c "LD_LIBRARY_PATH=$LD_LIBRARY_PATH $PG/psql -X -q -A -t -h /tmp -p $PPORT -U postgres -d app -f -" < "$f" 2>&1)
  ans=$(printf '%s\n' "$r" | sed -n '/^ANSWER$/,/^DECISION$/p' | grep -v -E '^(ANSWER|DECISION)$' | paste -sd, -)
  dec=$(printf '%s\n' "$r" | sed -n '/^DECISION$/{n;p;}' | head -1)
  err=$(printf '%s\n' "$r" | grep -m1 -E '^(ERROR|psql):' || true)
  echo "$i | ${dec:-NODECISION} | answer=${ans:-NOANSWER}${err:+ | $err}" >> "$out"
done
echo "seq=$seq iterations=$n not_expected=$(grep -vc "answer=$EXPECT\$" "$out")"
grep -v "answer=$EXPECT\$" "$out" | head -20
