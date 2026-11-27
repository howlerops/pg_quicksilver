#!/usr/bin/env bash
# f_primary.sh FIRST N: on the primary, as the postgres OS user, ALTER TABLE public.orders ADD COLUMN
# x<FIRST>..x<FIRST+N-1> int, one at a time, with a 0.2 s pause between them (a few per second).
# Each ALTER's result (or error) is printed, so the run is on record.
set -uo pipefail
PG=${PG:-/var/lib/postgresql/qsexp/pg17/usr/lib/postgresql/17/bin}
first=${1:?first column number}; n=${2:?count}
start=$(date +%s.%N)
errors=0
for i in $(seq "$first" $((first + n - 1))); do
  out=$("$PG/psql" -X -q -h /tmp -p 5860 -U postgres -d app -c "ALTER TABLE public.orders ADD COLUMN x$i int" 2>&1) || { errors=$((errors + 1)); echo "alter x$i: $out"; }
  sleep 0.2
done
end=$(date +%s.%N)
echo "f_primary: $n ALTER TABLE ... ADD COLUMN x$first..x$((first + n - 1)) on the primary in $(echo "$end - $start" | bc) s, errors=$errors"
