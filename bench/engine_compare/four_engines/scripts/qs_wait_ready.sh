#!/bin/bash
Q=/tmp/qsb; tag=$1; t0=$(cat $Q/results/qs_start_$tag.ts)
for i in $(seq 36000); do curl -sf http://127.0.0.1:9199/readyz >/dev/null 2>&1 && break; sleep 0.1; done
t1=$(date +%s.%N); echo "ready_after_s $(echo "$t1 - $t0" | bc)" | tee $Q/results/qs_ready_$tag.txt
