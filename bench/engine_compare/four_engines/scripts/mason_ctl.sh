#!/bin/bash
# mason_ctl.sh start|stop  (pinned to cpus 0-2)
export PATH=/opt/node22/bin:$PATH; cd /tmp/qsb/mason
case $1 in
 start) taskset -c 0-2 npx mason start --json >/dev/null 2>&1; taskset -c 0-2 npx mason wait --json > /tmp/qsb/results/mason_wait_last.json 2>&1;;
 stop) taskset -c 0-2 npx mason stop >/dev/null 2>&1;;
esac
