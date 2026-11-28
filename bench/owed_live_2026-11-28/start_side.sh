#!/usr/bin/env bash
# start_side.sh ENVFILE LOG PIDFILE  (run inside a node's mount namespace by ns_exec.sh)
# Starts qs-mirror as postgres with the variables of ENVFILE, detached, and records its PID.
envf=$1; log=$2; pidf=$3
BASE=/var/lib/postgresql/w2e
# shellcheck disable=SC2046  # the file is one NAME=value per word, split on purpose
setsid runuser -u postgres -- env $(cat "$envf") "$BASE/qs-mirror" >> "$log" 2>&1 < /dev/null &
echo $! > "$pidf"
