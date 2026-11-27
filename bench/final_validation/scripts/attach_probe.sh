#!/usr/bin/env bash
# attach_probe.sh PORT: call quicksilver.attach_lakes() three times and show the lakes registry
. "$(dirname "$0")/env.sh"
# shellcheck disable=SC2034  # not read here: a loop counter, or read by a script that sources this
for i in 1 2 3; do echo "attach_lakes: $(q $1 'SELECT quicksilver.attach_lakes()')"; sleep 1; done
q $1 "SELECT column_name FROM information_schema.columns WHERE table_schema='quicksilver' AND table_name='lakes'" | tr '\n' ' '; echo
