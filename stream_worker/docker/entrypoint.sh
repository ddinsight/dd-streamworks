#!/bin/bash
set -e

cd /opt/dd-streamworks/stream_worker

# if command starts with an option, prepend worker
if [ "$MASTER_WORKER" ]; then
    cli/ocadm upload -d devmodule/production appss -w 8
    cli/ocadm upload -d devmodule/production appsslog -w 4
    cli/ocadm upload -d devmodule/production networklog -w 8
    cli/ocadm upload -d devmodule/production tmkeeper -w 1
    cli/ocadm upload -d devmodule/production vidnet -w 4
    sleep 3
    cli/ocadm rebalance all
    exit
else
    python /opt/dd-streamworks/stream_worker/runworker.py
fi
