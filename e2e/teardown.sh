#!/bin/sh
# Best-effort cleanup for a failed e2e run: e2e/teardown.sh <port> <redis-port>
[ -n "$2" ] && redis-cli -p "$2" shutdown nosave 2>/dev/null
[ -n "$1" ] && pkill -f "build/bin/streaming" 2>/dev/null
exit 0
