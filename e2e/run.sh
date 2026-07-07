#!/bin/sh
# End-to-end: the compiled standalone server, a real redis, and curl as
# the SSE client. Asserts the health endpoint, event delivery to two
# concurrent SSE clients, correct SSE framing, 404s, and that a client
# disconnect neither kills the server nor stops delivery to the
# survivor.
#
# Usage: sh e2e/run.sh   (from the repo root; builds first)

PORT=4100
RPORT=16440
OUT=build/e2e
mkdir -p "$OUT"
rm -f "$OUT"/*.log "$OUT"/*.out

fail() { echo "FAIL $1"; sh e2e/teardown.sh $PORT $RPORT >/dev/null 2>&1; exit 1; }

lsof -nP -iTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1 && fail "port $PORT already in use (stale server?)"

PATH="$HOME/git/spinel/bin:$PATH" spin build >/dev/null || fail build

redis-server --port $RPORT --save '' --appendonly no --daemonize yes \
  --pidfile /tmp/mastodon-streaming-e2e-redis.pid --logfile "$PWD/$OUT/redis.log"
tries=0
until redis-cli -p $RPORT ping >/dev/null 2>&1; do
  tries=$((tries + 1)); [ $tries -gt 50 ] && fail "redis did not start"
  sleep 0.1
done

PORT=$PORT REDIS_HOST=127.0.0.1 REDIS_PORT=$RPORT ./build/bin/streaming > "$OUT/server.log" 2>&1 &
SERVER_PID=$!
tries=0
until curl -sf -m 2 "http://127.0.0.1:$PORT/api/v1/streaming/health" > "$OUT/health.out" 2>/dev/null; do
  tries=$((tries + 1)); [ $tries -gt 50 ] && fail "server did not start"
  sleep 0.1
done

[ "$(cat "$OUT/health.out")" = "OK" ] || fail "health body"
echo "ok   health"

code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/api/v1/streaming/nope")
[ "$code" = "404" ] || fail "unknown stream should 404 (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/elsewhere")
[ "$code" = "404" ] || fail "non-streaming path should 404 (got $code)"
echo "ok   404s"

# two concurrent SSE clients on the public stream
curl -sN "http://127.0.0.1:$PORT/api/v1/streaming/public" > "$OUT/sse1.out" 2>/dev/null &
CURL1=$!
curl -sN "http://127.0.0.1:$PORT/api/v1/streaming/public" > "$OUT/sse2.out" 2>/dev/null &
CURL2=$!
sleep 0.5

redis-cli -p $RPORT publish timeline:public \
  '{"event":"update","payload":{"id":"101","content":"first"},"queued_at":1718600000000}' >/dev/null
sleep 0.5

grep -q '^:)' "$OUT/sse1.out" || fail "client1 missing :) hello"
grep -q '^event: update$' "$OUT/sse1.out" || fail "client1 missing event line"
grep -q '^data: {"id":"101","content":"first"}$' "$OUT/sse1.out" || fail "client1 payload"
grep -q '^data: {"id":"101","content":"first"}$' "$OUT/sse2.out" || fail "client2 payload"
echo "ok   sse delivery x2"

# drop client1; the server must survive and keep delivering to client2
kill $CURL1 2>/dev/null; wait $CURL1 2>/dev/null
sleep 0.3
redis-cli -p $RPORT publish timeline:public '{"event":"delete","payload":"102"}' >/dev/null
sleep 0.5
kill -0 $SERVER_PID 2>/dev/null || fail "server died after client disconnect"
grep -q '^event: delete$' "$OUT/sse2.out" || fail "client2 missing delete after peer disconnect"
grep -q '^data: 102$' "$OUT/sse2.out" || fail "delete payload should be the bare id"
echo "ok   disconnect survival + bare delete id"

kill $CURL2 2>/dev/null; wait $CURL2 2>/dev/null
# TERM first (accept fiber exits <=1s; pump fibers at heartbeat cadence),
# then -9 as the bounded fallback so the harness never hangs on shutdown.
kill $SERVER_PID 2>/dev/null
sleep 1.2
kill -0 $SERVER_PID 2>/dev/null && kill -9 $SERVER_PID 2>/dev/null
redis-cli -p $RPORT shutdown nosave 2>/dev/null
echo "e2e: all checks passed"
