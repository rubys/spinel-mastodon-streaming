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
PGPORT=16460
PGDIR=/tmp/mastodon-streaming-e2e-pg
# key on the SERVER binary: libpq ships initdb/psql without postgres,
# and its bin dir may shadow the real toolchain on PATH
command -v postgres >/dev/null 2>&1 || PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
OUT=build/e2e
mkdir -p "$OUT"
rm -f "$OUT"/*.log "$OUT"/*.out

fail() { echo "FAIL $1"; sh e2e/teardown.sh $PORT $RPORT >/dev/null 2>&1; pg_ctl -D $PGDIR stop -m immediate >/dev/null 2>&1; exit 1; }

lsof -nP -iTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1 && fail "port $PORT already in use (stale server?)"

PATH="$HOME/git/spinel/bin:$PATH" spin build >/dev/null || fail build

redis-server --port $RPORT --save '' --appendonly no --daemonize yes \
  --pidfile /tmp/mastodon-streaming-e2e-redis.pid --logfile "$PWD/$OUT/redis.log"
tries=0
until redis-cli -p $RPORT ping >/dev/null 2>&1; do
  tries=$((tries + 1)); [ $tries -gt 50 ] && fail "redis did not start"
  sleep 0.1
done

# ephemeral PostgreSQL with a minimal doorkeeper-shaped schema
pg_ctl -D $PGDIR stop -m immediate >/dev/null 2>&1
rm -rf $PGDIR
initdb -D $PGDIR -U spinel_e2e --auth=trust -N >/dev/null 2>&1 || fail "initdb"
pg_ctl -D $PGDIR -o "-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGDIR" -l $PGDIR/log start >/dev/null 2>&1 || fail "pg start"
psql -h 127.0.0.1 -p $PGPORT -U spinel_e2e -d postgres -q <<'SQL' || fail "pg seed"
CREATE TABLE oauth_access_tokens (token text, resource_owner_id bigint, revoked_at timestamptz);
CREATE TABLE users (id bigint, account_id bigint);
INSERT INTO users VALUES (7, 42);
INSERT INTO oauth_access_tokens VALUES ('goodtok_123', 7, NULL);
INSERT INTO oauth_access_tokens VALUES ('revoked_tok', 7, now());
SQL

PORT=$PORT REDIS_HOST=127.0.0.1 REDIS_PORT=$RPORT DB_HOST=127.0.0.1 DB_PORT=$PGPORT DB_NAME=postgres DB_USER=spinel_e2e ./build/bin/streaming > "$OUT/server.log" 2>&1 &
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

# --- WebSocket API -----------------------------------------------------

# A: subscribe frame, full window. B: legacy ?stream= form, killed -9
# mid-stream. C: unknown stream -> error frame, no events. D: subscribe
# then unsubscribe 1s later -> gets the first event only.
ruby e2e/ws_client.rb $PORT /api/v1/streaming '{"type":"subscribe","stream":"public"}' 3 > "$OUT/ws_a.out" 2>/dev/null &
WSA=$!
ruby e2e/ws_client.rb $PORT '/api/v1/streaming?stream=public' '' 3 > "$OUT/ws_b.out" 2>/dev/null &
WSB=$!
ruby e2e/ws_client.rb $PORT /api/v1/streaming '{"type":"subscribe","stream":"nope"}' 3 > "$OUT/ws_c.out" 2>/dev/null &
WSC=$!
ruby e2e/ws_client.rb $PORT /api/v1/streaming '{"type":"subscribe","stream":"public"}|{"type":"unsubscribe","stream":"public"}' 3 > "$OUT/ws_d.out" 2>/dev/null &
WSD=$!
sleep 0.6
redis-cli -p $RPORT publish timeline:public '{"event":"update","payload":{"id":"201","content":"ws"},"queued_at":1}' >/dev/null
sleep 0.6
kill -9 $WSB 2>/dev/null; wait $WSB 2>/dev/null
sleep 0.6
redis-cli -p $RPORT publish timeline:public '{"event":"delete","payload":"202"}' >/dev/null
wait $WSA 2>/dev/null; wait $WSC 2>/dev/null; wait $WSD 2>/dev/null

UPD='{"stream":["public"],"event":"update","payload":"{\"id\":\"201\",\"content\":\"ws\"}"}'
DEL='{"stream":["public"],"event":"delete","payload":"202"}'
grep -qF "$UPD" "$OUT/ws_a.out" || fail "ws A missing double-encoded update"
grep -qF "$DEL" "$OUT/ws_a.out" || fail "ws A missing delete after peer was killed"
grep -qF "$UPD" "$OUT/ws_b.out" || fail "ws B (query form) missing update"
grep -qF '{"error":"Unknown stream type"}' "$OUT/ws_c.out" || fail "ws C missing error frame"
grep -qF "$UPD" "$OUT/ws_c.out" && fail "ws C received events for unknown stream"
grep -qF "$UPD" "$OUT/ws_d.out" || fail "ws D missing pre-unsubscribe update"
grep -qF "$DEL" "$OUT/ws_d.out" && fail "ws D received event after unsubscribe"
kill -0 $SERVER_PID 2>/dev/null || fail "server died during ws phase"
echo "ok   ws subscribe/query/unsubscribe/error + kill survival"

# --- authenticated user streams (SSE + WS) ------------------------------

code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/api/v1/streaming/user")
[ "$code" = "401" ] || fail "user without token should 401 (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/api/v1/streaming/user?access_token=nosuchtok")
[ "$code" = "401" ] || fail "bad token should 401 (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/api/v1/streaming/user?access_token=revoked_tok")
[ "$code" = "401" ] || fail "revoked token should 401 (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/api/v1/streaming/user?access_token=bad'quote")
[ "$code" = "401" ] || fail "malformed token should 401 (got $code)"
echo "ok   user 401 matrix"

curl -sN "http://127.0.0.1:$PORT/api/v1/streaming/user?access_token=goodtok_123" > "$OUT/user1.out" 2>/dev/null &
UCURL1=$!
curl -sN -H "Authorization: Bearer goodtok_123" "http://127.0.0.1:$PORT/api/v1/streaming/user" > "$OUT/user2.out" 2>/dev/null &
UCURL2=$!
ruby e2e/ws_client.rb $PORT '/api/v1/streaming?stream=user&access_token=goodtok_123' '' 2.4 > "$OUT/ws_u1.out" 2>/dev/null &
WSU1=$!
ruby e2e/ws_client.rb $PORT '/api/v1/streaming?stream=user' '' 2.4 goodtok_123 > "$OUT/ws_u2.out" 2>/dev/null &
WSU2=$!
ruby e2e/ws_client.rb $PORT /api/v1/streaming '{"type":"subscribe","stream":"user"}' 2.4 > "$OUT/ws_u3.out" 2>/dev/null &
WSU3=$!
sleep 0.8

pres=$(redis-cli -p $RPORT get subscribed:timeline:42)
[ "$pres" = "1" ] || fail "presence key missing (got '$pres')"
ttl=$(redis-cli -p $RPORT ttl subscribed:timeline:42)
[ "$ttl" -gt 0 ] || fail "presence key has no TTL (got $ttl)"
echo "ok   presence key + ttl"

redis-cli -p $RPORT publish timeline:42 '{"event":"notification","payload":{"id":"n9"}}' >/dev/null
wait $UCURL1 2>/dev/null; kill $UCURL1 2>/dev/null
wait $WSU1 2>/dev/null; wait $WSU2 2>/dev/null; wait $WSU3 2>/dev/null
sleep 0.4
kill $UCURL2 2>/dev/null; wait $UCURL2 2>/dev/null

grep -q '^event: notification$' "$OUT/user1.out" || fail "user SSE (query token) missing event"
grep -q '^data: {"id":"n9"}$' "$OUT/user1.out" || fail "user SSE payload"
grep -q '^event: notification$' "$OUT/user2.out" || fail "user SSE (bearer) missing event"
UENV='{"stream":["user"],"event":"notification","payload":"{\"id\":\"n9\"}"}'
grep -qF "$UENV" "$OUT/ws_u1.out" || fail "ws user (query token) missing envelope"
grep -qF "$UENV" "$OUT/ws_u2.out" || fail "ws user (subprotocol token) missing envelope"
grep -qF '{"error":"Unauthorized"}' "$OUT/ws_u3.out" || fail "anonymous user subscribe should get Unauthorized"
grep -qF "$UENV" "$OUT/ws_u3.out" && fail "anonymous ws received user events"
kill -0 $SERVER_PID 2>/dev/null || fail "server died during auth phase"
echo "ok   user streams: sse query+bearer, ws query+subprotocol, anon rejected"

# TERM first (accept fiber exits <=1s; pump fibers at heartbeat cadence),
# then -9 as the bounded fallback so the harness never hangs on shutdown.
kill $SERVER_PID 2>/dev/null
sleep 1.2
kill -0 $SERVER_PID 2>/dev/null && kill -9 $SERVER_PID 2>/dev/null
redis-cli -p $RPORT shutdown nosave 2>/dev/null
pg_ctl -D $PGDIR stop -m immediate >/dev/null 2>&1
rm -rf $PGDIR
echo "e2e: all checks passed"
