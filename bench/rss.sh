#!/bin/sh
# RSS comparison: this repo's compiled streaming server vs Mastodon's
# Node streaming server, both serving N idle SSE connections against
# the same throwaway Redis + PostgreSQL. Reports baseline RSS, RSS at
# N connections, and the per-connection delta.
#
#   sh bench/rss.sh [N] [path-to-mastodon-checkout]
#
# Needs: redis-server, postgres toolchain, node, and the mastodon
# checkout's streaming deps installed (npm install in the workspace).

N=${1:-300}
MASTODON=${2:-$HOME/git/mastodon}
RPORT=16470
PGPORT=16471
PGDIR=/tmp/rss-pg
OUT=build/bench
mkdir -p "$OUT"

command -v postgres >/dev/null 2>&1 || PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
ulimit -n 4096

fail() { echo "FAIL $1"; cleanup; exit 1; }
cleanup() {
  [ -n "$HOLDER" ] && kill "$HOLDER" 2>/dev/null
  [ -n "$SRV" ] && kill -9 "$SRV" 2>/dev/null
  redis-cli -p $RPORT shutdown nosave 2>/dev/null
  pg_ctl -D $PGDIR stop -m immediate >/dev/null 2>&1
}

rss_kb() { ps -o rss= -p "$1" | tr -d ' '; }

# One measured run: $1 = label, remaining args = server command.
# Sets BASE_KB / LOADED_KB / PER_CONN_KB.
measure() {
  label=$1; shift
  "$@" > "$OUT/$label.log" 2>&1 &
  SRV=$!
  tries=0
  until curl -sf -m 2 "http://127.0.0.1:$BENCH_PORT/api/v1/streaming/health" >/dev/null 2>&1; do
    tries=$((tries + 1)); [ $tries -gt 100 ] && fail "$label did not start"
    sleep 0.1
  done
  sleep 1
  BASE_KB=$(rss_kb $SRV)
  ruby bench/holder.rb $BENCH_PORT $N "$BENCH_PATH" > "$OUT/$label.holder" 2>&1 &
  HOLDER=$!
  tries=0
  until grep -q "HELD $N" "$OUT/$label.holder" 2>/dev/null; do
    kill -0 $HOLDER 2>/dev/null || { cat "$OUT/$label.holder"; fail "$label holder died"; }
    tries=$((tries + 1)); [ $tries -gt 600 ] && fail "$label holder timeout"
    sleep 0.1
  done
  sleep 3
  LOADED_KB=$(rss_kb $SRV)
  kill -0 $SRV 2>/dev/null || fail "$label server died under load"
  PER_CONN_KB=$(( (LOADED_KB - BASE_KB) / N ))
  kill $HOLDER 2>/dev/null; wait $HOLDER 2>/dev/null; HOLDER=""
  kill -9 $SRV 2>/dev/null; wait $SRV 2>/dev/null; SRV=""
  sleep 0.5
}

# --- shared backends ---------------------------------------------------

redis-cli -p $RPORT shutdown nosave 2>/dev/null
redis-server --port $RPORT --save '' --appendonly no --daemonize yes \
  --pidfile /tmp/rss-redis.pid --logfile "$PWD/$OUT/redis.log" || fail redis
pg_ctl -D $PGDIR stop -m immediate >/dev/null 2>&1
rm -rf $PGDIR
initdb -D $PGDIR -U bench --auth=trust -N >/dev/null 2>&1 || fail initdb
pg_ctl -D $PGDIR -o "-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGDIR" \
  -l $PGDIR/log start >/dev/null 2>&1 || fail "pg start"

# Node-shaped auth schema: current Mastodon requires a token for ALL
# streams (public included), and its lookup joins devices + reads
# scopes/chosen_languages. Both servers get the same tokened path.
psql -h 127.0.0.1 -p $PGPORT -U bench -d postgres -q <<'SQL' || fail "pg seed"
CREATE TABLE oauth_access_tokens (id bigint, token text, resource_owner_id bigint, scopes text, revoked_at timestamptz);
CREATE TABLE users (id bigint, account_id bigint, chosen_languages text[], disabled boolean DEFAULT FALSE, role_id bigint);
CREATE TABLE accounts (id bigint, suspended_at timestamptz);
CREATE TABLE user_roles (id bigint, permissions bigint);
CREATE TABLE devices (access_token_id bigint, device_id bigint);
CREATE TABLE settings (var text, value text);
INSERT INTO users VALUES (7, 42, NULL, FALSE, NULL);
INSERT INTO accounts VALUES (42, NULL);
INSERT INTO oauth_access_tokens VALUES (1, 'benchtok_1', 7, 'read', NULL);
SQL
BENCH_PATH="/api/v1/streaming/public?access_token=benchtok_1"

PATH="$HOME/git/spinel/bin:$PATH" spin build >/dev/null || fail build

# --- spinel ------------------------------------------------------------

BENCH_PORT=4210
measure spinel env PORT=$BENCH_PORT REDIS_HOST=127.0.0.1 REDIS_PORT=$RPORT \
  DB_HOST=127.0.0.1 DB_PORT=$PGPORT DB_NAME=postgres DB_USER=bench \
  ./build/bin/streaming
SP_BASE=$BASE_KB; SP_LOADED=$LOADED_KB; SP_PER=$PER_CONN_KB

# --- node --------------------------------------------------------------

[ -f "$MASTODON/streaming/index.js" ] || fail "no mastodon checkout at $MASTODON"
BENCH_PORT=4211
measure node env NODE_ENV=production LOG_LEVEL=error PORT=$BENCH_PORT \
  DATABASE_URL=postgres://bench@127.0.0.1:$PGPORT/postgres \
  REDIS_URL=redis://127.0.0.1:$RPORT \
  node "$MASTODON/streaming/index.js"
NODE_BASE=$BASE_KB; NODE_LOADED=$LOADED_KB; NODE_PER=$PER_CONN_KB

cleanup

SP_BIN_KB=$(du -k build/bin/streaming | cut -f1)

echo ""
echo "RSS at $N idle SSE connections (macOS ps RSS, KB)"
echo "server  baseline  at-$N  per-conn"
echo "spinel  $SP_BASE  $SP_LOADED  $SP_PER"
echo "node    $NODE_BASE  $NODE_LOADED  $NODE_PER"
echo ""
echo "spinel binary size: ${SP_BIN_KB} KB (self-contained; node runtime + node_modules excluded from its own figure)"
