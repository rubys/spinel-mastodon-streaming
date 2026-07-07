# mastodon_streaming (spinel-mastodon-streaming)

A port of Mastodon's Node streaming server to spinel Ruby — one static
binary in place of the Node/express/ws/ioredis process. Slice v0:
health endpoint + public SSE timelines fed from Redis.

Library and executable in one spin package (the structural version of
Python's `if __name__ == "__main__"`):

- `require "mastodon_streaming"` — the subsystem. `dispatch(req, res)`
  routes streaming requests (returns false for paths that aren't ours);
  `boot(redis_host, redis_port)` wires the hub and parks its feed fiber.
  The library never owns the process: no sockets, signals, forks, or
  scheduler loops at this layer — that discipline is what lets the
  single-process endgame (app + jobs + streaming in one spinel binary)
  mount it next to its own work.
- `bin/streaming.rb` — the standalone server: env parsing (PORT,
  REDIS_HOST, REDIS_PORT), `Main.dispatch` delegation, tep's scheduled
  server. `spin build` → `build/bin/streaming`.

## Architecture

```
Rails/Sidekiq ──publish──▶ Redis timeline:* channels
                              │ one subscription connection (spinel-redis)
                              ▼
                     StreamingHub feed fiber        (Tep::Scheduler.io_wait)
                              │ envelope split: event / raw payload
                              ▼
              Tep::Broadcast "sse:<channel>" topics (chunk-pre-framed SSE bytes)
                              │ raw fd fan-out
                              ▼
                   SSE client connections           (tep chunked streaming)
```

Channel subscriptions are refcounted on demand (first client in →
SUBSCRIBE, last out → UNSUBSCRIBE), as in the Node server. Client
disconnects are detected by EOF probe at heartbeat cadence (`:thump`,
matching Node); a dead client never kills the process (sp_net ignores
SIGPIPE) and never leaks its broadcast subscriptions.

The envelope scanner extracts `event` and the raw `payload` extent
byte-exactly without decoding JSON — object payloads re-emit verbatim,
string payloads (delete IDs) emit bare, matching Node.

## Tests

```sh
spin test        # envelope/channel parity lane (also runs under CRuby)
sh e2e/run.sh    # builds, boots redis + the binary, drives it with curl
```

e2e asserts: health body, 404s, event delivery to two concurrent SSE
clients, `:)` hello + SSE framing, disconnect survival with continued
delivery to the survivor, and bare delete IDs.

## Ledger (slice v0 exclusions)

- **Auth / PG** — no OAuth token check; public timelines only. Arrives
  with the spinel-pg client (SCRAM = sp_crypto's existing trio).
- **WebSocket API** — SSE only. tep's WS stack (handshake/driver/
  Connection) is already in the graph; the WS envelope + per-connection
  stream multiplexing is the next slice.
- **User/hashtag/list/direct streams, presence keys** (`subscribed:*`
  TTLs), **filters** — with auth.
- **workers > 1** — single worker; per-worker hub boot needs a
  post-fork hook in tep's Scheduled server.
- **Prometheus metrics, X-Request-Id, CORS headers** — with the
  conformance harness (Mastodon's own streaming integration tests +
  record/replay against Node), which is also what will police exact
  header/format parity.
- Path deps on sibling checkouts (`../spinel-redis`,
  `../roundhouse/runtime/spinel` for tep) until tep publishes as a spin
  package.

## Spinel notes

- Params arriving through stored listener blocks are poly; byte-level
  code coerces at entry (`.to_s`, identity for Strings) — same idiom as
  spinel-redis's wire boundary.
- Vendored methods this graph never calls need load-time type-seeding
  (see mastodon_streaming.rb; tep.rb house idiom).
