# mastodon_streaming (spinel-mastodon-streaming)

A port of Mastodon's Node streaming server to spinel Ruby — one static
binary in place of the Node/express/ws/ioredis/pg process. Current
slice: health, public timelines, and **authenticated user streams**
over both SSE and the WebSocket client API, fed from Redis with OAuth
tokens resolved against PostgreSQL (spinel-pg) and `subscribed:*`
presence keys maintained with TTL refresh.

Library and executable in one spin package (the structural version of
Python's `if __name__ == "__main__"`):

- `require "mastodon_streaming"` — the subsystem. `dispatch(req, res)`
  routes streaming requests (returns false for paths that aren't ours);
  `boot(redis_host, redis_port)` wires the hub and parks its feed +
  presence fibers; `boot_db(host, port, db, user, pass)` wires the
  OAuth resolver (optional — without it user streams 401).
  The library never owns the process: no sockets, signals, forks, or
  scheduler loops at this layer — that discipline is what lets the
  single-process endgame (app + jobs + streaming in one spinel binary)
  mount it next to its own work.
- `bin/streaming.rb` — the standalone server: env parsing (PORT,
  REDIS_HOST, REDIS_PORT, DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS),
  `Main.dispatch` delegation, tep's scheduled server. `spin build` →
  `build/bin/streaming`.

## Architecture

```
Rails/Sidekiq ──publish──▶ Redis timeline:* channels
                              │ one subscription connection (spinel-redis)
                              ▼
                     StreamingHub feed fiber        (Tep::Scheduler.io_wait)
                              │ envelope split: event / raw payload
                              ▼
         Tep::Broadcast topics: "sse:<ch>" (chunk-pre-framed SSE bytes)
                                "ws:<ch>"  (double-encoded WS envelopes)
                              │ raw fd fan-out        │ WS TEXT-framed fan-out
                              ▼                       ▼
                   SSE client connections    WS client connections
```

The WS lane speaks Mastodon's client protocol: connect to
`/api/v1/streaming` (optionally `?stream=public`), drive it with
`{"type":"subscribe","stream":"public"}` frames, receive
`{"stream":["public"],"event":"update","payload":"{\"id\":...}"}` —
payload double-encoded as a JSON string, delete IDs bare, matching
Node. Unknown streams get an `{"error":"Unknown stream type"}` frame.
Per-connection keepalive pings ride their own fibers (term-flag aware).

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
delivery to the survivor, bare delete IDs — and, on the WS side
(driven by `e2e/ws_client.rb`, a stdlib-only RFC 6455 client that
shares no code with the server): frame-subscribe, the `?stream=` query
form, unsubscribe taking effect between events, error frames for
unknown streams, and a `kill -9`'d client not disturbing the rest.

Porting this lane flushed four char-vs-byte bugs out of tep's WS codec
(masked frames are arbitrary binary; char-count arithmetic flakes
mask-randomly) — fixed in roundhouse's vendored tep, which the Cable
demo also rides.

## Auth

Node's token semantics: sources are the `access_token` query param,
the `Sec-WebSocket-Protocol` header (the browser client smuggles the
token as the WS subprotocol; the 101 echoes it), and
`Authorization: Bearer` — in that precedence. Tokens resolve through
one query against `oauth_access_tokens`/`users` (revoked excluded);
a presented-but-invalid token is a 401 pre-upgrade, no token means an
anonymous connection limited to public streams (`user` subscribes get
an error frame). Tokens pass a strict urlsafe-base64 charset gate AND
single-quote escaping before touching SQL (simple-query protocol;
extended protocol is the planned fix). While a user timeline has
subscribers, `subscribed:timeline:<id>` is kept alive with a 60s TTL
refreshed at heartbeat cadence — the signal Rails' FeedManager reads.

## Ledger (current exclusions)

- **hashtag/list/direct streams, scope enforcement, filters** —
  mapping + policy work now that account context exists.
- **workers > 1** — single worker; per-worker hub boot needs a
  post-fork hook in tep's Scheduled server.
- **Prometheus metrics, X-Request-Id, CORS headers** — with the
  conformance harness (Mastodon's own streaming integration tests +
  record/replay against Node), which is also what will police exact
  header/format parity.
- **PG connection pooling / scheduler-parked DB reads** — one shared
  blocking connection (exec is fiber-atomic; queries are sub-ms
  localhost lookups). Pool + io_wait-parked transport when measured.
- Path deps on sibling checkouts (`../spinel-redis`, `../spinel-pg`,
  `../roundhouse/runtime/spinel` for tep).

## Spinel notes

- Params arriving through stored listener blocks are poly; byte-level
  code coerces at entry (`.to_s`, identity for Strings) — same idiom as
  spinel-redis's wire boundary.
- Vendored methods this graph never calls need load-time type-seeding
  (see mastodon_streaming.rb; tep.rb house idiom).
