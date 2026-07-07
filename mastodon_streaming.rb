# mastodon_streaming — a port of Mastodon's Node streaming server to
# spinel Ruby, as a library. The standalone server is bin/streaming.rb
# (a thin main); embedders require this and route requests through
# MastodonStreaming.dispatch — the library never owns the process (no
# sockets, signals, forks, or scheduler loops at this layer).
#
# Slice v0 surface: /health, and the public SSE timelines fed from
# Redis. The exclusion ledger (auth/PG, WS, user streams, presence) is
# in the README.
require "redis"
require "tep"
require_relative "mastodon_streaming/envelope"
require_relative "mastodon_streaming/auth"
require_relative "mastodon_streaming/hub"
require_relative "mastodon_streaming/sse"
require_relative "mastodon_streaming/ws"

module MastodonStreaming
  HUB = StreamingHub.new
  AUTH = StreamingAuth.new
  PREFIX = "/api/v1/streaming"

  # Type-seeding (tep.rb house idiom): pin param types on vendored
  # methods this graph doesn't otherwise call — an un-called method's
  # params default in ways that can fail the C compile. Both calls are
  # no-ops (-1 guards / empty registry).
  Tep::Broadcast.unsubscribe(-1)
  Tep::Broadcast.unsubscribe_fd(-1)

  # Wire the hub to Redis and park its feed + presence fibers on the
  # scheduler. Call once per process, after any fork, before the server
  # loop.
  def self.boot(redis_host, redis_port)
    HUB.connect(redis_host, redis_port)
    HUB.spawn_feed
    HUB.spawn_presence
    0
  end

  # Wire the auth resolver to PostgreSQL. Optional: without it, every
  # token resolves to "" and user streams 401.
  def self.boot_db(host, port, dbname, user, password)
    AUTH.connect(host, port, dbname, user, password)
    0
  end

  def self.header_of(req, name)
    if req.req_headers.key?(name)
      return req.req_headers[name]
    end
    ""
  end

  def self.token_of(req)
    q = ""
    if req.query.key?("access_token")
      q = req.query["access_token"]
    end
    MastodonAuthToken.pick(q,
                           header_of(req, "sec-websocket-protocol"),
                           header_of(req, "authorization"))
  end

  # Route one request. Returns true when this request was ours (res is
  # filled in), false when the embedder should handle it. Standalone
  # mode 404s the false case in bin/streaming.rb.
  def self.dispatch(req, res)
    p = req.path
    if p.bytesize < PREFIX.bytesize
      return false
    end
    if p.byteslice(0, PREFIX.bytesize) != PREFIX
      return false
    end
    if p == PREFIX + "/health"
      res.status = 200
      res.headers["Content-Type"] = "text/plain"
      res.body = "OK"
      return true
    end
    # The WS client API lives at the prefix root (multiplexed streams
    # via subscribe/unsubscribe frames, or the legacy ?stream= form).
    if p == PREFIX
      return MastodonWs.upgrade(req, res)
    end
    if p == PREFIX + "/"
      return MastodonWs.upgrade(req, res)
    end
    if p == PREFIX + "/user"
      token = token_of(req)
      account = ""
      if token.bytesize > 0
        account = AUTH.resolve(token)
      end
      if account.bytesize == 0
        res.status = 401
        res.headers["Content-Type"] = "application/json"
        res.body = "{\"error\":\"Invalid access token\"}"
        return true
      end
      res.status = 200
      res.headers["Cache-Control"] = "no-store"
      res.headers["X-Accel-Buffering"] = "no"
      res.start_stream(SseStreamer.new(HUB, "timeline:" + account))
      return true
    end
    channel = MastodonChannels.redis_channel_for_path(p)
    if channel.bytesize > 0
      res.status = 200
      res.headers["Cache-Control"] = "no-store"
      res.headers["X-Accel-Buffering"] = "no"
      res.start_stream(SseStreamer.new(HUB, channel))
      return true
    end
    res.status = 404
    res.headers["Content-Type"] = "application/json"
    res.body = "{\"error\":\"Unknown stream type\"}"
    true
  end
end
