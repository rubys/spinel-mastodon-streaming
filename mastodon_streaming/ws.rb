# The WS client API: one connection, multiplexed streams, driven by
# JSON frames — {"type":"subscribe","stream":"public"} and the
# unsubscribe twin — plus the legacy ?stream=public query form that
# subscribes on open. Outbound events are the double-encoded envelope
# built in MastodonWsEnvelope. Mirrors the Cable glue's handler-object
# shape (cable.rb is the house precedent).
#
# Lifecycle bookkeeping: Broadcast fd-subscriptions are auto-dropped by
# dispatch_close, but the hub's per-channel refcounts are ours to
# release — WsSession tracks what this connection acquired and the
# on_close handler releases it.
require_relative "envelope"
require_relative "hub"

class WsSession
  def initialize(driver, hub)
    @driver = driver
    @hub = hub
    @streams = [""]
    @streams.delete_at(0)      # type-seed StrArray, start empty
  end

  def driver
    @driver
  end

  def streams
    @streams
  end

  def subscribed?(stream)
    i = 0
    while i < @streams.length
      if @streams[i] == stream
        return true
      end
      i = i + 1
    end
    false
  end

  def subscribe(stream)
    s = stream.to_s
    if subscribed?(s)
      return 0                 # duplicate subscribe: no-op, like Node
    end
    chan = MastodonChannels.redis_channel_for_stream(s)
    if chan.bytesize == 0
      @driver.text("{\"error\":\"Unknown stream type\"}")
      return -1
    end
    Tep::Broadcast.subscribe_ws("ws:" + chan, @driver.fd)
    @hub.acquire(chan)
    @streams.push(s)
    0
  end

  def unsubscribe(stream)
    s = stream.to_s
    if !subscribed?(s)
      return 0
    end
    chan = MastodonChannels.redis_channel_for_stream(s)
    Tep::Broadcast.unsubscribe_topic_fd("ws:" + chan, @driver.fd)
    @hub.release(chan)
    i = @streams.length - 1
    while i >= 0
      if @streams[i] == s
        @streams.delete_at(i)
      end
      i = i - 1
    end
    0
  end

  # Connection gone: Broadcast already dropped this fd's subscriptions
  # (dispatch_close); release the hub refcounts we acquired.
  def release_all
    i = 0
    while i < @streams.length
      chan = MastodonChannels.redis_channel_for_stream(@streams[i])
      if chan.bytesize > 0
        @hub.release(chan)
      end
      i = i + 1
    end
    @streams.delete_at(0) while @streams.length > 0
    0
  end
end

module MastodonWs
  # on_open: honor the legacy ?stream= query form, then start the
  # keepalive ping fiber (Node pings its clients too).
  class WsOpen < Tep::WebSocket::Handler
    attr_accessor :session

    def initialize
      super
      @session = WsSession.new(Tep::WebSocket::Driver.new(0), MastodonStreaming::HUB)
    end

    def handle_event(evt)
      qs = ""
      if req.query.key?("stream")
        qs = req.query["stream"]
      end
      if qs.bytesize > 0
        @session.subscribe(qs)
      end
      MastodonWs.spawn_ping(@session.driver)
      0
    end
  end

  class WsMessage < Tep::WebSocket::Handler
    attr_accessor :session

    def initialize
      super
      @session = WsSession.new(Tep::WebSocket::Driver.new(0), MastodonStreaming::HUB)
    end

    def handle_event(evt)
      data = evt.data.to_s
      kind = Tep::Json.get_str(data, "type")
      stream = Tep::Json.get_str(data, "stream")
      if kind == "subscribe"
        @session.subscribe(stream)
      elsif kind == "unsubscribe"
        @session.unsubscribe(stream)
      end
      0
    end
  end

  class WsClose < Tep::WebSocket::Handler
    attr_accessor :session

    def initialize
      super
      @session = WsSession.new(Tep::WebSocket::Driver.new(0), MastodonStreaming::HUB)
    end

    def handle_event(evt)
      @session.release_all
      0
    end
  end

  # Keepalive pings at the hub's heartbeat cadence; exits on write
  # failure (fd closed) or shutdown (term-flag rule: every
  # connection-lifetime fiber checks it each wake).
  def self.spawn_ping(driver)
    Tep::Scheduler.spawn_fiber(Fiber.new { MastodonWs.ping_loop(driver) })
    0
  end

  def self.ping_loop(driver)
    while true
      Tep::Scheduler.pause(MastodonStreaming::HUB.heartbeat_seconds)
      if Sock.sp_net_shutdown_requested != 0
        return 0
      end
      r = driver.ping("")
      if r < 0
        return 0
      end
    end
    0
  end

  # Upgrade GET /api/v1/streaming to a WS connection. Cable.upgrade's
  # shape: validate, one driver, one session shared by all handlers,
  # flip res.start_websocket. Sec-WebSocket-Protocol is ignored for now
  # (Mastodon's browser client smuggles the access token there — that
  # lands with auth; ledgered).
  def self.upgrade(req, res)
    hs = Tep::WebSocket::Handshake.check(req)
    if !hs.valid
      res.status = 400
      res.body = "invalid websocket upgrade"
      return true
    end
    drv = Tep::WebSocket::Driver.new(0)
    session = WsSession.new(drv, MastodonStreaming::HUB)

    on_open = MastodonWs::WsOpen.new
    on_open.session = session
    on_open.req = req
    drv.set_on_open(on_open)

    on_msg = MastodonWs::WsMessage.new
    on_msg.session = session
    on_msg.req = req
    drv.set_on_message(on_msg)

    on_close = MastodonWs::WsClose.new
    on_close.session = session
    on_close.req = req
    drv.set_on_close(on_close)

    res.start_websocket(hs.accept_key, drv)
    true
  end
end
