# StreamingHub — one Redis subscription connection per process, fanned
# out to local client fds through Tep::Broadcast.
#
# Message path: Redis push -> feed fiber drains -> envelope split ->
# SSE bytes, pre-framed as ONE chunked-encoding chunk -> published to
# the "sse:<channel>" broadcast topic -> raw-mode write to every
# subscribed client fd. Pre-framing is what lets Broadcast's raw write
# land inside each client's chunked response without corrupting it.
#
# Channel subscriptions are refcounted: first client on a channel sends
# SUBSCRIBE, last one out sends UNSUBSCRIBE — the on-demand model the
# Node streaming server uses.
require "redis"
require_relative "envelope"

# Transport stand-in so the hub can exist (typed, monomorphic) before
# connect() supplies the real Redis connection. Also handy in tests.
class NullRedisTransport
  def fd
    -1
  end

  def write(data)
    data.bytesize
  end

  def read_some(max)
    ""
  end

  def close
    0
  end
end

class StreamingHub
  def initialize
    @ps = RedisPubSub.new(NullRedisTransport.new)
    @presence = RedisClientCore.new(NullRedisTransport.new)
    @presence_on = false
    @counts = {}
    @heartbeat_seconds = 15
    @listener = RedisListener.new
    hub = self
    @listener.message do |channel, msg|
      hub.fan_out(channel, msg)
    end
  end

  def heartbeat_seconds
    @heartbeat_seconds
  end

  def set_heartbeat(seconds)
    @heartbeat_seconds = seconds
  end

  def connect(host, port)
    @ps = RedisPubSub.new(RedisTransport.new(host, port))
    # Second, command-mode connection: the subscription connection can
    # only speak (un)subscribe, and presence keys need SETEX.
    @presence = RedisClientCore.new(RedisTransport.new(host, port))
    @presence_on = true
    0
  end

  # subscribed:timeline:<id> markers with TTL, the signal Rails'
  # FeedManager reads to decide whether to push into a user timeline.
  # Set on first subscriber, refreshed at heartbeat cadence, left to
  # lapse by TTL after the last unsubscribe (Node behavior).
  def mark_presence(channel)
    if @presence_on
      if MastodonChannels.user_channel?(channel)
        @presence.setex("subscribed:" + channel, 60, "1")
      end
    end
    0
  end

  def refresh_presence
    @counts.each do |channel, n|
      if n > 0
        mark_presence(channel)
      end
    end
    0
  end

  def presence_loop
    while true
      Tep::Scheduler.pause(@heartbeat_seconds)
      if Sock.sp_net_shutdown_requested != 0
        return 0
      end
      refresh_presence
    end
    0
  end

  def spawn_presence
    hub = self
    f = Fiber.new { hub.presence_loop }
    Tep::Scheduler.spawn_fiber(f)
  end

  def pubsub
    @ps
  end

  def listener
    @listener
  end

  # One Redis message -> both client lanes:
  #   sse:<channel> — SSE bytes pre-framed as one chunked-encoding
  #     chunk (raw-mode subscribers inside chunked responses);
  #   ws:<channel>  — the WS client envelope (payload double-encoded,
  #     Node parity); subscribers registered via subscribe_ws, so
  #     Broadcast applies the TEXT framing.
  # .to_s at entry: these arrive through stored listener blocks, whose
  # params come in poly — the byte-level scanner needs real Strings
  # (String#to_s is identity, so it costs nothing when already typed).
  def fan_out(channel, msg)
    ch = channel.to_s
    m = msg.to_s
    ev = MastodonEnvelope.event_of(m)
    pl = MastodonEnvelope.payload_of(m)
    sse = "event: " + ev + "\n" + "data: " + pl + "\n\n"
    framed = sse.bytesize.to_s(16) + "\r\n" + sse + "\r\n"
    Tep::Broadcast.publish("sse:" + ch, framed)
    stream = MastodonChannels.stream_for_channel(ch)
    if stream.bytesize > 0
      Tep::Broadcast.publish("ws:" + ch, MastodonWsEnvelope.build(stream, ev, pl))
    end
  end

  # First client in subscribes the Redis channel. Returns the refcount.
  def acquire(channel)
    c = 0
    if @counts.key?(channel)
      c = @counts[channel]
    end
    @counts[channel] = c + 1
    if c == 0
      @ps.subscribe_start(channel)
      mark_presence(channel)
    end
    c + 1
  end

  # Last client out unsubscribes. Returns the remaining refcount.
  def release(channel)
    c = 0
    if @counts.key?(channel)
      c = @counts[channel]
    end
    if c <= 1
      @counts[channel] = 0
      if c == 1
        @ps.unsubscribe(channel)
      end
      return 0
    end
    @counts[channel] = c - 1
    c - 1
  end

  def refcount(channel)
    if @counts.key?(channel)
      return @counts[channel]
    end
    0
  end

  # Park-drain loop on the subscription fd; one fiber per process.
  # Same shape as Tep::RedisFeed#run_loop.
  def feed_loop
    while true
      ready = Tep::Scheduler.io_wait(@ps.fd, Tep::Scheduler::READ, 5)
      if ready > 0
        @ps.drain(@listener)
      end
    end
  end

  def spawn_feed
    hub = self
    f = Fiber.new { hub.feed_loop }
    Tep::Scheduler.spawn_fiber(f)
  end
end
