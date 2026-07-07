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
    0
  end

  def pubsub
    @ps
  end

  def listener
    @listener
  end

  # One Redis message -> one chunk-framed SSE event on the broadcast
  # topic. Chunk framing: <hex len>CRLF <bytes> CRLF.
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
