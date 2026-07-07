# SSE connection pump. Runs inside the connection's fiber via tep's
# streaming branch (res.start_stream): tep writes the chunked head,
# then calls pump(out). We write Mastodon's ":)" hello, subscribe the
# client fd to the hub's broadcast topic (fan-out happens off-fiber in
# the hub's feed fiber), and then just heartbeat until the client goes
# away.
#
# Close detection: Stream#write can't report failure (always 0), but
# poll marks a hung-up fd readable and an SSE client never sends bytes
# after its request — so readable + empty recv == EOF == client gone.
# Heartbeats double as the liveness cadence, matching Node's ":thump".
require_relative "hub"

class SseStreamer < Tep::Streamer
  def initialize(hub, channel)
    @hub = hub
    @channel = channel
  end

  def pump(out)
    out.write(":)\n")
    topic = "sse:" + @channel
    Tep::Broadcast.subscribe(topic, out.fd)
    @hub.acquire(@channel)
    while true
      Tep::Scheduler.pause(@hub.heartbeat_seconds)
      # Long-lived connection fibers must be term-aware: after SIGTERM
      # the sp_net term flag makes poll_run return -1, so io_wait can
      # never report EOF again — an unchecked pump would heartbeat
      # forever and wedge the worker with the port held. The accept
      # fiber does the same check (server_scheduled.rb).
      if Sock.sp_net_shutdown_requested != 0
        break
      end
      if SseStreamer.client_closed(out.fd)
        break
      end
      out.write(":thump\n")
    end
    Tep::Broadcast.unsubscribe_fd(out.fd)
    @hub.release(@channel)
    0
  end

  # EOF probe: poll(0) — POLLHUP/POLLERR surface as the readable bit —
  # then a recv that returns "" confirms the peer closed. Data from the
  # peer (pipelining junk) is drained and ignored.
  def self.client_closed(fd)
    ready = Tep::Scheduler.io_wait(fd, Tep::Scheduler::READ, 0)
    if ready == 0
      return false
    end
    data = Sock.sp_net_recv_some(fd, 512)
    data.bytesize == 0
  end
end
