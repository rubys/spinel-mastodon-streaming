# Idle-connection holder for the RSS benchmark: opens N SSE connections
# to the streaming endpoint, verifies each one's ":)" hello (so every
# counted connection is a real, served stream), prints "HELD <n>", then
# sits in a read-and-discard loop (heartbeats must be drained so TCP
# backpressure doesn't distort the server side). CRuby stdlib only.
#
#   ruby bench/holder.rb PORT COUNT [PATH]
STDOUT.sync = true
require "socket"

port = Integer(ARGV[0])
count = Integer(ARGV[1])
path = (ARGV[2].to_s == "" ? "/api/v1/streaming/public" : ARGV[2])

socks = []
count.times do |i|
  s = TCPSocket.new("127.0.0.1", port)
  s.write("GET #{path} HTTP/1.1\r\nHost: 127.0.0.1:#{port}\r\nAccept: text/event-stream\r\n\r\n")
  socks << s
rescue => e
  warn "holder: open ##{i} failed: #{e.message}"
  exit 1
end

# Wait until every connection has produced the SSE hello.
pending = socks.dup
bufs = Hash.new { |h, k| h[k] = +"" }
deadline = Time.now + 30
until pending.empty?
  raise "holder: #{pending.size} connections never got the hello" if Time.now > deadline
  ready = IO.select(pending, nil, nil, 1)
  next unless ready
  ready[0].each do |s|
    bufs[s] << s.readpartial(65536)
    pending.delete(s) if bufs[s].include?(":)")
  rescue EOFError, Errno::ECONNRESET
    raise "holder: connection dropped during hello"
  end
end
puts "HELD #{socks.size}"

# Hold: drain heartbeats forever; parent kills us when done.
loop do
  ready = IO.select(socks, nil, nil, 60)
  next unless ready
  ready[0].each do |s|
    s.readpartial(65536)
  rescue EOFError, Errno::ECONNRESET
    warn "holder: a held connection dropped"
    exit 2
  end
end
