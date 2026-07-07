# Minimal RFC 6455 client for the e2e harness — CRuby stdlib only.
# Doubles as an independent conformance probe: nothing here shares code
# with the server.
#
#   ruby e2e/ws_client.rb PORT PATH SEND_JSON READ_SECONDS [PROTOCOL]
#
# PROTOCOL (optional) is sent as Sec-WebSocket-Protocol — Mastodon's
# token-smuggling channel — and the 101 must echo it (exit 3 if not).
#
# Handshakes, then sends each "|"-separated part of SEND_JSON as a
# masked TEXT frame (first immediately, the rest 1s apart; "" sends
# nothing), printing every received TEXT frame (one per line) until
# READ_SECONDS elapses. Exits 2 on a failed handshake.
STDOUT.sync = true   # frames must survive a kill -9 (harness kills clients mid-window)
require "socket"
require "digest/sha1"
require "base64"

port = Integer(ARGV[0])
path = ARGV[1]
send_json = ARGV[2].to_s
read_seconds = Float(ARGV[3] || "1")
protocol = ARGV[4].to_s

sock = TCPSocket.new("127.0.0.1", port)
key = Base64.strict_encode64(Random.bytes(16))
req = "GET #{path} HTTP/1.1\r\n" \
      "Host: 127.0.0.1:#{port}\r\n" \
      "Upgrade: websocket\r\n" \
      "Connection: Upgrade\r\n" \
      "Sec-WebSocket-Key: #{key}\r\n"
req += "Sec-WebSocket-Protocol: #{protocol}\r\n" if protocol != ""
req += "Sec-WebSocket-Version: 13\r\n\r\n"
sock.write(req)

head = +""
head << sock.readpartial(4096) until head.include?("\r\n\r\n")
exit 2 unless head.start_with?("HTTP/1.1 101")
expected = Base64.strict_encode64(Digest::SHA1.digest(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
exit 2 unless head.include?(expected)
if protocol != ""
  exit 3 unless head.include?("Sec-WebSocket-Protocol: #{protocol}")
end

def send_text(sock, payload)
  mask = Random.bytes(4)
  masked = payload.bytes.each_with_index.map { |b, i| b ^ mask.getbyte(i % 4) }.pack("C*")
  len = payload.bytesize
  raise "long frames unsupported" if len > 125
  sock.write(([0x81, 0x80 | len].pack("C2")) + mask + masked)
end

parts = send_json == "" ? [] : send_json.split("|")
next_send = Time.now

buf = (head.split("\r\n\r\n", 2)[1] || "").b
deadline = Time.now + read_seconds
loop do
  while !parts.empty? && Time.now >= next_send
    send_text(sock, parts.shift)
    next_send = Time.now + 1.0
  end
  # decode complete frames out of buf
  while buf.bytesize >= 2
    b0 = buf.getbyte(0)
    b1 = buf.getbyte(1)
    len = b1 & 0x7f
    off = 2
    if len == 126
      break if buf.bytesize < 4
      len = buf.byteslice(2, 2).unpack1("n")
      off = 4
    elsif len == 127
      break if buf.bytesize < 10
      len = buf.byteslice(2, 8).unpack1("Q>")
      off = 10
    end
    break if buf.bytesize < off + len
    payload = buf.byteslice(off, len)
    buf = buf.byteslice(off + len, buf.bytesize - off - len)
    op = b0 & 0x0f
    puts payload if op == 1                 # TEXT
    # ping (0x9) / pong (0xA) / close (0x8): ignored for the harness
  end
  remain = deadline - Time.now
  break if remain <= 0
  if !parts.empty?
    till = next_send - Time.now
    remain = till if till >= 0 && till < remain
  end
  ready = IO.select([sock], nil, nil, remain > 0 ? remain : 0.01)
  next unless ready
  begin
    buf << sock.readpartial(4096)
  rescue EOFError, Errno::ECONNRESET
    break
  end
end
sock.close rescue nil
