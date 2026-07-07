# Byte scanner for the Mastodon streaming envelope, the JSON that Rails
# and Sidekiq publish to timeline:* Redis channels:
#
#   {"event":"update","payload":{...},"queued_at":1718600000000}
#   {"event":"delete","payload":"1234567890"}
#
# The streaming server re-emits `event` and `payload` (SSE data: lines,
# WS envelopes) without needing the values decoded — so this extracts
# the raw byte extent of a top-level key's value instead of parsing the
# document. Top-level walk only: a "payload" key nested inside the
# payload object can't confuse it. Byte-oriented (getbyte/byteslice),
# same discipline as the RESP parser.
#
# Dependency-free on purpose: parity tests run it under CRuby.
module MastodonEnvelope
  # The `event` value (a bare token like "update"); "" when absent.
  def self.event_of(msg)
    raw = value_raw(msg, "event")
    if raw.bytesize >= 2
      if raw.getbyte(0) == 34
        return raw.byteslice(1, raw.bytesize - 2)
      end
    end
    raw
  end

  # The raw `payload` value. Objects/arrays/numbers come back verbatim
  # (ready for a data: line); strings come back WITHOUT quotes — Node's
  # server emits delete IDs bare, and this mirrors that.
  def self.payload_of(msg)
    raw = value_raw(msg, "payload")
    if raw.bytesize >= 2
      if raw.getbyte(0) == 34
        return raw.byteslice(1, raw.bytesize - 2)
      end
    end
    raw
  end

  # Raw byte extent of `key`'s value at the top level; "" when absent
  # or when msg isn't an object.
  def self.value_raw(msg, key)
    n = msg.bytesize
    i = skip_ws(msg, 0)
    if i >= n
      return ""
    end
    if msg.getbyte(i) != 123            # '{'
      return ""
    end
    i = skip_ws(msg, i + 1)
    while i < n
      if msg.getbyte(i) == 125          # '}' — end of object
        return ""
      end
      if msg.getbyte(i) != 34           # keys are strings
        return ""
      end
      key_end = string_end(msg, i)
      if key_end < 0
        return ""
      end
      k = msg.byteslice(i + 1, key_end - i - 1)
      i = skip_ws(msg, key_end + 1)
      if i >= n
        return ""
      end
      if msg.getbyte(i) != 58           # ':'
        return ""
      end
      i = skip_ws(msg, i + 1)
      v_end = value_end(msg, i)
      if v_end < 0
        return ""
      end
      if k == key
        return msg.byteslice(i, v_end - i + 1)
      end
      i = skip_ws(msg, v_end + 1)
      if i < n
        if msg.getbyte(i) == 44         # ','
          i = skip_ws(msg, i + 1)
        end
      end
    end
    ""
  end

  def self.skip_ws(msg, i)
    n = msg.bytesize
    while i < n
      b = msg.getbyte(i)
      if b == 32 || b == 9 || b == 10 || b == 13
        i = i + 1
      else
        return i
      end
    end
    i
  end

  # msg[i] is an opening quote; index of the closing quote, honoring
  # backslash escapes. -1 if unterminated.
  def self.string_end(msg, i)
    n = msg.bytesize
    j = i + 1
    while j < n
      b = msg.getbyte(j)
      if b == 92                        # backslash: skip escaped byte
        j = j + 2
      elsif b == 34
        return j
      else
        j = j + 1
      end
    end
    -1
  end

  # Last index of the value starting at msg[i]: quoted string, brace/
  # bracket-balanced object/array, or a bare literal running to the
  # next top-level ',' / '}'. -1 on malformed input.
  def self.value_end(msg, i)
    n = msg.bytesize
    if i >= n
      return -1
    end
    b = msg.getbyte(i)
    if b == 34
      return string_end(msg, i)
    end
    if b == 123 || b == 91              # '{' or '['
      depth = 0
      j = i
      while j < n
        c = msg.getbyte(j)
        if c == 34
          j = string_end(msg, j)
          if j < 0
            return -1
          end
        elsif c == 123 || c == 91
          depth = depth + 1
        elsif c == 125 || c == 93       # '}' or ']'
          depth = depth - 1
          if depth == 0
            return j
          end
        end
        j = j + 1
      end
      return -1
    end
    # bare literal: number / true / false / null
    j = i
    while j < n
      c = msg.getbyte(j)
      if c == 44 || c == 125            # ',' or '}'
        return j - 1
      end
      j = j + 1
    end
    -1
  end
end

# Mastodon stream-path -> Redis channel mapping. Slice v0: the public
# timelines. The full vocabulary (user:, hashtag:, list:, direct)
# arrives with auth (needs PG) — ledgered in the README.
module MastodonChannels
  def self.redis_channel_for_path(path)
    if path == "/api/v1/streaming/public"
      return "timeline:public"
    end
    if path == "/api/v1/streaming/public/local"
      return "timeline:public:local"
    end
    ""
  end
end
