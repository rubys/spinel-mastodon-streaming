# Envelope scanner + channel map conformance. Dual-runtime (no
# snapshot): pure byte scanning, no ffi in the graph.
require "mastodon_streaming/envelope"

# object payload, queued_at trailing (the common update shape)
m = "{\"event\":\"update\",\"payload\":{\"id\":\"1\",\"content\":\"hi\"},\"queued_at\":1718600000000}"
puts "ev_update    " + (MastodonEnvelope.event_of(m) == "update").to_s
puts "pl_object    " + (MastodonEnvelope.payload_of(m) == "{\"id\":\"1\",\"content\":\"hi\"}").to_s

# string payload (delete carries a bare status id)
m = "{\"event\":\"delete\",\"payload\":\"123456789\"}"
puts "ev_delete    " + (MastodonEnvelope.event_of(m) == "delete").to_s
puts "pl_string    " + (MastodonEnvelope.payload_of(m) == "123456789").to_s

# key order flipped
m = "{\"payload\":{\"a\":1},\"event\":\"notification\"}"
puts "ev_flipped   " + (MastodonEnvelope.event_of(m) == "notification").to_s
puts "pl_flipped   " + (MastodonEnvelope.payload_of(m) == "{\"a\":1}").to_s

# nested "payload"/"event" keys inside the payload must not confuse the
# top-level walk
m = "{\"event\":\"update\",\"payload\":{\"event\":\"fake\",\"payload\":\"inner\",\"n\":{\"payload\":9}}}"
puts "ev_nested    " + (MastodonEnvelope.event_of(m) == "update").to_s
puts "pl_nested    " + (MastodonEnvelope.payload_of(m) == "{\"event\":\"fake\",\"payload\":\"inner\",\"n\":{\"payload\":9}}").to_s

# escaped quotes and braces inside strings
m = "{\"event\":\"update\",\"payload\":{\"content\":\"a \\\"quo}ted\\\" brace{\"}}"
puts "pl_escaped   " + (MastodonEnvelope.payload_of(m) == "{\"content\":\"a \\\"quo}ted\\\" brace{\"}").to_s

# whitespace tolerance
m = "{ \"event\" : \"update\" , \"payload\" : [1, 2, 3] }"
puts "pl_array_ws  " + (MastodonEnvelope.payload_of(m) == "[1, 2, 3]").to_s

# numeric payload and missing keys
m = "{\"event\":\"x\",\"payload\":42}"
puts "pl_number    " + (MastodonEnvelope.payload_of(m) == "42").to_s
m = "{\"payload\":1}"
puts "ev_missing   " + (MastodonEnvelope.event_of(m) == "").to_s
puts "pl_notjson   " + (MastodonEnvelope.payload_of("hello") == "").to_s
puts "pl_empty     " + (MastodonEnvelope.payload_of("") == "").to_s
puts "pl_emptyobj  " + (MastodonEnvelope.payload_of("{}") == "").to_s

# unicode content passes through byte-exact
m = "{\"event\":\"update\",\"payload\":{\"c\":\"héllo→\"}}"
puts "pl_utf8      " + (MastodonEnvelope.payload_of(m) == "{\"c\":\"héllo→\"}").to_s

# channel map
puts "ch_public    " + (MastodonChannels.redis_channel_for_path("/api/v1/streaming/public") == "timeline:public").to_s
puts "ch_local     " + (MastodonChannels.redis_channel_for_path("/api/v1/streaming/public/local") == "timeline:public:local").to_s
puts "ch_unknown   " + (MastodonChannels.redis_channel_for_path("/api/v1/streaming/user") == "").to_s
puts "ch_other     " + (MastodonChannels.redis_channel_for_path("/health") == "").to_s

# --- WS lane: json_quote + double-encoded envelope (Node parity) -----------

q = MastodonWsEnvelope.json_quote("plain")
puts "jq_plain     " + (q == "\"plain\"").to_s
q = MastodonWsEnvelope.json_quote("a\"b\\c")
puts "jq_escapes   " + (q == "\"a\\\"b\\\\c\"").to_s
q = MastodonWsEnvelope.json_quote("l1\nl2\r\tend")
puts "jq_ctl       " + (q == "\"l1\\nl2\\r\\tend\"").to_s
q = MastodonWsEnvelope.json_quote([1].pack("C*") + "x")
puts "jq_u0001     " + (q == "\"\\u0001x\"").to_s
q = MastodonWsEnvelope.json_quote("héllo→")
puts "jq_utf8      " + (q == "\"héllo→\"").to_s

env = MastodonWsEnvelope.build("public", "update", "{\"id\":\"1\"}")
puts "ws_env       " + (env == "{\"stream\":[\"public\"],\"event\":\"update\",\"payload\":\"{\\\"id\\\":\\\"1\\\"}\"}").to_s
env = MastodonWsEnvelope.build("public", "delete", "12345")
puts "ws_env_del   " + (env == "{\"stream\":[\"public\"],\"event\":\"delete\",\"payload\":\"12345\"}").to_s

# --- stream-name mappings ---------------------------------------------------

puts "st_pub       " + (MastodonChannels.redis_channel_for_stream("public") == "timeline:public").to_s
puts "st_local     " + (MastodonChannels.redis_channel_for_stream("public:local") == "timeline:public:local").to_s
puts "st_unknown   " + (MastodonChannels.redis_channel_for_stream("user") == "").to_s
puts "st_rev       " + (MastodonChannels.stream_for_channel("timeline:public") == "public").to_s
puts "st_rev_loc   " + (MastodonChannels.stream_for_channel("timeline:public:local") == "public:local").to_s
puts "st_rev_unk   " + (MastodonChannels.stream_for_channel("other") == "").to_s

# --- auth: token extraction precedence + charset gate + sql quoting --------

tk = MastodonAuthToken.pick("qtok", "ptok", "Bearer btok")
puts "tok_query    " + (tk == "qtok").to_s
tk = MastodonAuthToken.pick("", "ptok", "Bearer btok")
puts "tok_proto    " + (tk == "ptok").to_s
tk = MastodonAuthToken.pick("", "", "Bearer btok")
puts "tok_bearer   " + (tk == "btok").to_s
tk = MastodonAuthToken.pick("", "", "Basic xyz")
puts "tok_badhdr   " + (tk == "").to_s
tk = MastodonAuthToken.pick("", "", "")
puts "tok_none     " + (tk == "").to_s

puts "shape_ok     " + MastodonAuthToken.valid_shape?("Abc123_-xyz").to_s
puts "shape_quote  " + (!MastodonAuthToken.valid_shape?("a'b")).to_s
puts "shape_space  " + (!MastodonAuthToken.valid_shape?("a b")).to_s
puts "shape_empty  " + (!MastodonAuthToken.valid_shape?("")).to_s
puts "shape_long   " + (!MastodonAuthToken.valid_shape?("a" * 256)).to_s

puts "quote_plain  " + (MastodonAuthToken.sql_quote("abc") == "'abc'").to_s
puts "quote_sq     " + (MastodonAuthToken.sql_quote("a'b''c") == "'a''b''''c'").to_s
nul = [110, 0, 120].pack("C*")
puts "quote_nul    " + (MastodonAuthToken.sql_quote(nul) == "'nx'").to_s

# --- user channel mapping -----------------------------------------------------

puts "chan_user    " + (MastodonChannels.channel_for("user", "42") == "timeline:42").to_s
puts "chan_noacct  " + (MastodonChannels.channel_for("user", "") == "").to_s
puts "chan_pub     " + (MastodonChannels.channel_for("public", "") == "timeline:public").to_s
puts "uch_yes      " + MastodonChannels.user_channel?("timeline:42").to_s
puts "uch_public   " + (!MastodonChannels.user_channel?("timeline:public")).to_s
puts "uch_prefix   " + (!MastodonChannels.user_channel?("timeline:")).to_s
puts "uch_other    " + (!MastodonChannels.user_channel?("subscribed:timeline:1")).to_s
puts "rev_user     " + (MastodonChannels.stream_for_channel("timeline:42") == "user").to_s
