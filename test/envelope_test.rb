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
