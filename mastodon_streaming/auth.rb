# OAuth token resolution for the streaming API. Token sources mirror
# Node's precedence: the access_token query param, then the
# Sec-WebSocket-Protocol header (the browser client smuggles the token
# as the WS subprotocol), then Authorization: Bearer.
#
# The lookup mirrors Node's query (minus devices/chosen_languages):
# token -> users.account_id via oauth_access_tokens, revoked tokens
# excluded. Simple-query protocol means string interpolation: tokens
# pass a strict charset gate (Doorkeeper tokens are urlsafe-base64)
# AND get quoted — defense in depth until the extended protocol lands.
require "pg"
require_relative "envelope"

# Transport stand-in so the resolver exists (typed) before boot wires
# the real connection.
class NullPgTransport
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

class StreamingAuth
  def initialize
    @db = PgClientCore.new(NullPgTransport.new, "", "", "", "")
    @connected = false
  end

  def connected?
    @connected
  end

  def connect(host, port, dbname, user, password)
    c = PG.connect(host, port, dbname, user, password)
    @db = c
    @connected = true
    0
  end

  # Token -> account id; "" = missing/malformed/unknown/revoked (or no
  # DB wired). One blocking query on the shared connection: exec runs
  # to completion inside the calling fiber, so connection fibers can't
  # interleave mid-query (pool + scheduler-parked reads are ledgered).
  def resolve(token)
    if !@connected
      return ""
    end
    if !MastodonAuthToken.valid_shape?(token)
      return ""
    end
    sql = "SELECT users.account_id FROM oauth_access_tokens " +
          "JOIN users ON users.id = oauth_access_tokens.resource_owner_id " +
          "WHERE oauth_access_tokens.token = " + MastodonAuthToken.sql_quote(token) +
          " AND oauth_access_tokens.revoked_at IS NULL LIMIT 1"
    r = @db.exec(sql)
    if r.ntuples == 0
      return ""
    end
    v = r.getvalue(0, 0)
    v.to_s
  end
end
