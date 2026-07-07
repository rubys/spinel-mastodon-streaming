# Standalone Mastodon streaming server — the `main` face of the
# package. Owns everything the library refuses to: env parsing, the
# listen socket, signal handlers (installed by Scheduled#run), and the
# scheduler loop. Env vocabulary matches the Node server where it
# exists yet: PORT, REDIS_HOST, REDIS_PORT.
require "redis"
require "tep"
require "mastodon_streaming"

class MainApp
end

# Tep::APP.dispatch delegates every request to Main.dispatch.
module Main
  def self.dispatch(req, res)
    handled = MastodonStreaming.dispatch(req, res)
    if !handled
      res.status = 404
      res.headers["Content-Type"] = "text/plain"
      res.body = "not found"
    end
    0
  end
end

port_s = ENV["PORT"].to_s
port = 4000
if port_s != ""
  port = port_s.to_i
end
redis_host = ENV["REDIS_HOST"].to_s
if redis_host == ""
  redis_host = "127.0.0.1"
end
redis_port_s = ENV["REDIS_PORT"].to_s
redis_port = 6379
if redis_port_s != ""
  redis_port = redis_port_s.to_i
end

db_name = ENV["DB_NAME"].to_s
if db_name != ""
  db_host = ENV["DB_HOST"].to_s
  if db_host == ""
    db_host = "127.0.0.1"
  end
  db_port_s = ENV["DB_PORT"].to_s
  db_port = 5432
  if db_port_s != ""
    db_port = db_port_s.to_i
  end
  db_user = ENV["DB_USER"].to_s
  db_pass = ENV["DB_PASS"].to_s
  MastodonStreaming.boot_db(db_host, db_port, db_name, db_user, db_pass)
end
MastodonStreaming.boot(redis_host, redis_port)
puts "mastodon-streaming listening on :" + port.to_s + " (redis " + redis_host + ":" + redis_port.to_s + ")"
STDOUT.flush
Tep::Server::Scheduled.new(MainApp.new).run(port, 1, true)
