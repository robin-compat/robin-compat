# frozen_string_literal: true

# Run with the real Sidekiq (test/interop/Gemfile): runs, in an embedded
# Sidekiq server, the jobs Robin pushed and scheduled on the "back" queue
# (as after rolling back from Robin to Sidekiq), and prints their
# arguments as JSON.
require "sidekiq"
require "sidekiq/api"
require "json"
require "logger"

REDIS = {url: ENV.fetch("REDIS_URL")}.freeze
EXPECTED = Integer(ENV.fetch("EXPECTED"))
Sidekiq.configure_client do |config|
  config.redis = REDIS
  config.logger = Logger.new($stderr, level: Logger::WARN) # stdout is for the JSON
end

class InteropJob
  include Sidekiq::Job

  def perform(*args) = Sidekiq.redis { |c| c.rpush("interop:ran", Sidekiq.dump_json(args)) }
end

server = Sidekiq.configure_embed do |config|
  config.redis = REDIS
  config.queues = %w[back]
  config.concurrency = 1
  config.average_scheduled_poll_interval = 0.2
  config.logger = Logger.new(IO::NULL)
end
server.run
deadline = Time.now + 15
sleep 0.1 until Sidekiq.redis { |c| c.llen("interop:ran") } >= EXPECTED || Time.now > deadline
server.stop

puts JSON.generate(Sidekiq.redis { |c| c.lrange("interop:ran", 0, -1) }.map { |raw| Sidekiq.load_json(raw) })
