# frozen_string_literal: true

# Run with the real Sidekiq (test/interop/Gemfile): pushes and schedules
# jobs, and fails some in an embedded Sidekiq server so its retry and
# dead sets have entries Sidekiq itself wrote. Prints what it did as JSON.
require "sidekiq"
require "sidekiq/api"
require "json"
require "logger"

REDIS = {url: ENV.fetch("REDIS_URL")}.freeze
Sidekiq.configure_client do |config|
  config.redis = REDIS
  config.logger = Logger.new($stderr, level: Logger::WARN) # stdout is for the JSON
end

class InteropJob
  include Sidekiq::Job

  def perform(*) = nil
end

class FlakyJob
  include Sidekiq::Job

  sidekiq_options retry: 3, backtrace: 5

  def perform(*) = raise("sidekiq boom")
end

class DoomedJob
  include Sidekiq::Job

  sidekiq_options retry: 0

  def perform(*) = raise("sidekiq dies")
end

FlakyJob.set(queue: "failing").perform_async("flaky")
DoomedJob.set(queue: "failing").perform_async("doomed")
server = Sidekiq.configure_embed do |config|
  config.redis = REDIS
  config.queues = %w[failing]
  config.concurrency = 1
  config.logger = Logger.new(IO::NULL)
end
server.run
deadline = Time.now + 10
sleep 0.05 until (Sidekiq::RetrySet.new.size == 1 && Sidekiq::DeadSet.new.size == 1) || Time.now > deadline
server.stop

puts JSON.generate(
  "queued" => InteropJob.perform_async(1, "two"),
  "scheduled" => InteropJob.perform_in(3600, 3),
  "other" => InteropJob.set(queue: "other").perform_async({"k" => "v"}),
  "retry" => Sidekiq::RetrySet.new.first&.jid,
  "dead" => Sidekiq::DeadSet.new.first&.jid
)
