# frozen_string_literal: true

# Run with the real Sidekiq and sidekiq-cron (test/interop/Gemfile).
# Reads the cron job Robin saved (and tries to claim the tick Robin
# enqueued, at CLAIMED), then saves one of its own and enqueues its tick
# at TICK. Prints what it found and did as JSON.
require "sidekiq"
require "sidekiq-cron"
require "json"
require "logger"

Sidekiq.configure_client do |config|
  config.redis = {url: ENV.fetch("REDIS_URL")}
  config.logger = Logger.new($stderr, level: Logger::WARN) # stdout is for the JSON
end

class InteropJob
  include Sidekiq::Job

  def perform(*) = nil
end

robins = Sidekiq::Cron::Job.find("from robin")
read = {
  "names" => Sidekiq::Cron::Job.all("*").map(&:name).sort,
  "job" => {"name" => robins.name, "cron" => robins.cron, "klass" => robins.klass, "args" => robins.args,
            "status" => robins.status, "source" => robins.source, "description" => robins.description,
            "queue" => JSON.parse(robins.message).fetch("queue"),
            "last_enqueue_time" => robins.last_enqueue_time&.to_i, "history" => robins.jid_history_from_redis.size},
  "claims_robins_tick" => robins.should_enqueue?(Time.at(Float(ENV.fetch("CLAIMED"))).utc)
}

Sidekiq::Cron::Job.create(name: "from sidekiq-cron", cron: "* * * * *", class: "InteropJob", args: ["x"],
  queue: "cron", retry: "3", description: "made by sidekiq-cron")
mine = Sidekiq::Cron::Job.find("from sidekiq-cron")
mine.test_and_enqueue_for_time!(Time.at(Float(ENV.fetch("TICK"))).utc)

puts JSON.generate(read.merge("enqueued" => Sidekiq::Cron::Job.find("from sidekiq-cron").jid_history_from_redis.size))
