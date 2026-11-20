# frozen_string_literal: true

# Run with the real Sidekiq and sidekiq-scheduler (test/interop/Gemfile).
# Reads the schedule Robin saved (and tries to push the tick Robin pushed,
# at CLAIMED), then schedules a job of its own, pushes its tick at TICK,
# and disables it from the dashboard's toggle. Prints what it found and
# did as JSON.
require "sidekiq"
require "sidekiq-scheduler"
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

redis = SidekiqScheduler::RedisManager
scheduler = SidekiqScheduler::Scheduler.instance
Sidekiq.reload_schedule!
read = {
  "schedule" => Sidekiq.get_schedule,
  "enabled" => Sidekiq.schedule.keys.sort.to_h { |name| [name, scheduler.job_enabled?(name)] },
  "pushes_robins_tick" => redis.register_job_instance("from robin", Time.at(Integer(ENV.fetch("CLAIMED")))),
  "last_time" => Time.parse(redis.get_job_last_time("from robin")).to_i,
  "next_time" => Time.parse(redis.get_job_next_time("from robin")).to_i,
  "changed" => redis.get_schedule_changes(0, Time.now.to_f + 1).sort
}

mine = {"cron" => "* * * * *", "class" => "InteropJob", "args" => ["x"], "queue" => "scheduled"}
Sidekiq.set_schedule("from sidekiq-scheduler", mine)
scheduler.idempotent_job_enqueue("from sidekiq-scheduler", Time.at(Integer(ENV.fetch("TICK"))),
  SidekiqScheduler::Utils.sanitize_job_config(mine))
Sidekiq.reload_schedule!
scheduler.toggle_job_enabled("from sidekiq-scheduler")

puts JSON.generate(read)
