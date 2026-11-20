# frozen_string_literal: true

# Run with the real Sidekiq (test/interop/Gemfile): reads what Robin wrote
# through Sidekiq's own API, prints it as JSON, then acts on it the way
# Sidekiq's web UI would (retries the retry, kills the scheduled job,
# quiets the processes).
require "sidekiq"
require "sidekiq/api"
require "json"
require "logger"

Sidekiq.configure_client do |config|
  config.redis = {url: ENV.fetch("REDIS_URL")}
  config.logger = Logger.new($stderr, level: Logger::WARN) # stdout is for the JSON
end

stats = Sidekiq::Stats.new
queue = Sidekiq::Queue.new("default")
job = queue.first
scheduled = Sidekiq::ScheduledSet.new.first
retried = Sidekiq::RetrySet.new.first
processes = Sidekiq::ProcessSet.new.to_a
work = []
Sidekiq::Workers.new.each do |process, thread, w|
  # Sidekiq 7.0 and 7.1 yield a Hash, and 7.2 on a Sidekiq::Work.
  work << if w.is_a?(Hash)
    [process, thread, w["queue"], Sidekiq::JobRecord.new(w["payload"]).klass, w["run_at"].to_f]
  else
    [process, thread, w.queue, w.job.klass, w.run_at.to_f]
  end
end

result = {
  "stats" => {"enqueued" => stats.enqueued, "scheduled" => stats.scheduled_size, "retries" => stats.retry_size,
              "dead" => stats.dead_size, "processes" => stats.processes_size, "workers" => stats.workers_size,
              "processed" => stats.processed, "failed" => stats.failed},
  "queues" => Sidekiq::Queue.all.map(&:name).sort,
  "queue" => {"size" => queue.size, "latency" => queue.latency, "class" => job.klass, "display_class" => job.display_class,
              "args" => job.args, "enqueued_at" => job.enqueued_at.to_f, "created_at" => job.created_at.to_f},
  "scheduled" => {"class" => scheduled.klass, "args" => scheduled.args, "at" => scheduled.at.to_f},
  "retry" => {"class" => retried.klass, "retry_count" => retried["retry_count"], "error_class" => retried["error_class"],
              "error_message" => retried["error_message"], "backtrace" => retried.error_backtrace, "at" => retried.at.to_f,
              "failed_at" => retried["failed_at"]},
  "processes" => processes.map do |p|
    {"identity" => p.identity, "busy" => p["busy"], "concurrency" => p["concurrency"], "beat" => p["beat"],
     "queues" => p.queues, "hostname" => p["hostname"], "started_at" => p["started_at"], "quiet" => p["quiet"]}
  end,
  "work" => work
}

retried.retry
scheduled.kill
processes.each(&:quiet!)

puts JSON.generate(result)
