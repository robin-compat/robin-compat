# frozen_string_literal: true

require "json"

module SidekiqScheduler
  # The schedule methods sidekiq-scheduler gives Sidekiq (Sidekiq.schedule=,
  # Sidekiq.set_schedule and so on). The schedule is kept in Redis: name =>
  # {"cron" => "0 * * * *", "class" => "HardWorker", "args" => [...], ...}.
  module Schedule
    # Replaces the schedule: each job is saved (its "class" defaulting to
    # its name, and its "queue" to its class's) and the jobs not in
    # +schedule_hash+ are removed.
    def schedule=(schedule_hash)
      prepared = prepare_schedule(schedule_hash)
      removed = get_all_schedules.keys - prepared.keys
      prepared.each { |name, config| set_schedule(name, config) }
      removed.each { |name| remove_schedule(name) }
      @schedule = prepared
    end

    # The schedule as last set or loaded (nil before).
    def schedule = @schedule

    # Loads the schedule from Redis.
    def reload_schedule! = @schedule = get_schedule

    alias_method :schedule!, :reload_schedule!

    # A job's schedule, or (without a name) every job's.
    def get_schedule(name = nil)
      return get_all_schedules unless name

      json = RedisManager.get_job_schedule(name)
      JSON.parse(json) if json
    end

    def get_all_schedules = RedisManager.get_all_schedules.transform_values { |json| JSON.parse(json) }

    # Saves a job's schedule, noting the change for dynamic schedulers
    # unless it's the same. Returns +config+.
    def set_schedule(name, config)
      unless get_schedule(name) == config
        RedisManager.set_job_schedule(name, config)
        RedisManager.add_schedule_change(name)
      end
      config
    end

    def remove_schedule(name)
      RedisManager.remove_job_schedule(name)
      RedisManager.add_schedule_change(name)
    end

    private

    def prepare_schedule(schedule_hash)
      Utils.stringify_keys(schedule_hash).to_h do |name, config|
        config = {"class" => name}.merge(config)
        queue = Utils.sidekiq_queue(config.fetch("class"))
        config["queue"] ||= queue if queue
        [name, config]
      end
    end
  end
end
