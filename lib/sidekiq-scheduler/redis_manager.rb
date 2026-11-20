# frozen_string_literal: true

require "json"

module SidekiqScheduler
  # sidekiq-scheduler's data in Redis, in its keys and formats:
  #
  #   schedules                         hash  name => schedule JSON
  #   schedules_changed                 zset  names, scored by when they changed
  #   sidekiq-scheduler:states          hash  name => {"enabled": ...} JSON
  #   sidekiq-scheduler:next_times      hash  name => "2030-01-01 00:00:00 +0000"
  #   sidekiq-scheduler:last_times      hash  name => the same
  #   sidekiq-scheduler:pushed:<name>   zset  the times (epoch seconds) pushed
  #
  # A job is pushed for a time only by whoever adds that time to its pushed
  # set, so Robin and sidekiq-scheduler never push the same cron tick twice.
  module RedisManager
    # How long a pushed time is remembered.
    REGISTERED_JOBS_THRESHOLD_IN_SECONDS = 24 * 60 * 60

    class << self
      attr_reader :key_prefix

      # Scopes the keys, as for apps sharing a database. A ":" is added
      # unless the prefix ends in ":" or ".".
      def key_prefix=(value)
        @key_prefix = (value.nil? || value.end_with?(":", ".")) ? value : "#{value}:"
      end

      def get_job_schedule(name) = command("HGET", schedules_key, name)

      def get_job_state(name) = command("HGET", schedules_state_key, name)

      def get_job_next_time(name) = command("HGET", next_times_key, name)

      def get_job_last_time(name) = command("HGET", last_times_key, name)

      def set_job_schedule(name, config) = command("HSET", schedules_key, name, JSON.generate(config))

      def set_job_state(name, state) = command("HSET", schedules_state_key, name, JSON.generate(state))

      def set_job_next_time(name, next_time) = command("HSET", next_times_key, name, next_time)

      def set_job_last_time(name, last_time) = command("HSET", last_times_key, name, last_time)

      def remove_job_schedule(name) = command("HDEL", schedules_key, name)

      def remove_job_next_time(name) = command("HDEL", next_times_key, name)

      # name => schedule JSON, for every schedule.
      def get_all_schedules = command("HGETALL", schedules_key).each_slice(2).to_h

      def schedule_exist? = command("EXISTS", schedules_key).positive?

      # The names of the schedules changed from +from+ up to (not
      # including) +to+.
      def get_schedule_changes(from, to) = command("ZRANGE", schedules_changed_key, from, "(#{to}", "BYSCORE")

      def add_schedule_change(name) = command("ZADD", schedules_changed_key, Time.now.to_f, name)

      # Drops schedules_changed when it's a set (as sidekiq-scheduler kept
      # it before 4.0).
      def clean_schedules_changed
        Robin.redis do |c|
          c.call("DEL", schedules_changed_key) unless c.call("TYPE", schedules_changed_key) == "zset"
        end
      end

      # Whether this added +time+ to the job's pushed times: true for the
      # one process (Robin's or sidekiq-scheduler's) that's to push it.
      def register_job_instance(job_name, time)
        key = pushed_job_key(job_name)
        added, = Robin.redis do |c|
          c.multi do |tx|
            tx.call("ZADD", key, Integer(time), Integer(time))
            tx.call("EXPIRE", key, REGISTERED_JOBS_THRESHOLD_IN_SECONDS)
          end
        end
        added.positive?
      end

      # Forgets the job's pushed times older than a day.
      def remove_elder_job_instances(job_name)
        command("ZREMRANGEBYSCORE", pushed_job_key(job_name), "-inf", Integer(Time.now) - REGISTERED_JOBS_THRESHOLD_IN_SECONDS)
      end

      def pushed_job_key(job_name) = "#{prefix}sidekiq-scheduler:pushed:#{job_name}"

      def next_times_key = "#{prefix}sidekiq-scheduler:next_times"

      def last_times_key = "#{prefix}sidekiq-scheduler:last_times"

      def schedules_state_key = "#{prefix}sidekiq-scheduler:states"

      def schedules_key = "#{prefix}schedules"

      def schedules_changed_key = "#{prefix}schedules_changed"

      private

      # The prefix of Sidekiq's own keys in Robin (none, by default), then
      # key_prefix.
      def prefix = "#{Robin.config.key_prefix}#{key_prefix}"

      def command(*args) = Robin.redis { |c| c.call(*args) }
    end
  end
end
