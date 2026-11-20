# frozen_string_literal: true

require "json"
require "sidekiq/api"

module SidekiqScheduler
  # The scheduler: loads Sidekiq.schedule (from Redis, when dynamic) and
  # pushes each job as it comes due. Only Robin's leader pushes, and each
  # push is claimed in Redis (RedisManager.register_job_instance), so
  # sidekiq-scheduler processes and Robin's never push a cron tick twice.
  #
  # The class forwards its instance's methods to Scheduler.instance, as
  # sidekiq-scheduler's does (Sidekiq::Scheduler.reload_schedule!).
  class Scheduler
    attr_accessor :enabled, :dynamic, :dynamic_every, :listened_queues_only, :rufus_scheduler_options

    # The jobs scheduled, by name (ScheduledJob each).
    attr_reader :scheduled_jobs

    class << self
      attr_writer :instance

      def instance = @instance ||= new

      def method_missing(name, ...)
        method_defined?(name) ? instance.public_send(name, ...) : super
      end

      def respond_to_missing?(name, *) = method_defined?(name)
    end

    def initialize(config = Config.new(without_defaults: true))
      @scheduler_config = config
      self.enabled = config.enabled?
      self.dynamic = config.dynamic?
      self.dynamic_every = config.dynamic_every?
      self.listened_queues_only = config.listened_queues_only?
      self.rufus_scheduler_options = config.rufus_scheduler_options || {}
      @scheduled_jobs = {}
      @tried = {}
    end

    def print_schedule
      logger.info "Scheduling Info\tLast Run"
      scheduled_jobs.each_value { |job| logger.info "#{job.schedule}\t#{RedisManager.get_job_last_time(job.name)}\t" }
    end

    # Schedules Sidekiq.schedule's jobs (reloading it first when dynamic)
    # and starts pushing them as they come due.
    def load_schedule!
      return logger.info("SidekiqScheduler is disabled") unless enabled

      logger.info "Loading Schedule"
      if dynamic
        Sidekiq.reload_schedule!
        @current_changed_score = Time.now.to_f
      end
      schedule = Sidekiq.schedule.to_h
      logger.info "Schedule empty! Set Sidekiq.schedule" if schedule.empty?
      @scheduled_jobs = {}
      schedule.each { |name, config| load_schedule_job(name, config) }
      logger.info "Schedules Loaded"
      @poller ||= Poller.new(self, Robin.config).start
    end

    # Schedules one job, unless its rails_env (a comma-separated list)
    # leaves out RAILS_ENV.
    def load_schedule_job(name, config)
      return unless config["rails_env"].nil? || rails_env_matches?(config)

      logger.info "Scheduling #{name} #{config}"
      job = ScheduledJob.build(name, config)
      return logger.info("no #{ScheduledJob::TYPES.join(" / ")} found for #{config["class"]} (#{name}) - skipping") unless job

      @scheduled_jobs[name] = job
      Utils.update_job_next_time(name, job.next_time)
    end

    # Pushes the job for +time+, unless it was pushed for it already.
    def idempotent_job_enqueue(job_name, time, config)
      if RedisManager.register_job_instance(job_name, time)
        logger.info "queueing #{config.fetch("class")} (#{job_name})"
        handle_errors { enqueue_job(config, time) }
        RedisManager.remove_elder_job_instances(job_name)
      else
        logger.debug { "Ignoring #{job_name} job as it has been already enqueued" }
      end
    end

    # Pushes a job from its schedule: with Active Job for Active Job
    # classes, else Sidekiq's client. With include_metadata, its arguments
    # end with {"scheduled_at" => +time+ (epoch seconds)}.
    def enqueue_job(job_config, time = Time.now)
      config = job_config.dup
      config["class"] = Utils.try_to_constantize(config.fetch("class"))
      args = config["args"]
      if !args.instance_of?(Hash)
        args = Array(args)
      elsif Sidekiq.compatible_before?("7.3") && args.respond_to?(:symbolize_keys)
        # sidekiq-scheduler 5 (for Sidekiq 7.2 and earlier) symbolizes a
        # Hash's keys, with Active Support; 6 doesn't.
        args = args.symbolize_keys
      end
      args = [*(args.instance_of?(Array) ? args : [args]), {"scheduled_at" => time.to_f.round(3)}] if config.delete("include_metadata")
      config["args"] = args
      Utils.active_job_enqueue?(config.fetch("class")) ? Utils.enqueue_with_active_job(config) : Utils.enqueue_with_sidekiq(config)
    end

    # Stops pushing, and unschedules every job.
    def clear_schedule!(*) # sidekiq-scheduler's stop option, unused
      @poller&.terminate
      @poller = nil
      @scheduled_jobs = {}
      @tried = {}
    end

    def reload_schedule!
      return logger.info("SidekiqScheduler is disabled") unless enabled

      logger.info "Reloading Schedule"
      clear_schedule!
      load_schedule!
    end

    # Reschedules the jobs whose schedules changed in Redis since the last
    # look (for dynamic schedules).
    def update_schedule
      since, @current_changed_score = @current_changed_score, Time.now.to_f
      changes = RedisManager.get_schedule_changes(since, @current_changed_score)
      return if changes.empty?

      logger.info "Updating schedule"
      Sidekiq.reload_schedule!
      changes.each do |name|
        @scheduled_jobs.delete(name)
        @tried.delete(name)
        config = Sidekiq.schedule[name]
        load_schedule_job(name, config) if config
      end
      logger.info "Schedule updated"
    end

    # Whether a job in the schedule is enabled: its state in Redis (the
    # dashboard's toggle), else its "enabled" (true by default). nil when
    # it isn't in the schedule.
    def job_enabled?(name)
      config = Sidekiq.schedule.to_h[name]
      schedule_state(name).fetch("enabled", config.fetch("enabled", true)) if config
    end

    def toggle_job_enabled(name)
      set_schedule_state(name, schedule_state(name).merge({"enabled" => !job_enabled?(name)}))
    end

    def toggle_all_jobs(new_state)
      Sidekiq.schedule!.each_key { |name| set_schedule_state(name, schedule_state(name).merge({"enabled" => new_state})) }
    end

    # Pushes the jobs due at +now+ (each once for each of its times), as
    # Robin's leader does: Poller calls this.
    def enqueue_due(now = Time.now)
      queues = listened_queues if listened_queues_only
      @scheduled_jobs.each_value do |job|
        time = job.due(now)
        next if @tried[job.name] == time # nil (not due) or tried already

        @tried[job.name] = time
        next unless queues.nil? || queues.empty? || queues.include?(job.config["queue"].to_s)

        idempotent_job_enqueue(job.name, time, job.config) if job_enabled?(job.name)
        Utils.update_job_last_time(job.name, time)
        Utils.update_job_next_time(job.name, job.next_time(now))
      rescue => e
        Robin.config.handle_exception(e, {context: "sidekiq-scheduler job #{job.name}"})
      end
    end

    # How long Poller can wait from +now+: until the next job is due, at
    # most a second.
    def seconds_until_next(now = Time.now)
      soonest = @scheduled_jobs.each_value.filter_map { |job| job.next_time(now) }.min
      soonest ? (soonest - now).clamp(0.05, 1.0) : 1.0
    end

    def to_hash = {scheduler_config: @scheduler_config.to_hash}

    def inspect
      "#<SidekiqScheduler::Scheduler enabled=#{enabled} dynamic=#{dynamic} dynamic_every=#{dynamic_every} " \
        "listened_queues_only=#{listened_queues_only} rufus_scheduler_options=#{rufus_scheduler_options}>"
    end

    private

    def logger = Sidekiq.logger

    def schedule_state(name)
      state = RedisManager.get_job_state(name)
      state ? JSON.parse(state) : {}
    end

    def set_schedule_state(name, state) = RedisManager.set_job_state(name, state)

    def rails_env_matches?(config) = config.fetch("rails_env").delete(" \t\n").split(",").include?(ENV["RAILS_ENV"])

    # The queues the live processes take jobs from: the jobs whose queue
    # none of them does aren't pushed (listened_queues_only).
    def listened_queues
      Sidekiq::ProcessSet.new.flat_map(&:queues)
    end

    # Errors pushing a job are logged, as sidekiq-scheduler does.
    def handle_errors
      yield
    rescue => e
      logger.info "#{e.class}: #{e}"
    end
  end
end
