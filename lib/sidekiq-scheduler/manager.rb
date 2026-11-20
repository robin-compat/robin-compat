# frozen_string_literal: true

module SidekiqScheduler
  # Sets up the Scheduler a server runs: Config's settings, overridden by
  # those given Sidekiq::Scheduler (and Sidekiq.schedule) beforehand, as in
  # an initializer. A schedule that isn't dynamic is saved to Redis,
  # replacing the one there.
  class Manager
    def initialize(config)
      set_current_scheduler_options(config)
      @scheduler_instance = Scheduler.new(config)
      Scheduler.instance = @scheduler_instance
      Sidekiq.schedule = config.schedule if @scheduler_instance.enabled && !@scheduler_instance.dynamic
    end

    def start = @scheduler_instance.load_schedule!

    def stop = @scheduler_instance.clear_schedule!

    def to_hash = {scheduler: @scheduler_instance.to_hash}

    private

    def set_current_scheduler_options(config)
      current = Scheduler
      config.enabled = current.enabled unless current.enabled.nil?
      config.dynamic = current.dynamic unless current.dynamic.nil?
      config.dynamic_every = current.dynamic_every unless current.dynamic_every.nil?
      schedule = Sidekiq.schedule
      config.schedule = schedule unless schedule.nil? || schedule.empty?
      config.listened_queues_only = current.listened_queues_only unless current.listened_queues_only.nil?
    end
  end
end
