# frozen_string_literal: true

module SidekiqScheduler
  # The scheduler's settings: the :scheduler: section of config/sidekiq.yml
  # (with these defaults), or none.
  class Config
    DEFAULT_OPTIONS = {
      enabled: true, dynamic: false, dynamic_every: "5s", schedule: {}, rufus_scheduler_options: {}
    }.freeze
    private_constant :DEFAULT_OPTIONS

    # +sidekiq_config+ is a Sidekiq::Config (its [:scheduler]). Without it,
    # or without_defaults, settings not given are nil.
    def initialize(sidekiq_config: nil, without_defaults: false)
      @sidekiq_config = sidekiq_config
      given = sidekiq_config&.fetch(:scheduler, nil).to_h.transform_keys(&:to_sym)
      @settings = without_defaults ? given : DEFAULT_OPTIONS.merge(given)
    end

    def enabled? = @settings[:enabled]

    def enabled=(value)
      @settings[:enabled] = value
    end

    def dynamic? = @settings[:dynamic]

    def dynamic=(value)
      @settings[:dynamic] = value
    end

    def dynamic_every? = @settings[:dynamic_every]

    def dynamic_every=(value)
      @settings[:dynamic_every] = value
    end

    def schedule = @settings[:schedule]

    def schedule=(value)
      @settings[:schedule] = value
    end

    def listened_queues_only? = @settings[:listened_queues_only]

    def listened_queues_only=(value)
      @settings[:listened_queues_only] = value
    end

    # Taken, and unused: Robin doesn't schedule with rufus-scheduler.
    def rufus_scheduler_options = @settings[:rufus_scheduler_options]

    def rufus_scheduler_options=(value)
      @settings[:rufus_scheduler_options] = value
    end

    # The queues of +sidekiq_config+, or of Sidekiq's configuration.
    def sidekiq_queues = (@sidekiq_config || Sidekiq.default_configuration).queues

    def to_hash
      {enabled: enabled?, dynamic: dynamic?, dynamic_every: dynamic_every?, schedule: schedule,
       listened_queues_only: listened_queues_only?, rufus_scheduler_options: rufus_scheduler_options}
    end
  end
end
