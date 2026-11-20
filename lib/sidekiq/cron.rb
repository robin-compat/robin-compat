# frozen_string_literal: true

require "sidekiq"
require "robin/compat/cron_line"

module Sidekiq
  # sidekiq-cron's interface, on Robin: cron jobs kept in Redis, created at
  # runtime or loaded from config/schedule.yml.
  #
  #   Sidekiq::Cron::Job.create(name: "Hard worker", cron: "*/5 * * * *", class: "HardWorker")
  #
  # The jobs are stored as sidekiq-cron stores them, so the ones it made
  # keep running, and the two can run side by side while you switch.
  # Robin's leader enqueues them, claiming each tick in Redis the way
  # sidekiq-cron does, so a tick is enqueued once by either.
  #
  # A schedule is a cron line (five fields, or six with seconds first),
  # with an optional time zone at its end ("0 22 * * 1-5 America/Chicago";
  # without one, Rails' Time.zone, else the system's). Natural-language
  # schedules ("every day at five") need the fugit gem.
  module Cron
    class << self
      def configuration = @configuration ||= Configuration.new

      def configure
        yield configuration if block_given?
      end

      def reset! = @configuration = Configuration.new

      # What a server does as it starts (unless disabled): loads the
      # schedule file, and polls for due jobs until it quiets or stops.
      def boot(config)
        return unless configuration.enabled

        Job.migrate_old_jobs_if_needed!
        loader = ScheduleLoader.new
        if loader.has_schedule_file?
          loader.load_schedule.each do |name, errors|
            config.logger.warn { "Cron Jobs - #{name} in the schedule file is invalid: #{errors.join(", ")}" }
          end
        end
        interval = Integer(configuration.cron_poll_interval)
        return unless interval.positive?

        @poller = Poller.new(config, interval: interval).start
        config.on(:quiet) { halt }
        config.on(:shutdown) { halt }
      end

      # Stops polling, as a server quiets or stops.
      def halt
        @poller&.terminate
        @poller = nil
      end

      # A schedule as a Robin::Cron: a cron line, with an optional time
      # zone at its end, or (with fugit) natural language, read as
      # natural_cron_parsing_mode says: the first of several schedules
      # (:single) or an error (:strict).
      def parse(schedule)
        Robin::Compat::CronLine.parse(schedule)
      rescue Robin::Cron::ParseError => e
        parser = Robin::Compat::CronLine.fugit
        raise e.exception("#{e} (natural-language schedules need the fugit gem)") unless parser

        natural = parser.parse_nat(schedule, multi: (configuration.natural_cron_parsing_mode == :strict) && :fail)
        raise unless natural

        parse(natural.to_cron_s) # a cron line, which fugit made
      end
    end

    # Sidekiq::Cron.configure's settings, with sidekiq-cron's defaults.
    # cron_poll_process_count is taken but unused: only Robin's leader
    # polls.
    class Configuration
      attr_accessor :enabled, :cron_poll_interval, :cron_schedule_file, :cron_history_size,
        :default_namespace, :available_namespaces, :reschedule_grace_period
      attr_reader :natural_cron_parsing_mode, :cron_poll_process_count

      def initialize
        @enabled = true
        @cron_poll_interval = 30
        @cron_schedule_file = "config/schedule.yml"
        @cron_history_size = 10
        @default_namespace = "default"
        @available_namespaces = [@default_namespace]
        @natural_cron_parsing_mode = :single
        @reschedule_grace_period = 60
      end

      def natural_cron_parsing_mode=(mode)
        raise ArgumentError, "Unknown natural cron parsing mode: #{mode.inspect}" unless %i[single strict].include?(mode)

        @natural_cron_parsing_mode = mode
      end

      def cron_poll_process_count=(count)
        raise ArgumentError, "invalid cron process count: #{count.inspect}" unless count.instance_of?(Integer) && count.positive?

        @cron_poll_process_count = count
      end
    end

    # The namespaces cron jobs are grouped in.
    class Namespace
      class << self
        # available_namespaces (or, with :auto, those found in Redis), and
        # the default one.
        def all
          names = case (available = Cron.configuration.available_namespaces)
          when nil then []
          when Array then available
          when :auto then Cron.redis { |c| c.call("KEYS", "#{Cron.prefix}cron_jobs:*") }.map { |key| key.split(":").last }
          else raise ArgumentError, "Unexpected value provided for `available_namespaces`: #{available.inspect}"
          end
          names | [Cron.configuration.default_namespace]
        end

        def all_with_count = all.map { |name| {count: count(name), name: name} }

        def count(name = nil) = Job.count(name)

        # Whether available_namespaces lists them (rather than :auto).
        def available_namespaces_provided?
          available = Cron.configuration.available_namespaces
          !available.nil? && !available.equal?(:auto)
        end
      end
    end

    class << self
      # The Redis cron jobs live in: Robin's (Sidekiq's).
      def redis(&) = Robin.redis(&)

      # Prefixed to sidekiq-cron's keys: the prefix of Sidekiq's own keys
      # (none, by default).
      def prefix = Robin.config.key_prefix
    end
  end
end

require "sidekiq/cron/job"
require "sidekiq/cron/schedule_loader"
require "sidekiq/cron/poller"
