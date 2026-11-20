# frozen_string_literal: true

require "robin/compat/cron_line"

module SidekiqScheduler
  # sidekiq-scheduler's helpers.
  module Utils
    # A schedule's keys that say when it runs (or describe it), rather than
    # what job it pushes.
    RUFUS_METADATA_KEYS = %w[description at cron every in interval enabled].freeze

    # Compact durations: "45m", "1h30m", "2d", "1w".
    DURATION = /\A(?:\d+[wdhms])+\z/
    DURATION_UNITS = {"w" => 604_800, "d" => 86_400, "h" => 3_600, "m" => 60, "s" => 1}.freeze
    private_constant :DURATION, :DURATION_UNITS

    class << self
      # +object+ with its Hashes' keys (inside Arrays and Sets too) Strings.
      def stringify_keys(object) = deep_keys(object, &:to_s)

      # +object+ with its Hashes' keys (inside Arrays and Sets too) Symbols.
      def symbolize_keys(object) = deep_keys(object, &:to_sym)

      # The class a String names, or +klass+ itself (when it isn't a
      # String, or no such class is loaded).
      def try_to_constantize(klass)
        klass.instance_of?(String) ? Object.const_get(klass) : klass
      rescue NameError
        klass
      end

      # An instance of the Active Job class +klass+, with +args+ (an
      # Array of arguments, or a Hash of keyword arguments with
      # +keyword_argument+, else one argument).
      def initialize_active_job(klass, args, keyword_argument = false)
        if args.instance_of?(Array)
          klass.new(*args)
        elsif args.instance_of?(Hash) && keyword_argument
          klass.new(**symbolize_keys(args))
        else
          klass.new(args)
        end
      end

      # Whether +klass+ is an Active Job class (pushed with Active Job).
      def active_job_enqueue?(klass)
        base = active_job_base
        (klass.instance_of?(Class) && base && klass < base) || false
      end

      def enqueue_with_sidekiq(config) = Sidekiq::Client.push(sanitize_job_config(config))

      def enqueue_with_active_job(config)
        # Active Job keeps the job's own queue for queue: nil.
        initialize_active_job(config.fetch("class"), config["args"], config["keyword_argument"]).enqueue(queue: config["queue"])
      end

      def sanitize_job_config(config) = config.except(*RUFUS_METADATA_KEYS)

      # Saves when a job runs next, or forgets it (nil).
      def update_job_next_time(name, next_time)
        if next_time
          RedisManager.set_job_next_time(name, format_time(next_time))
        else
          RedisManager.remove_job_next_time(name)
        end
      end

      def update_job_last_time(name, last_time)
        RedisManager.set_job_last_time(name, format_time(last_time)) if last_time
      end

      # The queue a Sidekiq job class (by name or itself) sets, for a
      # schedule without one. nil for Active Job classes, which pick
      # theirs as they're enqueued.
      def sidekiq_queue(klass)
        klass = try_to_constantize(klass)
        klass.sidekiq_options["queue"] if klass.respond_to?(:sidekiq_options) && !active_job_enqueue?(klass)
      end

      # A duration as seconds: a number, "45m", "1h30m", or (with the fugit
      # gem) "2 hours and 30 minutes".
      def duration(value)
        return value if value.is_a?(Numeric)

        text = value.to_s.strip
        return Integer(text, 10) if text.match?(/\A\d+\z/)
        return text.scan(/(\d+)(\w)/).sum { |count, unit| Integer(count, 10) * DURATION_UNITS.fetch(unit) } if DURATION.match?(text)

        fugit = Robin::Compat::CronLine.fugit
        raise ArgumentError, "#{text.inspect} isn't a duration like \"45m\" or \"1h30m\" (others need the fugit gem)" unless fugit

        parsed = fugit::Duration.parse(text)
        raise ArgumentError, "#{text.inspect} isn't a duration" unless parsed

        parsed.to_sec
      end

      private

      def deep_keys(object, &)
        case object
        when Hash then object.to_h { |key, value| [yield(key), deep_keys(value, &)] }
        when Array, Set then object.map { |value| deep_keys(value, &) }
        else object
        end
      end

      # As rufus-scheduler (et-orbi) writes times, in the system's zone.
      # mutant:disable -- a Time's to_s is this format, and Robin's Redis
      # client writes a Time as its to_s
      def format_time(time) = time.getlocal.strftime("%Y-%m-%d %H:%M:%S %z")

      # ActiveJob::Base, or nil without Active Job.
      # mutant:disable -- Active Job is loaded for every test at once under
      # mutant (the tests stub this to cover both)
      def active_job_base = (defined?(::ActiveJob::Base) && ::ActiveJob::Base) || nil
    end
  end
end
