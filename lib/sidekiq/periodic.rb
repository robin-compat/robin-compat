# frozen_string_literal: true

require "sidekiq"

module Sidekiq
  # Sidekiq Enterprise's periodic jobs, as Robin::Periodic's:
  #
  #   Sidekiq.configure_server do |config|
  #     config.periodic do |mgr|
  #       mgr.tz = ActiveSupport::TimeZone.new("Pacific Time (US & Canada)")
  #       mgr.register("0 * * * *", "HourlyReport", retry: 2, "args" => ["summary"])
  #     end
  #   end
  module Periodic
    # What config.periodic yields.
    class Manager
      def initialize(registry)
        @registry = registry
      end

      # The zone of the jobs registered after it's set (an
      # ActiveSupport::TimeZone, TZInfo zone or name); nil is the system's.
      attr_accessor :tz

      # Options are the job's ("retry", "queue"...), with String or Symbol
      # keys, plus "args" (static), "tz" (this job's zone) and Robin's
      # "description".
      def register(cron, klass, options = {})
        options = options.transform_keys(&:to_sym)
        zone = options.delete(:tz) || tz
        @registry.register(cron, klass, **options, tz: Periodic.zone_name(zone))
      end
    end

    # Each periodic job, as Sidekiq Enterprise's LoopSet lists them.
    class LoopSet
      include Enumerable

      # A periodic job: its id, cron schedule, class name, zone, job
      # options, next run (a Time) and history ([jid, Time] pairs, newest
      # first).
      Loop = Struct.new(:lid, :schedule, :klass, :tz, :options, :next_run, :history)

      # How many runs a Loop's history shows.
      HISTORY = 25

      def each
        return to_enum unless block_given?

        Robin::Periodic.list(history: HISTORY).each do |entry|
          history = entry.fetch("history").map { |run| [run.fetch("jid"), Periodic.time(run.fetch("at"))] }
          yield Loop.new(entry.fetch("id"), entry.fetch("cron"), entry.fetch("class"), entry.fetch("tz"),
            entry.fetch("options").merge({"queue" => entry.fetch("queue"), "args" => entry.fetch("args")}),
            entry.fetch("next_at")&.then { |ms| Periodic.time(ms) }, history)
        end
      end

      def size = count
    end

    # Checks a periodic configuration in a test (raising
    # Robin::Periodic::Invalid with every problem found):
    #
    #   Sidekiq::Periodic::ConfigTester.new.verify { |mgr| mgr.register(...) }
    class ConfigTester
      def verify(&block)
        Robin::Periodic.verify! { |registry| block.call(Manager.new(registry)) }
        true
      end
    end

    class << self
      # A zone's IANA name (nil for none: the system's zone).
      def zone_name(zone)
        return zone.tzinfo.name if zone.respond_to?(:tzinfo)
        return zone.identifier if zone.respond_to?(:identifier)

        zone&.to_s
      end

      def time(ms) = Time.at(Rational(ms, 1000))
    end
  end

  # Sidekiq Enterprise's cron parser.
  class CronParser
    def initialize(expression, tz: nil)
      @cron = Robin::Cron.new(expression, tz: Periodic.zone_name(tz))
    end

    # The next run after +after+ (a Time).
    def next(after = Time.now) = @cron.next_time(after)
  end
end
