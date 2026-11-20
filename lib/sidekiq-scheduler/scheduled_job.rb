# frozen_string_literal: true

require "time"
require "robin/compat/cron_line"

module SidekiqScheduler
  # A job in the schedule, and when it's due:
  #
  #   cron: "0 * * * *"           as a cron line (an optional zone at its end)
  #   every: "45m"                at the multiples of 45 minutes since the
  #   interval: "45m"             Unix epoch (Robin's "@every 45m")
  #   at: "3001/01/01"            once, at that time (unless it's past)
  #   in: "1h"                    once, an hour after the scheduler started
  #
  # A value can be an Array of it and rufus-scheduler's options, which are
  # taken and unused (first_in, say: every and interval keep to their
  # multiples).
  class ScheduledJob
    TYPES = %w[cron every at in interval].freeze

    attr_reader :name, :config, :type

    # The job +name+ in +config+ (its schedule), scheduled at +now+, or nil
    # when it has no cron, every, at, in or interval.
    def self.build(name, config, now = Time.now)
      type = TYPES.find { |key| !config[key].to_s.empty? }
      new(name, config, type, now) if type
    end

    def initialize(name, config, type, now)
      @name = name
      @config = config
      @type = type
      @armed_at = Time.at(Integer(now))
      value, = config.fetch(type) # an Array's first (not a Time's #to_a)
      case type
      when "cron" then @recurring = Robin::Compat::CronLine.parse(value.to_s)
      when "every", "interval" then @recurring = Robin::Cron::Every.new("@every #{seconds(value)}s")
      when "at" then @at = at(value)
      else @at = @armed_at + Utils.duration(value)
      end
    end

    # The time the job is due for at +now+: its latest tick (not before it
    # was scheduled), or its one time once that's come. nil otherwise.
    def due(now)
      if @recurring
        tick = @recurring.match?(now) ? Time.at(Integer(now)) : @recurring.previous_time(now)
        tick if tick && tick >= @armed_at
      elsif @at && @at <= now
        @at
      end
    end

    # When it's due next after +now+ (nil for a one-time job that's done).
    def next_time(now = Time.now)
      return @recurring.next_time(now) if @recurring

      @at if @at && @at > now
    end

    # What it's scheduled by ("0 * * * *", "45m"...).
    def schedule = config.fetch(type)

    private

    def seconds(value)
      seconds = Utils.duration(value)
      raise ArgumentError, "#{name}: #{value.inspect} isn't a whole number of seconds" unless seconds.positive? && (seconds % 1).zero?

      Integer(seconds)
    end

    # nil when it's past: rufus-scheduler drops those.
    def at(value)
      time = value.is_a?(Time) ? value : Time.parse(value.to_s)
      time unless time < @armed_at
    end
  end
end
