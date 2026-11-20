# frozen_string_literal: true

require "delegate"
require "time"
require "sidekiq"

module Sidekiq
  # Deploy marks, as Robin::Deploy's: the dashboard's metrics charts draw a
  # line at each.
  #
  #   require "sidekiq/deploy"
  #   Sidekiq::Deploy.mark!("v42")
  #   Sidekiq::Deploy.mark!  # labelled with the latest git commit
  #
  # A label marked in the last hour isn't marked again, so every process
  # may mark the same deploy (at boot, say).
  class Deploy
    # Robin keeps marks as long as its hourly metrics
    # (Config#metrics_retention_days, 90 days by default) instead.
    MARK_TTL = 90 * 24 * 60 * 60

    # The label of a mark made without one: the latest commit's short hash
    # and subject.
    LABEL_MAKER = -> { `git log -1 --format="%h %s"`.strip }

    def self.mark!(label = nil) = new.mark!(label: label)

    # Marks go through +pool+ (anything whose #with yields a connection,
    # such as Sidekiq.redis_pool), or Robin's own.
    def initialize(pool = nil)
      @config = pool ? Through.new(Robin.config, pool) : Robin.config
    end

    # Marks a deploy at +at+. Returns false when +label+ was already marked
    # in the last hour, true otherwise.
    def mark!(at: Time.now, label: nil)
      Robin::Deploy.mark!(label || LABEL_MAKER.call, at: at, config: @config)
    end

    # The marks made on +date+ (a Date or Time, in UTC): the time of each,
    # to the minute in ISO 8601, and its label. The first of a minute's
    # marks is kept.
    def fetch(date = Time.now.utc)
      day = Time.utc(date.year, date.month, date.day)
      marks = Robin::Deploy.list(from: day, to: day + Rational(86_399_999, 1000), config: @config)
      marks.each_with_object({}) do |mark, found|
        found[Time.at(mark.fetch("at") / 60_000 * 60).utc.iso8601] ||= mark.fetch("label").to_s
      end
    end

    # Robin's configuration, with Redis reached through another pool.
    class Through < SimpleDelegator
      def initialize(config, pool)
        super(config)
        @pool = pool
      end

      def redis(&) = @pool.with(&)
    end
    private_constant :Through
  end
end
