# frozen_string_literal: true

module Sidekiq
  # Sidekiq's capsules are Robin's (what config.capsule yields), with
  # Sidekiq's other readers. In a worker thread,
  # Thread.current[:sidekiq_capsule] is the capsule it works for.
  Capsule = Robin::Capsule

  module CapsuleMethods
    # How its queues are checked: :strict (in order, no weights), :random
    # (every weight 1) or :weighted.
    def mode
      if strict?
        :strict
      elsif weights.values.all? { |weight| weight == 1 }
        :random
      else
        :weighted
      end
    end

    def to_h = {concurrency: concurrency, mode: mode, weights: weights}

    # Robin's capsules share their config's Redis pool.
    def redis_pool = RedisConnection::Pool.new(config.redis_pool)

    def redis(&) = redis_pool.with(&)

    def logger = config.logger
  end
  Robin::Capsule.include(CapsuleMethods)

  # Sets Thread.current[:sidekiq_capsule] in each worker thread as it
  # starts (see Sidekiq.defaults!).
  CAPSULE_TRACKER = lambda do |event, _payload|
    Thread.current[:sidekiq_capsule] = Robin::Capsule.current if event == "worker.started"
  end
end
