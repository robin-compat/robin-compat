# frozen_string_literal: true

require "sidekiq"
require "sidekiq/batch"

module Sidekiq
  # Sidekiq Enterprise's rate limiters, as Robin::Limiter's:
  #
  #   STRIPE = Sidekiq::Limiter.concurrent("stripe", 10, wait_timeout: 0, lock_timeout: 30)
  #   STRIPE.within_limit { charge! }
  #
  # Inside a job, an OverLimit reschedules the job instead of failing it,
  # after Sidekiq's backoff (about 5 minutes more each time) unless
  # Limiter.configure or the limiter gives another.
  module Limiter
    OverLimit = Robin::Limiter::OverLimit

    # Sidekiq's default backoff: 5 minutes for each time the job hit a
    # limit, plus up to 5 minutes of jitter.
    DEFAULT_BACKOFF = ->(_limiter, job, _exception) { (300 * job.fetch("overrated")) + rand(1..300) }

    # Stands in for the limiter when an error listed in
    # Limiter.configure's +errors+ is treated as hitting a limit.
    ErrorLimit = Struct.new(:name, :type, :reschedule, :backoff)

    # What Limiter.configure yields.
    class Configuration
      # Errors that count as hitting a rate limit (e.g. an API's "429").
      attr_reader :errors

      # The backoff for limiters without one of their own: a callable
      # (limiter, job, exception) returning seconds, where job["overrated"]
      # counts the times the job hit a limit.
      attr_accessor :backoff

      # The pool the limiters keep their state in (nil: Robin's own).
      attr_reader :pool

      def initialize
        @errors = []
        @backoff = DEFAULT_BACKOFF
      end

      # Keeps the limiters' state in another Redis (Sidekiq's redis
      # options).
      def redis=(options)
        config = Robin::Config.new
        Config.new(config).redis = options
        @pool = config.redis_pool
      end
    end

    class << self
      def configure
        yield configuration
        install_errors_middleware unless configuration.errors.empty?
      end

      def configuration = @configuration ||= Configuration.new

      def concurrent(name, count, **) = wrap(Robin::Limiter.concurrent(name, count, **options(**)))

      def bucket(name, count, interval, **) = wrap(Robin::Limiter.bucket(name, count, interval, **options(**)))

      def window(name, count, interval, **) = wrap(Robin::Limiter.window(name, count, interval, **options(**)))

      def leaky(name, count, interval, **) = wrap(Robin::Limiter.leaky(name, count, interval, **options(**)))

      def points(name, points, interval, **) = Points.new(Robin::Limiter.points(name, points, interval, **options(**)))

      def unlimited(name = "unlimited") = Robin::Limiter.unlimited(name)

      # A Sidekiq backoff (limiter, job, exception) as Robin takes it:
      # (count, error, job), the count being job["overrated"].
      def backoff(callable = nil)
        lambda do |count, error, job|
          (callable || configuration.backoff).call(error.limiter, job.merge({"overrated" => count}), error)
        end
      end

      # Runs the block with the limiters' calls going to the configured
      # Redis, if any (else where Robin::Limiter.via says, or Robin's).
      def with_redis(&)
        pool = configuration.pool
        pool ? Robin::Limiter.via(pool, &) : yield
      end

      private

      # Sidekiq's limiter options as Robin's: policy :ignore is :skip,
      # backoffs take (limiter, job, exception), and +ttl+ (how long an
      # unused limiter's data stays) is Robin's own, fixed one.
      def options(policy: :raise, backoff: nil, ttl: nil, **options)
        policy = policy.to_sym
        options.merge(policy: (policy == :ignore) ? :skip : policy, backoff: backoff(backoff))
      end

      def wrap(limiter) = Handle.new(limiter)

      # Inside Robin's batch tracking, so a batch doesn't count the error
      # as its job failing when the job is rescheduled instead.
      def install_errors_middleware
        Robin.config.server_middleware { |chain| chain.insert_after(Batch::Server, ErrorsMiddleware) }
      end
    end

    # Turns the errors listed in Limiter.configure's +errors+ into
    # OverLimit, so the job is rescheduled rather than failed.
    class ErrorsMiddleware
      include Robin::Middleware::Stateless

      def call(_job, _payload, _queue)
        yield
      rescue *Limiter.configuration.errors => e
        raise OverLimit.new(ErrorLimit.new(e.class.name, "error", nil, Limiter.backoff))
      end
    end

    # A limiter whose calls go to the Redis Limiter.configure set.
    class Handle < SimpleDelegator
      %i[within_limit used to_h reset_state over_limit!].each do |name|
        define_method(name) { |*args, **opts, &block| Limiter.with_redis { super(*args, **opts, &block) } }
      end
    end

    # A points limiter whose block reports what it actually used:
    #
    #   OPENAI.within_limit(estimate: 1_000) { |handle| handle.points_used(response.tokens) }
    class Points < Handle
      def within_limit(estimate: 1)
        super(points: estimate) { yield Usage.new(self, estimate) }
      end

      %i[adjust refund].each do |name|
        define_method(name) { |points| Limiter.with_redis { super(points) } }
      end

      # Settles the difference between the estimate and what was used.
      Usage = Struct.new(:limiter, :estimate) do
        def points_used(points) = limiter.adjust(points - estimate)
      end
    end
  end
end
