# frozen_string_literal: true

require "socket"
require "sidekiq"

module Sidekiq
  module Pro
    # Sidekiq Pro's StatsD metrics, sent through the app's dogstatsd
    # client with Sidekiq Pro's names (all prefixed "sidekiq."):
    #
    #   config.dogstatsd = -> { Datadog::Statsd.new("localhost", 8125) }
    #   config.server_middleware { |chain| chain.add Sidekiq::Middleware::Server::Statsd }
    #
    # config[:use_datadog_extensions] = false leaves out the *_dist
    # (Datadog distribution) metrics.
    module Metrics
      # Robin's event names and the names Sidekiq Pro sends them under.
      NAMES = {
        "jobs.recovered" => "jobs.recovered.fetch", "batch.duration" => "batches.duration",
        "batch.duration_dist" => "batches.duration_dist"
      }.freeze

      @lock = Mutex.new

      class << self
        # The dogstatsd client (made by the factory on first use).
        # mutant:disable -- the lock only matters to threads racing to
        # make the client, which a test can't stage deterministically.
        def client = @lock.synchronize { @client ||= @factory&.call }

        def install(config, factory)
          @factory = factory
          @client = nil
          config.statsd = Events.new
        end

        def distributions? = Sidekiq.default_configuration.fetch(:use_datadog_extensions, true)

        # Sends a metric with the client, if there is one. Like Robin's
        # own StatsD client, network trouble never raises into the code it
        # measures.
        def emit(method, *args, **options)
          client&.public_send(method, *args, **options)
          nil
        rescue IOError, SystemCallError, SocketError
          nil
        end
      end

      # Robin's StatsD events (batches, recovered and poison pill jobs...)
      # forwarded to the dogstatsd client under Sidekiq Pro's names. Job
      # counts and timings come from Middleware::Server::Statsd instead.
      class Events
        def dogstatsd? = true

        def distributions? = Metrics.distributions?

        def increment(name, value = 1, tags: {}) = Metrics.emit(:increment, metric(name), by: value, tags: tags(tags))

        def gauge(name, value, tags: {}) = Metrics.emit(:gauge, metric(name), value, tags: tags(tags))

        def timing(name, ms, tags: {}) = Metrics.emit(:timing, metric(name), ms, tags: tags(tags))

        def distribution(name, value, tags: {})
          return if name == "jobs.perform_dist" # sent by the middleware, with Sidekiq's tags

          Metrics.emit(:distribution, metric(name), value, tags: tags(tags))
        end

        def job_metrics(_jobs)
        end

        def queue_metrics(_queues)
        end

        def close
        end

        private

        def metric(name) = "sidekiq.#{NAMES.fetch(name, name)}"

        def tags(tags) = tags.flat_map { |key, value| Array(value).map { |v| "#{key}:#{v}" } }
      end
    end
  end

  module Middleware
    module Server
      # Counts and times jobs: sidekiq.jobs.count, .success, .failure,
      # .perform (ms) and .perform_dist, tagged worker:<class> and
      # queue:<queue> (Statsd.options may supply other options).
      class Statsd
        include Robin::Middleware::Stateless

        class << self
          # A callable (class name, job, queue) returning the options sent
          # with each metric (tags:, sample_rate:...).
          attr_accessor :options
        end

        def call(_job, payload, queue)
          started = monotonic
          ok = false
          result = yield
          ok = true
          result
        ensure
          report(payload, queue, ok, (monotonic - started) * 1000)
        end

        private

        def monotonic = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

        def report(payload, queue, ok, ms)
          klass = Robin.display_class(payload)
          options = self.class.options&.call(klass, payload, queue) || {tags: ["worker:#{klass}", "queue:#{queue}"]}
          Pro::Metrics.emit(:increment, "sidekiq.jobs.count", **options)
          Pro::Metrics.emit(:increment, ok ? "sidekiq.jobs.success" : "sidekiq.jobs.failure", **options)
          Pro::Metrics.emit(:timing, "sidekiq.jobs.perform", ms, **options)
          Pro::Metrics.emit(:distribution, "sidekiq.jobs.perform_dist", ms, **options) if Pro::Metrics.distributions?
        rescue => e # from Statsd.options; the job's outcome stands
          Robin.config.handle_exception(e, {context: "statsd options", job: payload})
        end
      end
    end
  end
end
