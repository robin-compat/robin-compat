# frozen_string_literal: true

require "delegate"

module Sidekiq
  # What Sidekiq.configure_server and friends yield: Robin's configuration
  # with Sidekiq's names. Anything it doesn't translate goes straight to
  # the Robin::Config underneath (+robin+).
  class Config < SimpleDelegator
    # config[:name] keys and the Robin settings they read and write.
    SETTINGS = {
      concurrency: :concurrency, timeout: :timeout, max_retries: :max_retries,
      dead_max_jobs: :dead_max_jobs, dead_timeout_in_seconds: :dead_timeout,
      average_scheduled_poll_interval: :poll_interval, poll_interval_average: :poll_interval,
      backtrace_cleaner: :backtrace_cleaner, max_iteration_runtime: :max_iteration_runtime,
      labels: :labels, tag: :tag, environment: :environment, queues: :queues,
      error_handlers: :error_handlers, death_handlers: :death_handlers
    }.freeze

    # Redis options Sidekiq takes that Robin names differently (or, like
    # redis-client's driver choice, doesn't need).
    REDIS_RENAMES = {size: :pool_size, network_timeout: :timeout}.freeze
    REDIS_IGNORED = %i[driver reconnect_attempts role protocol id].freeze

    def initialize(robin)
      super
      @extra = {}
    end

    def robin = __getobj__

    # -- Sidekiq's hash access -----------------------------------------------

    def [](key)
      setting = SETTINGS[key.to_sym]
      setting ? public_send(setting) : @extra[key.to_sym]
    end

    def []=(key, value)
      setting = SETTINGS[key.to_sym]
      setting ? public_send(:"#{setting}=", value) : @extra[key.to_sym] = value
    end

    def fetch(key, *default, &)
      key?(key) ? self[key] : @extra.fetch(key.to_sym, *default, &)
    end

    def key?(key) = SETTINGS.key?(key.to_sym) || @extra.key?(key.to_sym)

    def merge!(hash)
      hash.each { |key, value| self[key] = value }
      self
    end

    def dig(*keys) = keys.drop(1).reduce(self[keys.first]) { |value, key| value&.dig(key) }

    # -- settings with other names ----------------------------------------------

    def average_scheduled_poll_interval=(seconds)
      self.poll_interval = seconds
    end

    def strict_args!(mode = :raise)
      self.strict_args = mode
    end

    # Silences logging when set to nil, as Sidekiq does.
    def logger=(logger)
      robin.logger = logger || ::Logger.new(nil)
    end

    def default_capsule(&)
      capsule("default", &)
    end

    # Sidekiq's redis options, translated: +size+ is the pool size,
    # +network_timeout+ every timeout, +name+ the Sentinel master's name
    # when +sentinels+ are given.
    def redis=(options)
      options = options.transform_keys(&:to_sym)
      robin_options = options.except(*REDIS_IGNORED, :name, *REDIS_RENAMES.keys)
      if (size = options[:size])
        robin_options[:pool_size] = size
      end
      if (timeout = options[:network_timeout] || options[:timeout])
        robin_options.delete(:timeout)
        robin_options.merge!(connect_timeout: timeout, read_timeout: timeout, write_timeout: timeout)
      end
      if options[:sentinels] && (name = options[:name])
        robin_options[:master_name] = name
      end
      robin.redis = robin_options
    end

    def redis_pool = RedisConnection::Pool.new(robin.redis_pool)

    def redis
      robin.redis { |conn| yield RedisConnection::Connection.new(conn) }
    end

    def redis_info = redis(&:info)

    # Middleware chains with Sidekiq's extra methods.
    def server_middleware
      chain = Middleware::Chain.new(robin.server_middleware)
      yield chain if block_given?
      chain
    end

    def client_middleware
      chain = Middleware::Chain.new(robin.client_middleware)
      yield chain if block_given?
      chain
    end

    # Lifecycle events. :beat runs on every heartbeat; :heartbeat on the
    # first one only (Sidekiq also runs it after Redis comes back).
    def on(event, &block)
      case event.to_sym
      when :beat then robin.on(:heartbeat, &block)
      when :heartbeat
        fired = false
        robin.on(:heartbeat) do
          next if fired

          fired = true
          block.call
        end
      else robin.on(event.to_sym, &block)
      end
    end

    def reap_idle_redis_connections(timeout = nil)
      robin.redis = redis_options.merge(idle_timeout: timeout) if timeout
    end

    def thread_priority=(_priority)
    end

    # -- Sidekiq Pro and Enterprise -----------------------------------------

    # Robin's fetch is always reliable, with crash recovery and poison
    # pill detection.
    def super_fetch!(&)
    end

    # Robin's scheduler always moves due jobs atomically.
    def reliable_scheduler!
    end

    # Robin keeps hourly metrics for config.metrics_retention_days.
    def retain_history(days = nil, &)
      self.metrics_retention_days = days if days
    end

    def dogstatsd=(factory)
      require "sidekiq/pro/metrics"
      Pro::Metrics.install(robin, factory)
    end

    def periodic(&block)
      require "sidekiq/periodic"
      robin.periodic { |registry| block.call(Periodic::Manager.new(registry)) }
    end

    def to_s = "#<Sidekiq::Config #{robin}>"
    alias_method :inspect, :to_s
  end
end
