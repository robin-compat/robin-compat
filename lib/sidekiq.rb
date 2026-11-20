# frozen_string_literal: true

require "robin"
require "robin/compat/version"

# Sidekiq's interface, on top of Robin. Apps keep their Sidekiq code (job
# classes, configuration, tests, API calls) and run on Robin, which works
# on Sidekiq's Redis data where it is: no prefix on the keys a job's life
# runs through, the same job fields and units, the same process records.
module Sidekiq
  # The Sidekiq version whose interface this matches.
  VERSION = "8.1.0"
  NAME = "Sidekiq"

  Shutdown = Robin::Shutdown

  class << self
    def server? = Robin.server?

    def pro? = true

    def ent? = true

    # The configuration Sidekiq.configure_* yield (Sidekiq::Config).
    def default_configuration = @config ||= Config.new(Robin.config)

    # Runs the block with the configuration in a server process. Blocks are
    # kept, so an embedded server (configure_embed) runs them too.
    def configure_server(&block)
      server_blocks << block
      block.call(default_configuration) if server?
    end

    def configure_client
      yield default_configuration unless server?
    end

    # Runs a server inside this process; see Robin.configure_embed. The
    # configure_server blocks run first, then this one.
    def configure_embed(&block)
      Robin.configure_embed do |robin|
        config = Config.new(robin)
        config.concurrency = 2
        server_blocks.each { |b| b.call(config) }
        block&.call(config)
      end
    end

    def server_blocks = @server_blocks ||= []

    # Yields a connection that speaks the redis-client interface
    # (Sidekiq::RedisConnection::Connection).
    def redis(&) = default_configuration.redis(&)

    def redis_pool = default_configuration.redis_pool

    def redis_info = default_configuration.redis_info

    def logger = Robin.logger

    def logger=(logger)
      default_configuration.logger = logger
    end

    def dump_json(object) = Robin.dump_json(object)

    def load_json(string) = Robin.load_json(string)

    def strict_args!(mode = :raise)
      Robin.config.strict_args = mode
    end

    # Options every job class starts from (sidekiq_options adds to them).
    def default_job_options = Job.default_options

    def default_job_options=(options)
      Job.default_options = options
    end

    # Holds pushes made inside an Active Record transaction until it
    # commits (see Robin::Rails::TransactionalPush).
    def transactional_push!
      require "robin/rails/transactional_push"
      Robin.config.push_deferrer = Robin::Rails::TransactionalPush
    end

    # Sidekiq 8's testing switch: :fake, :inline or :disable.
    def testing!(mode = :fake, &)
      raise "Unknown testing mode: #{mode}" unless %i[fake inline disable].include?(mode.to_sym)

      require "sidekiq/testing"
      Testing.public_send(:"#{mode}!", &)
    end

    def freeze! = default_configuration

    def loader
    end
  end
end

require "sidekiq/redis_connection"
require "sidekiq/config"
require "sidekiq/capsule"
require "sidekiq/middleware/chain"
require "sidekiq/middleware/modules"
require "sidekiq/job"
require "sidekiq/client"
require "sidekiq/logger"
require "sidekiq/job_retry"
require "sidekiq/iterable_job"

module Sidekiq
  # Gives a Robin config Sidekiq's defaults where it still has Robin's:
  # Sidekiq's Redis layout (the job keys unprefixed, Robin's own under
  # "robin:"), its 25 retries, and its backoff: count**4 + 15 seconds (or
  # what sidekiq_retry_in says) plus up to 10 * (count + 1) seconds of
  # jitter, where the count is 0 after the first failure. Worker threads
  # get Thread.current[:sidekiq_capsule].
  def self.defaults!(config)
    config.event_handlers << CAPSULE_TRACKER unless config.event_handlers.include?(CAPSULE_TRACKER)
    if config.key_prefix == "robin:"
      config.internal_key_prefix = "robin:"
      config.key_prefix = ""
    end
    config.max_retries = 25 if config.max_retries == 20
    config.retry_backoff ||= Job::RETRY_BACKOFF
    # With sidekiq/batch loaded, batch callbacks get Sidekiq's status.
    if defined?(Batch::Status) && config.batch_status_class == Robin::Batch::Status
      config.batch_status_class = Batch::Status
    end
    config
  end
end

module Sidekiq
  # Sets up Rails integration when this is a Rails app (see Sidekiq::Rails).
  def self.integrate_rails
    require "sidekiq/rails" if defined?(::Rails::Engine)
  end
end

Sidekiq.defaults!(Robin.config)
Sidekiq.integrate_rails
