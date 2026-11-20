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

  # The Sidekiq versions Sidekiq.compatible_with takes.
  COMPATIBLE = Gem::Requirement.new(">= 7.0", "< 8.2")

  class << self
    # The Sidekiq version the app was written for, which decides where
    # Sidekiq's versions disagree: "8.1" unless set (to "7.0", say, or
    # "7.2.4"). Before 8.0, a job records its times in epoch seconds
    # (Floats) and an Active Job names Sidekiq 7's wrapper class, so
    # Sidekiq 7 processes sharing the Redis can run Robin's jobs, and Pro's
    # StatsD metrics have no "sidekiq." prefix. Before 7.3, a Hash of
    # arguments in a sidekiq-scheduler schedule has its keys symbolized (as
    # sidekiq-scheduler 5 does, with Active Support), and before 7.1,
    # push_bulk leaves out the jobs middleware stopped.
    def compatible_with = @compatible_with&.to_s || "8.1"

    def compatible_with=(version)
      parsed = Gem::Version.new(version) if Gem::Version.correct?(version)
      raise ArgumentError, "robin-compat follows Sidekiq 7.0 to 8.1, not #{version.inspect}" unless parsed && COMPATIBLE.satisfied_by?(parsed)

      @compatible_with = parsed
      Robin.config.job_timestamps = compatible_before?("8.0") ? :seconds : :milliseconds
    end

    # Whether the app was written for a Sidekiq older than +version+.
    def compatible_before?(version) = @compatible_with ? @compatible_with < version : false

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
  # sidekiq-cron's interface (see sidekiq/cron.rb), loaded when it's used.
  autoload :Cron, "sidekiq/cron"

  # A server runs sidekiq-cron's jobs (Cron.boot) if the app uses
  # Sidekiq::Cron, has its schedule file or has its jobs in Redis.
  CRON_START = -> { Cron.boot(Robin.config) if cron? }

  # mutant:disable -- whether Sidekiq::Cron is loaded yet, which it always
  # is once the tests have loaded it
  def self.cron? = !autoload?(:Cron) || cron_found?

  # Whether sidekiq-cron's schedule file or jobs (in its default
  # namespace, or from before namespaces) are there.
  def self.cron_found?
    return true if %w[config/schedule.yml config/schedule.yaml].any? { |file| File.exist?(file) }

    prefix = Robin.config.key_prefix
    redis { |c| c.call("EXISTS", "#{prefix}cron_jobs:default", "#{prefix}cron_jobs") }.positive?
  end

  # sidekiq-scheduler's interface (see sidekiq-scheduler.rb), loaded when
  # it's used: Sidekiq::Scheduler, SidekiqScheduler, or these methods.
  autoload :Scheduler, "sidekiq/scheduler"
  ::Object.autoload :SidekiqScheduler, "sidekiq-scheduler"

  # Sidekiq.schedule= and the rest, until sidekiq-scheduler.rb (loaded by
  # the first call) gives Sidekiq the real ones.
  module ScheduleLoader
    %i[schedule schedule= schedule! reload_schedule! get_schedule get_all_schedules set_schedule remove_schedule].each do |name|
      define_method(name) do |*args|
        require "sidekiq-scheduler"
        public_send(name, *args)
      end
    end
  end
  private_constant :ScheduleLoader
  extend ScheduleLoader

  # A server runs sidekiq-scheduler's jobs (SidekiqScheduler.boot) if the
  # app uses its interface or config/sidekiq.yml has a :scheduler: section.
  SCHEDULER_START = -> { SidekiqScheduler.boot(default_configuration) if scheduler? }

  # mutant:disable -- whether SidekiqScheduler is loaded yet, which it
  # always is once the tests have loaded it
  def self.scheduler? = !::Object.autoload?(:SidekiqScheduler) || scheduler_configured?

  def self.scheduler_configured? = !default_configuration[:scheduler].nil?

  # Gives a Robin config Sidekiq's defaults where it still has Robin's:
  # Sidekiq's Redis layout (the job keys unprefixed, Robin's own under
  # "robin:"), its 25 retries, and its backoff: count**4 + 15 seconds (or
  # what sidekiq_retry_in says) plus up to 10 * (count + 1) seconds of
  # jitter, where the count is 0 after the first failure. Worker threads
  # get Thread.current[:sidekiq_capsule], and servers run sidekiq-cron's
  # and sidekiq-scheduler's jobs (see CRON_START and SCHEDULER_START).
  def self.defaults!(config)
    config.event_handlers << CAPSULE_TRACKER unless config.event_handlers.include?(CAPSULE_TRACKER)
    [CRON_START, SCHEDULER_START].each do |start|
      config.on(:startup, &start) unless config.lifecycle.fetch(:startup).include?(start)
    end
    if config.key_prefix == "robin:"
      config.internal_key_prefix = "robin:"
      config.key_prefix = ""
    end
    config.max_retries = 25 if config.max_retries == 20
    config.job_timestamps = :seconds if compatible_before?("8.0")
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
