# frozen_string_literal: true

require "sidekiq"
require "robin/cli"

module Sidekiq
  # The `sidekiq` command: Robin's server, reading config/sidekiq.yml
  # (with Sidekiq's settings) and taking Sidekiq's options.
  #
  #   bundle exec sidekiq -C config/sidekiq.yml -e production
  class CLI
    # Settings in config/sidekiq.yml the server itself reads (or that
    # Sidekiq no longer uses); the rest go into the configuration
    # (config[:max_retries] and so on).
    SERVER_SETTINGS = %i[concurrency queues require timeout tag environment processes health_port capsules
      verbose pidfile logfile strict].freeze

    # Sidekiq's application environment comes from the first of these,
    # else it's development.
    ENVIRONMENT_VARIABLES = %w[APP_ENV RAILS_ENV RACK_ENV].freeze

    def self.instance = @instance ||= new

    # Keeps the command line for #run, as Sidekiq's CLI#parse does.
    def parse(argv = ARGV)
      @argv = argv.dup
    end

    # Runs a server, as `sidekiq` does. Sidekiq's default concurrency is
    # RAILS_MAX_THREADS when that's set, and its default environment
    # development (so the config file's development section applies).
    # Sidekiq's own keyword options (boot_app:...) don't apply.
    def run(argv = @argv || ARGV, env: ENV, **)
      threads = env["RAILS_MAX_THREADS"]
      Robin.config.concurrency = Integer(threads) if threads
      env["RACK_ENV"] = "development" unless env.values_at(*ENVIRONMENT_VARIABLES).any?
      Robin::CLI.new(argv).run
    end

    # Runs a swarm, as Sidekiq Enterprise's `sidekiqswarm` does:
    # SIDEKIQ_COUNT processes (default: one per CPU core), each restarted
    # above SIDEKIQ_MAXMEM_MB megabytes of memory. The app is loaded once,
    # before forking.
    def swarm(argv = ARGV, env: ENV)
      limit = env["SIDEKIQ_MAXMEM_MB"]
      Robin.config.memory_limit_mb = Integer(limit) if limit
      count = env["SIDEKIQ_COUNT"]
      run(["swarm", *(["-n", count] if count), *argv], env:)
    end

    # Applies the config file's other settings (see SERVER_SETTINGS), and
    # its verbose setting. Null values leave defaults alone. Hashes are
    # environment sections or capsules, except :scheduler:
    # (sidekiq-scheduler's settings). Settings Robin has no name for are
    # kept in Sidekiq.default_configuration when it's +config+'s.
    def self.apply_settings(options, config)
      sidekiq = Sidekiq.default_configuration
      sidekiq = Config.new(config) unless sidekiq.robin.equal?(config)
      options.each do |key, value|
        next if SERVER_SETTINGS.include?(key) || value.nil? || (value.instance_of?(Hash) && !key.equal?(:scheduler))

        sidekiq[key] = value
      end
      config.logger.level = ::Logger::DEBUG if options[:verbose]
    end
  end
end

Robin::CLI.config_paths.unshift("config/sidekiq.yml", "config/sidekiq.yml.erb")
Robin::CLI.config_file_hooks << ->(options, config) { Sidekiq::CLI.apply_settings(options, config) }
