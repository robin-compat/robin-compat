# frozen_string_literal: true

# Sidekiq Enterprise's interface: rate limiters, unique jobs, periodic
# jobs, encryption, leader election and multi-process servers. Robin has
# all of these built in.
require "sidekiq-pro"
require "sidekiq/limiter"
require "sidekiq/periodic"

module Sidekiq
  module Enterprise
    # Robin's unique jobs are always on (unique_for: in sidekiq_options).
    def self.unique!(*)
    end

    # SIDEKIQ_LEADER=false keeps the process out of leader elections.
    def self.leader_from_env(config = Robin.config, env = ENV)
      config.leader_candidate = false if env["SIDEKIQ_LEADER"] == "false"
    end

    # Unique-job helpers.
    module Unique
      # The jid holding the unique lock for a job of +klass+ with +args+,
      # or nil: locked?(queue = nil, klass, args). The queue defaults to
      # the class's.
      def self.locked?(*query)
        klass, args = query.last(2)
        name = klass.to_s
        queue = (query.size > 2) ? query.first : default_queue(name)
        digest = Robin::Unique.digest(name, queue, args)
        Robin.redis { |c| c.call("GET", Robin::Unique.lock_key(Robin.config, digest)) }
      end

      # mutant:disable -- `const_get` on Unique instead of Object differs
      # only for constants nested in Unique, none of which are job classes.
      def self.default_queue(name) = Object.const_get(name).get_sidekiq_options.fetch("queue")
    end

    # Encrypted arguments: the block returns the key (32 bytes) for each
    # version; the active version encrypts, older ones still decrypt.
    # Older versions whose key the block can't give (nil, or an error such
    # as a key file that's gone) are left out.
    module Crypto
      def self.enable(active_version:, config: Robin.config, &block)
        keys = {active_version => block.call(active_version)}
        (1...active_version).each do |version|
          key = begin
            block.call(version)
          rescue
            nil
          end
          keys[version] = key if key
        end
        config.encryption_keys = keys
      end
    end
  end

  # Helpers for an app's own long-running components (a class that sets
  # @config to a Sidekiq::Config or Robin::Config, or uses Robin's).
  module Component
    def config = @config || Sidekiq.default_configuration

    def logger = config.logger

    def redis(&) = Sidekiq.redis(&)

    # Whether this process leads the cluster.
    def leader?
      robin = config.respond_to?(:robin) ? config.robin : config
      Robin::Leader.leader?(robin)
    end

    # Reports an error with the configured error handlers.
    def handle_exception(exception, context = {}) = config.handle_exception(exception, context)

    # A thread named +name+ whose errors are reported rather than lost.
    def safe_thread(name, &block)
      Thread.new do
        Thread.current.name = "sidekiq.#{name}"
        block.call
      rescue Exception => e # rubocop:disable Lint/RescueException
        handle_exception(e, {context: "Exception in #{name} thread"})
        raise
      end
    end
  end
end

Sidekiq::Enterprise.leader_from_env
