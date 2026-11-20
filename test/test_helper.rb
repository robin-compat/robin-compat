# frozen_string_literal: true

# The same conventions as the robin gem's test/test_helper.rb.
if ENV["COVERAGE"] != "false" && RUBY_ENGINE == "ruby" && !defined?(Mutant)
  require "simplecov"
  SimpleCov.start do
    root File.expand_path("..", __dir__)
    enable_coverage :branch
    enable_coverage :method
    skip %r{\A/test/}
    # Enforced for full runs (CI, `rake test`), not when running one file.
    minimum_coverage line: 100, branch: 100, method: 100 if ENV["CI"] || ENV["COVERAGE_STRICT"]
  end
end

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "sidekiq"
require "minitest/autorun"
# Assertions that take only true and false for true and false (see
# minitest-strict).
require "minitest/strict"

begin
  # `cover "Sidekiq::Thing*"` tells mutant which tests exercise which code.
  require "mutant/minitest/coverage"
rescue LoadError
  # mutant is MRI-only; on other engines coverage declarations are no-ops.
  module Minitest
    class Test
      def self.cover(_expression) = nil
    end
  end
end

# assert_roundtrips (test/support/roundtrips.rb), as in the robin gem's tests.
require_relative "support/roundtrips"
Minitest::Test.include(RoundTrips::Assertions)

# assert_allocations (minitest-memory) pins how many objects a path may
# allocate. Only CRuby traces allocations: elsewhere the block runs, and
# the test is skipped.
require "minitest/memory"
Minitest::Test.include(Minitest::Memory)

REDIS_URL = ENV.fetch("ROBIN_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")

module RobinCompatTestHelpers
  # Every process gets its own key prefix, so parallel test runs (and
  # mutant's forked workers) never see or clear each other's data. The
  # job keys sit right under it, as Sidekiq's sit at the top level, and
  # Robin's own under "robin:".
  def key_prefix_for_test = "robincompattest:#{Process.pid}:"

  def setup
    super
    Robin.config = Robin::Config.new
    Robin.config.redis = {url: REDIS_URL}
    Robin.config.key_prefix = key_prefix_for_test
    Robin.config.internal_key_prefix = "#{key_prefix_for_test}robin:"
    Robin.config.logger = Logger.new(nil)
    Sidekiq.instance_variable_set(:@compatible_with, nil)
    Sidekiq.defaults!(Robin.config)
    Sidekiq.instance_variable_set(:@config, nil)
    flush_redis
  end

  def teardown
    flush_redis
    Robin.config.redis_pool.shutdown
    super
  end

  def flush_redis
    Robin.redis do |c|
      cursor = "0"
      loop do
        cursor, keys = c.call("SCAN", cursor, "MATCH", "#{key_prefix_for_test}*", "COUNT", 1000)
        c.call("DEL", *keys) unless keys.empty?
        break if cursor == "0"
      end
    end
  end

  def config = Robin.config

  def redis(*) = Robin.redis { |c| c.call(*) }

  # The job keys as Sidekiq names them, under the test's prefix.
  def key(name) = "#{key_prefix_for_test}#{name}"

  # Adds a client middleware to +config+, returning the list it fills
  # with the Redis pool of each push (all pools share one Redis here, so
  # where a job went is told this way).
  # Runs the block with what +object+ requires recorded (and still
  # required); returns the paths. The files are loaded already here, so
  # this is how a lazy require is seen.
  def recording_requires(object)
    paths = []
    object.define_singleton_method(:require) { |path| (paths << path) && super(path) }
    yield
    paths
  ensure
    object.singleton_class.remove_method(:require)
  end

  def record_push_pools(config = self.config)
    pools = []
    recorder = Class.new { define_method(:call) { |_class, _job, _queue, pool, &blk| (pools << pool) && blk.call } }
    config.client_middleware { |c| c.add recorder }
    pools
  end
end

class RobinCompatTest < Minitest::Test
  include RobinCompatTestHelpers
end
