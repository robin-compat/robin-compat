# frozen_string_literal: true

require_relative "test_helper"

class ConfigTest < RobinCompatTest
  cover "Sidekiq::Config*"

  def sidekiq = @sidekiq ||= Sidekiq::Config.new(config)

  def test_wraps_and_delegates
    assert_same config, sidekiq.robin
    sidekiq.concurrency = 7

    assert_equal 7, config.concurrency
    assert_equal "#<Sidekiq::Config #{config}>", sidekiq.to_s
    assert_equal sidekiq.to_s, sidekiq.inspect
  end

  def test_hash_access
    sidekiq[:max_retries] = 7
    sidekiq["dead_timeout_in_seconds"] = 60
    sidekiq[:average_scheduled_poll_interval] = 3
    sidekiq[:custom] = "x"

    assert_equal [7, 60, 3], [config.max_retries, config.dead_timeout, config.poll_interval]
    assert_equal [7, 60, 3, "x", nil], [sidekiq[:max_retries], sidekiq[:dead_timeout_in_seconds], sidekiq["average_scheduled_poll_interval"],
      sidekiq[:custom], sidekiq[:nothing]]
    assert_true sidekiq.key?(:timeout)
    assert_true sidekiq.key?("custom")
    assert_false sidekiq.key?(:nothing)
    assert_equal 7, sidekiq.fetch(:max_retries)
    assert_equal "x", sidekiq.fetch(:custom)
    assert_equal :d, sidekiq.fetch(:nothing, :d)
    assert_equal :b, sidekiq.fetch(:nothing) { :b }
    assert_raises(KeyError) { sidekiq.fetch(:nothing) }
    assert_same sidekiq, sidekiq.merge!(tag: "t", labels: ["l"])
    assert_equal ["t", ["l"]], [config.tag, config.labels]
    sidekiq[:nested] = {"a" => {"b" => 1}}

    assert_equal 1, sidekiq.dig(:nested, "a", "b")
    assert_nil sidekiq.dig(:nothing, "a")
  end

  def test_keys_may_be_strings_or_symbols
    sidekiq["stringly"] = "y"

    assert_equal "y", sidekiq[:stringly]
    assert_equal "y", sidekiq["stringly"]
    assert_true sidekiq.key?("timeout")
    assert_equal "key not found: :nothing", assert_raises(KeyError) { sidekiq.fetch("nothing") }.message
  end

  def test_settings_with_other_names
    sidekiq.average_scheduled_poll_interval = 9
    sidekiq.strict_args!

    assert_equal [9, :raise], [config.poll_interval, config.strict_args]
    sidekiq.strict_args!(false)

    assert_false config.strict_args
    logger = Logger.new(nil)
    sidekiq.logger = logger

    assert_same logger, config.logger
    sidekiq.logger = nil

    assert_instance_of Logger, config.logger
    assert_nil config.logger.instance_variable_get(:@logdev), "logs nowhere"
    assert_same config.logger, Robin.config.logger
  end

  def test_default_capsule
    sidekiq.default_capsule { |c| c.concurrency = 3 }

    assert_equal 3, config.default_capsule.concurrency
  end

  def test_redis_options_are_translated
    sidekiq.redis = {url: "redis://h:1/2", size: 9, network_timeout: 4, driver: :hiredis, reconnect_attempts: 2,
                     role: :master, protocol: 3, id: "x", pool_timeout: 7}

    assert_equal({url: "redis://h:1/2", pool_size: 9, connect_timeout: 4, read_timeout: 4, write_timeout: 4, pool_timeout: 7},
      config.redis_options)
    sidekiq.redis = {"url" => "redis://h", "timeout" => 2, "name" => "ignored"}

    assert_equal({url: "redis://h", connect_timeout: 2, read_timeout: 2, write_timeout: 2}, config.redis_options)
    sidekiq.redis = {sentinels: [{host: "s1"}], name: "mymaster"}

    assert_equal({sentinels: [{host: "s1"}], master_name: "mymaster"}, config.redis_options)
    sidekiq.redis = {sentinels: ["s1:1"]}

    assert_equal({sentinels: ["s1:1"]}, config.redis_options)
    sidekiq.redis = {sentinels: ["s1:1"], name: nil}

    assert_equal({sentinels: ["s1:1"]}, config.redis_options)
    sidekiq.redis = {url: "redis://h", sentinels: nil, name: "ignored"}

    assert_equal({url: "redis://h", sentinels: nil}, config.redis_options)
  ensure
    config.redis = {url: REDIS_URL}
  end

  def test_redis
    assert_equal "PONG", sidekiq.redis(&:ping)
    assert_same config.redis_pool, sidekiq.redis_pool.pool
    assert sidekiq.redis_info.key?("redis_version")
  end

  def test_middleware_chains
    a = Class.new
    chain = sidekiq.server_middleware { |c| c.add a }

    assert_instance_of Sidekiq::Middleware::Chain, chain
    assert config.server_middleware.exists?(a)
    assert_same config.client_middleware, sidekiq.client_middleware { |c| c.add a }.__getobj__
    assert config.client_middleware.exists?(a)
    assert_instance_of Sidekiq::Middleware::Chain, sidekiq.server_middleware
    assert_same config.client_middleware, sidekiq.client_middleware.__getobj__
  end

  def test_events
    seen = []
    sidekiq.on(:beat) { seen << :beat }
    sidekiq.on(:heartbeat) { seen << :heartbeat }
    sidekiq.on("startup") { seen << :startup }
    sidekiq.on("beat") { seen << :beat2 }
    3.times { config.fire_event(:heartbeat, reraise: true) }
    config.fire_event(:startup)

    assert_equal %i[beat heartbeat beat2 beat beat2 beat beat2 startup], seen
  end

  def test_idle_connections_and_thread_priority
    config.redis = {url: REDIS_URL, name: "robin-test"}
    sidekiq.reap_idle_redis_connections(30)

    assert_equal({url: REDIS_URL, name: "robin-test", idle_timeout: 30}, config.redis_options)
    sidekiq.reap_idle_redis_connections

    assert_equal 30, config.redis_options[:idle_timeout]
    sidekiq.thread_priority = -1 # Robin's threads keep their priorities

    assert_respond_to sidekiq, :thread_priority=
  end

  def test_pro_and_enterprise_settings
    assert_nil sidekiq.super_fetch!
    assert_nil(sidekiq.super_fetch! { |_job, _pill| nil })
    assert_nil sidekiq.reliable_scheduler!
    sidekiq.retain_history(30)

    assert_equal 30, config.metrics_retention_days
    sidekiq.retain_history { nil }

    assert_equal 30, config.metrics_retention_days
  end

  def test_periodic
    managers = []
    paths = recording_requires(sidekiq) do
      sidekiq.periodic { |mgr| (managers << mgr) && mgr.register("0 * * * *", "HourlyJob", retry: 1) }
    end

    assert_equal ["sidekiq/periodic"], paths
    registry = Robin::Periodic::Registry.new
    config.periodic_blocks.each { |block| block.call(registry) }
    entry = registry.instance_variable_get(:@entries).values.first

    assert_equal ["0 * * * *", "HourlyJob", {"retry" => 1}], entry.values_at("cron", "class", "options")
    assert_instance_of Sidekiq::Periodic::Manager, managers.first
  end

  def test_dogstatsd
    paths = recording_requires(sidekiq) { sidekiq.dogstatsd = -> { :client } }

    assert_equal ["sidekiq/pro/metrics"], paths

    assert_instance_of Sidekiq::Pro::Metrics::Events, config.statsd
    assert_equal :client, Sidekiq::Pro::Metrics.client
  end

  # Sidekiq 7's handlers take two parameters, Sidekiq 8's three.
  def test_error_handlers_with_two_or_three_parameters
    log = StringIO.new
    config.logger = Logger.new(log)
    seen = []
    callable = Object.new
    callable.define_singleton_method(:call) { |ex, context| seen << [:object, ex.message, context] }
    config.error_handlers.replace([
      ->(ex, context) { seen << [:two, ex.message, context] },
      ->(ex, *rest) { seen << [:splat, ex.message, rest] },
      callable,
      ->(*) { raise ArgumentError, "boom" },
      ->(ex, context, cfg) { seen << [:three, ex.message, context, cfg] },
      Class.new(Proc).new { |ex, context| seen << [:subclass, ex.message, context] },
      proc { |ex| seen << [:one, ex.message] }
    ])
    config.handle_exception(RuntimeError.new("x"), {a: 1})

    sidekiq7 = {_config: config, a: 1}

    assert_equal [[:two, "x", sidekiq7], [:splat, "x", [sidekiq7]], [:object, "x", sidekiq7],
      [:three, "x", {a: 1}, config], [:subclass, "x", sidekiq7], [:one, "x"]], seen
    assert_includes log.string, "!!! error handler raised: ArgumentError: boom"
    seen.clear
    Sidekiq.default_configuration.handle_exception(RuntimeError.new("y"))

    assert_equal [:two, "y", {_config: config}], seen.first
  end
end
