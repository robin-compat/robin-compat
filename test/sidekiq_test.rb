# frozen_string_literal: true

require_relative "test_helper"

class SidekiqModuleTest < RobinCompatTest
  cover "Sidekiq."

  def teardown
    Sidekiq.server_blocks.clear
    Robin.instance_variable_set(:@server, false)
    super
  end

  def test_constants
    assert_equal "8.1.0", Sidekiq::VERSION
    assert_equal "Sidekiq", Sidekiq::NAME
    assert_same Robin::Shutdown, Sidekiq::Shutdown
    assert_true Sidekiq.pro?
    assert_true Sidekiq.ent?
  end

  def test_defaults_give_robin_sidekiqs_layout_and_retries
    c = Robin::Config.new

    assert_same c, Sidekiq.defaults!(c)
    assert_equal ["", "robin:", 25, Sidekiq::Job::RETRY_BACKOFF], [c.key_prefix, c.internal_key_prefix, c.max_retries, c.retry_backoff]
    assert_equal "queue:default", c.key("queue", "default")
    assert_equal "robin:inflight:x", c.key("inflight", "x")
    custom = Robin::Config.new
    backoff = ->(*) { 1 }
    custom.key_prefix = "app:"
    custom.max_retries = 3
    custom.retry_backoff = backoff
    Sidekiq.defaults!(custom)

    assert_equal ["app:", "app:", 3, backoff], [custom.key_prefix, custom.internal_key_prefix, custom.max_retries, custom.retry_backoff]
  end

  def test_configure_server_and_client
    seen = []
    Sidekiq.configure_client { |c| seen << [:client, c] }
    Sidekiq.configure_server { |c| seen << [:server, c] }

    assert_equal [[:client, Sidekiq.default_configuration]], seen
    assert_instance_of Sidekiq::Config, Sidekiq.default_configuration
    assert_same config, Sidekiq.default_configuration.robin
    assert_same Sidekiq.default_configuration, Sidekiq.default_configuration
    Robin.server!
    Sidekiq.configure_server { |c| seen << [:server2, c] }
    Sidekiq.configure_client { |c| seen << [:client2, c] }

    assert_equal %i[client server2], seen.map(&:first)
    assert_same Sidekiq.default_configuration, seen.last.last
    assert_equal 2, Sidekiq.server_blocks.size
    assert_true Sidekiq.server?
  end

  def test_configure_embed_runs_the_server_blocks
    seen = []
    Sidekiq.configure_server { |c| seen << [:server, c.concurrency] }
    embedded = Sidekiq.configure_embed do |c|
      seen << [:embed, c.class]
      c.queues = %w[a]
    end

    assert_equal [[:server, 2], [:embed, Sidekiq::Config]], seen
    assert_instance_of Robin::Embedded, embedded
    assert_equal %w[a], config.queues
    assert_instance_of Robin::Embedded, Sidekiq.configure_embed
  end

  def test_redis_logger_and_json
    assert_equal "PONG", Sidekiq.redis(&:ping)
    assert_instance_of Sidekiq::RedisConnection::Pool, Sidekiq.redis_pool
    assert Sidekiq.redis_info.key?("redis_version")
    assert_same Robin.logger, Sidekiq.logger
    logger = Logger.new(nil)
    Sidekiq.logger = logger

    assert_same logger, config.logger
    assert_equal '{"a":1}', Sidekiq.dump_json({"a" => 1})
    assert_equal({"a" => 1}, Sidekiq.load_json('{"a":1}'))
  end

  def test_strict_args_and_job_options
    Sidekiq.strict_args!

    assert_equal :raise, config.strict_args
    Sidekiq.strict_args!(:warn)

    assert_equal :warn, config.strict_args
    assert_same Sidekiq::Job.default_options, Sidekiq.default_job_options
  end

  def test_transactional_push_uses_robin_rails
    paths = recording_requires(Sidekiq) { Sidekiq.transactional_push! }

    assert_equal ["robin/rails/transactional_push"], paths

    assert_same Robin::Rails::TransactionalPush, config.push_deferrer
  end

  def test_rails_integration_loads_with_rails
    require "rails"

    assert_equal ["sidekiq/rails"], recording_requires(Sidekiq) { Sidekiq.integrate_rails }
    assert defined?(Sidekiq::Rails)
    engine = ::Rails.send(:remove_const, :Engine)

    assert_empty recording_requires(Sidekiq) { assert_nil Sidekiq.integrate_rails }
  ensure
    ::Rails.const_set(:Engine, engine) if engine
  end

  def test_unknown_testing_modes
    error = assert_raises(RuntimeError) { Sidekiq.testing!(:sideways) }

    assert_equal "Unknown testing mode: sideways", error.message
  end

  def test_defaults_track_worker_threads_capsules_once
    Sidekiq.defaults!(config)
    Sidekiq.defaults!(config)

    assert_equal [Sidekiq::CAPSULE_TRACKER], config.event_handlers
  end

  def test_defaults_leave_robins_batch_status_without_sidekiq_batch
    require "sidekiq/batch"
    batch = Sidekiq.send(:remove_const, :Batch)

    assert_same Robin::Batch::Status, Sidekiq.defaults!(Robin::Config.new).batch_status_class
  ensure
    Sidekiq.const_set(:Batch, batch) if batch
  end

  def test_defaults_give_batch_callbacks_sidekiqs_status
    require "sidekiq/batch"
    c = Sidekiq.defaults!(Robin::Config.new)

    assert_same Sidekiq::Batch::Status, c.batch_status_class
    other = Class.new(Robin::Batch::Status)
    c.batch_status_class = other

    assert_same other, Sidekiq.defaults!(c).batch_status_class
  end

  def test_testing_switch
    assert_equal ["sidekiq/testing"], recording_requires(Sidekiq) { Sidekiq.testing! }
    assert_predicate Sidekiq::Testing, :fake?
    Sidekiq.testing!("disable")

    assert_predicate Sidekiq::Testing, :disabled?
    Sidekiq.testing!(:fake)

    assert_predicate Sidekiq::Testing, :fake?
    Sidekiq.testing!(:inline) { assert_predicate Sidekiq::Testing, :inline? }

    assert_predicate Sidekiq::Testing, :fake?
    Sidekiq.testing!(:disable)

    assert_predicate Sidekiq::Testing, :disabled?
  ensure
    Sidekiq::Testing.disable!
    Robin::Client.interceptor = nil
  end

  def test_freeze_and_loader
    assert_same Sidekiq.default_configuration, Sidekiq.freeze!
    assert_nil Sidekiq.loader
  end
end
