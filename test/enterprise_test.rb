# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-ent"

module EnterpriseTestJobs
  class UniqueJob
    include Sidekiq::Job

    sidekiq_options unique_for: 600, queue: "low"

    def perform(*) = nil
  end

  class Component
    include Sidekiq::Component

    def initialize(config = nil)
      @config = config
    end
  end
end

class SidekiqEnterpriseTest < RobinCompatTest
  include EnterpriseTestJobs

  cover "Sidekiq::Enterprise*"
  cover "Sidekiq::Component*"
  cover "Sidekiq::Client.reliable_push!"

  KEY = "k" * 32

  Elector = Struct.new(:leader?)

  def test_unique_is_always_on
    assert_nil Sidekiq::Enterprise.unique!
    assert_nil Sidekiq::Enterprise.unique!(config)
  end

  def test_locked
    jid = UniqueJob.perform_async(1, "a")

    assert_equal jid, Sidekiq::Enterprise::Unique.locked?(UniqueJob, [1, "a"])
    assert_equal jid, Sidekiq::Enterprise::Unique.locked?("low", UniqueJob.name, [1, "a"])
    assert_equal jid, Sidekiq::Enterprise::Unique.locked?(:low, UniqueJob, [1, "a"])
    assert_nil Sidekiq::Enterprise::Unique.locked?("default", UniqueJob, [1, "a"])
    assert_nil Sidekiq::Enterprise::Unique.locked?(UniqueJob, [2, "a"])
  end

  def test_crypto
    asked = []
    Sidekiq::Enterprise::Crypto.enable(active_version: 3) do |version|
      asked << version
      raise Errno::ENOENT, "secret.1.key" if version == 1

      KEY.tr("k", version.to_s) unless version == 2
    end

    assert_equal [3, 1, 2], asked
    assert_equal({3 => "3" * 32}, config.encryption_keys)
  end

  def test_crypto_keeps_older_keys
    Sidekiq::Enterprise::Crypto.enable(active_version: 2) { |version| KEY.tr("k", version.to_s) }

    assert_equal({2 => "2" * 32, 1 => "1" * 32}, config.encryption_keys)
  end

  def test_crypto_with_one_version_and_another_config
    other = Robin::Config.new
    keys = Sidekiq::Enterprise::Crypto.enable(active_version: 1, config: other) { KEY }

    assert_equal({1 => KEY}, keys)
    assert_equal({1 => KEY}, other.encryption_keys)
    assert_nil config.encryption_keys
  end

  def test_crypto_raises_when_the_active_key_cant_be_had
    assert_raises(KeyError) { Sidekiq::Enterprise::Crypto.enable(active_version: 2) { |v| {1 => KEY}.fetch(v) } }
  end

  def test_leader_from_env
    Sidekiq::Enterprise.leader_from_env(config, {"SIDEKIQ_LEADER" => "true"})

    assert config.leader_candidate
    Sidekiq::Enterprise.leader_from_env(config, {})

    assert config.leader_candidate
    other = Robin::Config.new
    Sidekiq::Enterprise.leader_from_env(other, {"SIDEKIQ_LEADER" => "false"})

    refute other.leader_candidate
    assert config.leader_candidate
  end

  def test_leader_from_env_defaults
    ENV["SIDEKIQ_LEADER"] = "false"
    Sidekiq::Enterprise.leader_from_env

    refute config.leader_candidate
  ensure
    ENV.delete("SIDEKIQ_LEADER")
  end

  def test_component_leader
    other = Robin::Config.new

    refute_predicate Component.new, :leader?
    Robin::Leader.register(config, Elector.new(true))

    assert_predicate Component.new, :leader?
    assert_predicate Component.new(config), :leader?
    assert_predicate Component.new(Sidekiq::Config.new(config)), :leader?
    refute_predicate Component.new(other), :leader?
    refute_predicate Component.new(Sidekiq::Config.new(other)), :leader?
  ensure
    Robin::Leader.unregister(config)
  end

  def test_component_helpers
    errors = []
    config.error_handlers.replace([->(ex, ctx, _c) { errors << [ex.message, ctx] }])
    component = Component.new

    assert_same Sidekiq.default_configuration, component.config
    assert_same config.logger, component.logger
    assert_equal "PONG", component.redis(&:ping)
    component.handle_exception(RuntimeError.new("direct"), {a: 1})
    component.handle_exception(RuntimeError.new("bare"))
    worker = component.safe_thread("poller") do
      Thread.current.report_on_exception = false
      raise "in thread"
    end
    error = assert_raises(RuntimeError) { worker.join }

    assert_equal "in thread", error.message
    assert_equal "sidekiq.poller", worker.name
    assert_equal [["direct", {a: 1}], ["bare", {}], ["in thread", {context: "Exception in poller thread"}]], errors
    assert_equal :done, component.safe_thread("ok") { :done }.value
    require "sidekiq-ent/periodic/testing"

    assert defined?(Sidekiq::Periodic::ConfigTester)
  end

  def test_reliable_push
    assert Sidekiq::Client.reliable_push!
    assert config.reliable_push
    other = Robin::Config.new
    Sidekiq::Client.reliable_push!(other)

    assert other.reliable_push
  end
end
