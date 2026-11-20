# frozen_string_literal: true

require_relative "test_helper"

class SidekiqCapsuleTest < RobinCompatTest
  cover "Sidekiq::CapsuleMethods*"

  def capsule(queues)
    cap = Sidekiq::Capsule.new("c", config)
    cap.queues = queues
    cap
  end

  def test_sidekiq_capsules_are_robins
    assert_same Robin::Capsule, Sidekiq::Capsule
    assert_kind_of Sidekiq::Capsule, Sidekiq.default_configuration.capsule("default")
  end

  def test_mode
    assert_equal :strict, capsule(%w[a b]).mode
    assert_equal :random, capsule(["a,1", "b,1"]).mode
    assert_equal :weighted, capsule(["a,1", "b,3"]).mode
    assert_equal :weighted, capsule(["a,2", "b,2"]).mode
  end

  def test_to_h
    cap = capsule(["a,1", "b,3"])
    cap.concurrency = 4

    assert_equal({concurrency: 4, mode: :weighted, weights: {"a" => 1, "b" => 3}}, cap.to_h)
  end

  def test_redis_goes_through_the_configs_pool
    cap = capsule(%w[a])

    assert_same config.redis_pool, cap.redis_pool.pool
    cap.redis { |conn| conn.set(key("capsule"), "1") }

    assert_equal "1", redis("GET", key("capsule"))
    assert_kind_of Sidekiq::RedisConnection::Connection, cap.redis { |conn| conn }
  end

  def test_logger_is_the_configs
    assert_same config.logger, capsule(%w[a]).logger
  end

  def test_worker_threads_know_their_capsule
    cap = capsule(%w[a])
    seen = Thread.new do
      Thread.current[:robin_capsule] = cap
      Robin::Events.emit(config, "worker.started", capsule: "c")
      Thread.current[:sidekiq_capsule]
    end.value

    assert_same cap, seen
    other = Thread.new do
      Thread.current[:robin_capsule] = cap
      Robin::Events.emit(config, "worker.died", error: nil)
      Thread.current[:sidekiq_capsule]
    end.value

    assert_nil other
  end
end
