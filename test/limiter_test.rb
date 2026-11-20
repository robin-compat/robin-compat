# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-ent"

module LimiterTestJobs
  class SlowDown < StandardError; end

  class Limited
    include Sidekiq::Job

    def perform(error) = raise(Object.const_get(error), "limited")
  end
end

class SidekiqLimiterTest < RobinCompatTest
  include LimiterTestJobs

  cover "Sidekiq::Limiter*"

  def teardown
    Sidekiq::Limiter.instance_variable_set(:@configuration, nil)
    super
  end

  def scheduled = redis("ZRANGE", key("schedule"), 0, -1, "WITHSCORES").each_slice(2).map { |raw, s| [JSON.parse(raw), Float(s)] }

  # Runs a job through the server middleware, as a server would.
  def run_job(error, extra = {})
    job = {"class" => Limited.name, "args" => [error], "jid" => "jid1", "queue" => "default"}.merge(extra)
    instance = Limited.new
    instance.jid = "jid1"
    instance.payload = job
    config.server_middleware.invoke(instance, job, "default") { instance.perform(error) }
  end

  def over_limit(limiter) = raise(Robin::Limiter::OverLimit.new(limiter.__getobj__))

  def test_limiters_with_sidekiqs_options
    concurrent = Sidekiq::Limiter.concurrent(:erp, 5, wait_timeout: 1, lock_timeout: 10, ttl: 3600)
    bucket = Sidekiq::Limiter.bucket("stripe", 30, :second, wait_timeout: 0, policy: :ignore)
    window = Sidekiq::Limiter.window("sms", 10, 30, policy: "raise", reschedule: 3)
    leaky = Sidekiq::Limiter.leaky("shopify", 60, :minute, wait_timeout: 0, policy: :ignore)
    points = Sidekiq::Limiter.points("gql", 100, 10, wait_timeout: 2, policy: :ignore, ttl: 60)

    assert_instance_of Robin::Limiter::Concurrent, concurrent.__getobj__
    assert_equal ["erp", 5, 1.0, 10.0, :raise], [concurrent.name, concurrent.limit, concurrent.wait_timeout, concurrent.lock_timeout, concurrent.policy]
    assert_instance_of Robin::Limiter::Bucket, bucket.__getobj__
    assert_equal ["stripe", 30, 1.0, 0.0, :skip], [bucket.name, bucket.limit, bucket.interval, bucket.wait_timeout, bucket.policy]
    assert_instance_of Robin::Limiter::Window, window.__getobj__
    assert_equal ["sms", 10, 30.0, :raise, 3], [window.name, window.limit, window.interval, window.policy, window.reschedule]
    assert_instance_of Robin::Limiter::Leaky, leaky.__getobj__
    assert_equal ["shopify", 60, 60.0, 0.0, :skip], [leaky.name, leaky.limit, leaky.interval, leaky.wait_timeout, leaky.policy]
    assert_instance_of Robin::Limiter::Points, points.__getobj__
    assert_equal ["gql", 100, 10.0, 2.0, :skip], [points.name, points.limit, points.interval, points.wait_timeout, points.policy]
    assert_respond_to points.backoff, :call
    assert_respond_to concurrent.backoff, :call
  end

  def test_within_limit_and_state
    limiter = Sidekiq::Limiter.bucket("api", 2, :minute, wait_timeout: 0)

    assert_equal(:ran, limiter.within_limit { |l| (l == limiter.__getobj__) ? :ran : :other })
    assert_equal :ok, limiter.within_limit(used: 1) { :ok }
    assert_equal 2, limiter.used
    assert_equal 2, limiter.to_h.fetch("used")
    error = assert_raises(Sidekiq::Limiter::OverLimit) { limiter.within_limit { :never } }
    assert_same limiter.__getobj__, error.limiter
    assert_equal 2, limiter.over_limit! # the failed call counted once
    limiter.reset_state

    assert_equal 0, limiter.used
  end

  def test_ignore_policy_skips_the_block
    limiter = Sidekiq::Limiter.concurrent("one", 1, wait_timeout: 0, policy: :ignore)

    assert_nil(limiter.within_limit { limiter.within_limit { :inner } })
  end

  def test_points_limiter_settles_the_estimate
    limiter = Sidekiq::Limiter.points("graphql", 1_000, 20, wait_timeout: 0)
    handle = nil
    limiter.within_limit(estimate: 200) do |h|
      handle = h
      h.points_used(50)
    end

    assert_instance_of Sidekiq::Limiter::Points::Usage, handle
    assert_equal [limiter, 200], handle.to_a
    assert_in_delta 50, limiter.used, 1
    assert_in_delta 1_000, limiter.refund(50), 1
    assert_in_delta 900, limiter.adjust(100), 1
    limiter.within_limit { |h| h.points_used(1) }

    assert_in_delta 100, limiter.used, 1
    slow = Sidekiq::Limiter.points("slow", 1_000, 86_400)
    slow.within_limit { nil } # an estimate of 1, kept

    assert_equal 1, slow.used
    assert_raises(ArgumentError) { limiter.within_limit(estimate: 1_001) { nil } }
  end

  def test_unlimited
    limiter = Sidekiq::Limiter.unlimited

    assert_instance_of Robin::Limiter::Unlimited, limiter
    assert_equal "unlimited", limiter.name
    assert_equal "admin", Sidekiq::Limiter.unlimited(:admin).name
    assert_equal(:ran, limiter.within_limit { :ran })
  end

  def test_over_limit_in_a_job_backs_off_as_sidekiq_does
    limiter = Sidekiq::Limiter.concurrent("erp", 1, wait_timeout: 0)
    job = {"class" => Limited.name, "args" => [], "jid" => "j1", "queue" => "default", "limited" => 2}
    before = Time.now.to_f
    config.server_middleware.invoke(Limited.new, job, "default") { over_limit(limiter) }
    (stored, at), = scheduled

    assert_equal 3, stored.fetch("limited")
    refute stored.key?("overrated")
    # 300 * 3 + rand(1..300) seconds
    assert_operator at - before, :>=, 900
    assert_operator at - before, :<=, 1_202
  end

  def test_backoffs
    seen = []
    Sidekiq::Limiter.configure do |c|
      c.backoff = ->(limiter, job, exception) do
        seen << [:global, limiter.name, job.fetch("overrated"), job.fetch("jid"), exception.class]
        60
      end
    end
    global = Sidekiq::Limiter.window("w", 1, :minute, wait_timeout: 0)
    own = Sidekiq::Limiter.window("o", 1, :minute, wait_timeout: 0, backoff: ->(limiter, job, _e) do
      seen << [:own, limiter.name, job.fetch("overrated")]
      120
    end)
    job = {"class" => Limited.name, "args" => [], "jid" => "j1", "queue" => "default"}
    now = Time.now.to_f
    config.server_middleware.invoke(Limited.new, job.dup, "default") { over_limit(global) }
    config.server_middleware.invoke(Limited.new, job.merge("jid" => "j2", "limited" => 4), "default") { over_limit(own) }

    assert_equal [[:global, "w", 1, "j1", Robin::Limiter::OverLimit], [:own, "o", 5]], seen
    ats = scheduled.to_h { |stored, at| [stored.fetch("jid"), at - now] }

    assert_in_delta 60, ats.fetch("j1"), 2
    assert_in_delta 120, ats.fetch("j2"), 2
  end

  def test_default_backoff
    [1, 2, 3].each do |seed|
      srand(seed)
      expected = rand(601..900) # 300 * 2 + rand(1..300)
      srand(seed)

      assert_equal expected, Sidekiq::Limiter::DEFAULT_BACKOFF.call(nil, {"overrated" => 2}, nil)
    end
    assert_same Sidekiq::Limiter::DEFAULT_BACKOFF, Sidekiq::Limiter.configuration.backoff
  end

  def test_configured_errors_count_as_over_limit
    Sidekiq::Limiter.configure { |c| c.errors << SlowDown }
    chain = config.server_middleware.map(&:klass)

    assert_equal chain.index(Robin::Batch::ServerMiddleware) + 1, chain.index(Sidekiq::Limiter::ErrorsMiddleware)
    run_job(SlowDown.name)
    (stored, _), = scheduled

    assert_equal 1, stored.fetch("limited")
    assert_raises(ArgumentError) { run_job(ArgumentError.name) }
  end

  def test_configured_errors_use_the_configured_backoff
    calls = []
    Sidekiq::Limiter.configure do |c|
      c.errors << SlowDown
      c.backoff = ->(limiter, job, exception) do
        calls << [limiter.name, limiter.type, limiter.reschedule, job.fetch("overrated"), exception.message]
        30
      end
    end
    now = Time.now.to_f
    run_job(SlowDown.name)

    assert_equal [[SlowDown.name, "error", nil, 1, %(rate limiter "#{SlowDown.name}" (error) is over its limit)]], calls
    assert_in_delta 30, scheduled.first.last - now, 2
  end

  def test_a_batch_doesnt_count_a_configured_error_as_a_failure
    Sidekiq::Limiter.configure { |c| c.errors << SlowDown }
    batch = Robin::Batch.new
    batch.jobs { Limited.perform_async(SlowDown.name) }
    run_job(SlowDown.name, "bid" => batch.bid, "jid" => batch.status.jids.first)

    assert_equal 0, batch.status.failures
    assert_equal 1, scheduled.size
  end

  def test_configure_without_errors_adds_no_middleware
    Sidekiq::Limiter.configure { |c| c.backoff = ->(*) { 1 } }

    refute config.server_middleware.exists?(Sidekiq::Limiter::ErrorsMiddleware)
  end

  def test_limiters_keep_state_in_the_configured_redis
    url = REDIS_URL.sub(/db=\d+/, "db=14")
    Sidekiq::Limiter.configure { |c| c.redis = {url: url, size: 2} }
    pool = Sidekiq::Limiter.configuration.pool

    assert_equal 2, pool.size
    limiter = Sidekiq::Limiter.bucket("elsewhere", 5, :hour)
    limiter.within_limit { nil }
    points = Sidekiq::Limiter.points("pts", 10, 60)
    points.within_limit(estimate: 4) { |h| h.points_used(3) }

    assert_equal 1, limiter.used
    assert_in_delta 3, points.used, 1
    assert_equal 0, Robin::Limiter.bucket("elsewhere", 5, :hour).used
    assert_nil redis("GET", config.key("lmtr", "elsewhere", "b", Robin.now_ms / 3_600_000))
    assert_equal "1", pool.with { |c| c.call("GET", config.key("lmtr", "elsewhere", "b", Robin.now_ms / 3_600_000)) }
  ensure
    pool&.with do |c|
      keys = c.call("KEYS", "#{key_prefix_for_test}*")
      c.call("DEL", *keys) unless keys.empty?
    end
    pool&.shutdown
  end

  def test_with_redis_without_a_pool_yields
    assert_equal(:here, Sidekiq::Limiter.with_redis { :here })
    assert_nil Robin::Limiter.current_pool
  end

  def test_without_a_configured_redis_robins_via_still_applies
    shard = Robin::Config.new
    shard.redis = {url: REDIS_URL.sub(/db=\d+/, "db=14")}
    pool = shard.redis_pool
    seen = Robin::Limiter.via(pool) { Sidekiq::Limiter.with_redis { Robin::Limiter.current_pool } }

    assert_same pool, seen
  ensure
    pool&.shutdown
  end

  def test_over_limit_alias
    assert_same Robin::Limiter::OverLimit, Sidekiq::Limiter::OverLimit
  end
end
