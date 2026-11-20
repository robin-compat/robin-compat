# frozen_string_literal: true

require_relative "test_helper"

module JobTestJobs
  RAN = []

  class HardJob
    include Sidekiq::Job

    sidekiq_options :queue => "critical", :retry => 5, "tags" => ["t"]

    def perform(*args) = RAN << [jid, args]
  end

  class ChildJob < HardJob
    sidekiq_options retry: 2
  end

  class OldStyle
    include Sidekiq::Worker

    def perform = RAN << :old
  end
end

# Lets a test fix Robin's clock.
module JobTestClock
  class << self
    attr_accessor :now_ms
  end

  def now_ms = JobTestClock.now_ms || super
end
Robin.singleton_class.prepend(JobTestClock)

class JobTest < RobinCompatTest
  cover "Sidekiq::Job*"

  include JobTestJobs

  def setup
    super
    RAN.clear
  end

  def queued(queue = "critical") = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  def scheduled = redis("ZRANGE", key("schedule"), 0, -1, "WITHSCORES").each_slice(2).map { |raw, s| [JSON.parse(raw), Float(s)] }

  def test_including_makes_a_robin_job
    assert_includes HardJob.ancestors, Robin::Job
    assert_same Sidekiq::Job, Sidekiq::Worker
    assert_includes OldStyle.ancestors, Robin::Job
    klass = Class.new { include Sidekiq::Job }

    assert_operator klass.ancestors.index(Sidekiq::Job::InstanceMethods), :<, klass.ancestors.index(Robin::Job)
    assert_includes klass.singleton_class.ancestors, Sidekiq::Job::ClassMethods
  end

  def test_sidekiq_options_inherit_and_stringify
    assert_equal({"retry" => 5, "queue" => "critical", "tags" => ["t"]}, HardJob.get_sidekiq_options)
    assert_equal({"retry" => 2, "queue" => "critical", "tags" => ["t"]}, ChildJob.get_sidekiq_options)
    assert_equal({"retry" => true, "queue" => "default"}, OldStyle.get_sidekiq_options)
    klass = Class.new(OldStyle) { sidekiq_options "dead" => false, "nested" => {a: 1} }

    assert_equal({"a" => 1}, klass.get_sidekiq_options.fetch("nested"))
    assert_false klass.get_sidekiq_options.fetch("dead")
  end

  def test_retry_for_makes_retry_unlimited_as_in_sidekiq
    klass = Class.new(OldStyle) { sidekiq_options retry: 3, retry_for: 3600 }

    assert_equal [true, 3600], klass.get_sidekiq_options.values_at("retry", "retry_for")
  end

  def test_default_options_are_stringified_and_frozen
    Sidekiq::Job.default_options = {queue: "low"}

    assert_equal({"queue" => "low"}, Sidekiq::Job.default_options)
    assert_predicate Sidekiq::Job.default_options, :frozen?
  ensure
    Sidekiq::Job.default_options = {"retry" => true, "queue" => "default"}
  end

  def test_sidekiq_options_returns_them_and_keeps_what_it_isnt_given
    pool = Sidekiq.redis_pool
    klass = Class.new(OldStyle) { sidekiq_options retry: 3, pool: pool }

    assert_equal({"retry" => 3, "queue" => "low", "pool" => pool}, klass.sidekiq_options(queue: "low"))
    assert_equal({"retry" => 3, "queue" => "low", "pool" => pool}, klass.sidekiq_options)
    assert_equal({"retry" => 3, "queue" => "low", "pool" => pool}, klass.sidekiq_options(nil)) # as Sidekiq allows
  end

  def test_default_job_options
    Sidekiq.default_job_options = {"backtrace" => true, :queue => "low"}

    assert_equal({"backtrace" => true, "queue" => "low"}, Sidekiq.default_job_options)
    klass = Class.new { include Sidekiq::Job }

    assert_equal({"backtrace" => true, "queue" => "low"}, klass.get_sidekiq_options)
  ensure
    Sidekiq::Job.default_options = {"retry" => true, "queue" => "default"}
  end

  def test_perform_async_in_and_at
    jid = assert_roundtrips(1) { HardJob.perform_async(1, "a") }
    job = queued.first

    assert_equal [jid, "HardJob", [1, "a"], "critical", 5, ["t"]],
      job.values_at("jid", "class", "args", "queue", "retry", "tags").tap { |v| v[1] = v[1].split("::").last }
    before = Time.now.to_f
    assert_roundtrips(1) { HardJob.perform_in(60, 2) }
    HardJob.perform_at(Time.at(before + 120), 3)
    (job_in, score_in), (job_at, score_at) = scheduled

    assert_in_delta before + 60, score_in, 1
    assert_in_delta before + 120, score_at, 0.01
    assert_equal [[2], [3]], [job_in["args"], job_at["args"]]
    refute job_in.key?("at"), "the score says when"
  end

  def test_set_options_and_waits
    HardJob.set(queue: "low").set(retry: false).perform_async(1)

    assert_equal [[1], false], queued("low").first.values_at("args", "retry")
    HardJob.set(sync: false).perform_async(9)

    assert_equal [[9]], queued.map { |j| j["args"] }
    assert_empty RAN
    redis("DEL", key("queue:critical"))
    setter = HardJob.set(wait: 30)
    setter.perform_async(1)
    setter.perform_async(2)
    HardJob.set(wait_until: Time.now + 90).perform_async(3)
    scores = scheduled.map(&:last)

    assert_equal 3, scores.size
    assert_in_delta scores[0], scores[1], 0.001, "a reused setter keeps its time"
    assert_in_delta Time.now.to_f + 90, scores.max, 2
    assert_equal [], queued
  end

  def test_setters_schedule_with_their_options
    HardJob.set(queue: "low").perform_in(60, 1)
    HardJob.set(queue: "low").perform_at(Time.now + 120, 2)
    (first, first_at), (second, second_at) = scheduled

    assert_equal [["low", [1], "JobTestJobs::HardJob"], ["low", [2], "JobTestJobs::HardJob"]],
      [first, second].map { |job| job.values_at("queue", "args", "class") }
    assert_in_delta Time.now.to_f + 60, first_at, 2
    assert_in_delta Time.now.to_f + 120, second_at, 2
  end

  def test_wait_counts_from_when_it_is_set
    JobTestClock.now_ms = Robin.now_ms + 3_600_000
    setter = HardJob.set(wait: 30)
    JobTestClock.now_ms = nil
    setter.perform_async(1)

    assert_in_delta Time.now.to_f + 3_630, scheduled.first.last, 2
  ensure
    JobTestClock.now_ms = nil
  end

  def test_sync_and_perform_inline_run_through_middleware
    seen = []
    client = Class.new { define_method(:call) { |*args, &blk| (seen << [:client, args[0], args[2], args[3]]) && blk.call } }
    server = Class.new { define_method(:call) { |job, payload, queue, &blk| (seen << [:server, job.class, payload["jid"], queue]) && blk.call } }
    config.client_middleware { |c| c.add client }
    config.server_middleware { |c| c.add server }

    assert_true HardJob.perform_inline(1)
    assert_true HardJob.perform_sync(2)
    assert_true HardJob.set(sync: true).perform_async(3)
    assert_equal [[1], [2], [3]], RAN.map(&:last)
    assert_equal [:client, :server] * 3, seen.map(&:first)
    assert_equal [HardJob.name, "critical", config.redis_pool], seen[0].drop(1)
    assert_equal [HardJob, RAN.first.first, "critical"], seen[1].drop(1)
    assert_equal [], queued
  end

  def test_perform_inline_runs_the_job_as_pushed
    seen = []
    klass = Class.new(OldStyle) do
      define_method(:perform) { |*args| seen << [jid, payload.slice("jid", "args", "queue"), payload.key?("sync"), payload.key?("pool"), args] }
    end
    klass.define_singleton_method(:name) { "InlineJob" }
    Object.const_set(:InlineJob, klass)
    klass.set(sync: true, pool: Sidekiq.redis_pool, queue: "q").perform_async(1, "a")
    jid = seen.first.first

    assert_equal [[jid, {"jid" => jid, "args" => [1, "a"], "queue" => "q"}, false, false, [1, "a"]]], seen
    assert_match(/\A\h{24}\z/, jid)
  ensure
    Object.send(:remove_const, :InlineJob) if Object.const_defined?(:InlineJob)
  end

  def test_perform_inline_stopped_by_middleware
    config.client_middleware { |c| c.add(Class.new { def call(*) = nil }) }

    assert_nil HardJob.perform_inline(1)
    config.client_middleware(&:clear)
    config.server_middleware { |c| c.add(Class.new { def call(*) = false }) }

    assert_nil HardJob.perform_inline(1)
    assert_empty RAN
  end

  def test_perform_inline_is_true_whatever_perform_returns
    assert_true Class.new(OldStyle) { def perform = nil }.tap { |k| Object.const_set(:NilJob, k) }.perform_inline
  ensure
    Object.send(:remove_const, :NilJob) if Object.const_defined?(:NilJob)
  end

  def test_perform_bulk_keeps_the_setters_options
    later = Time.now.to_f + 600
    HardJob.set(queue: "low", wait_until: Time.at(later)).perform_bulk([[1], [2]], batch_size: 1)

    assert_equal [["low", [1]], ["low", [2]]], scheduled.map { |job, _| job.values_at("queue", "args") }.sort_by(&:last)
    scheduled.each { |_, score| assert_in_delta later, score, 0.001 }
  end

  def test_perform_bulk
    jids = HardJob.perform_bulk([[1], [2], [3]], batch_size: 2)

    assert_equal 3, jids.size
    assert_equal [[1], [2], [3]], queued.reverse.map { |j| j["args"] }
    later = Time.now.to_f + 600
    HardJob.set(queue: "low").perform_bulk([[4], [5]], at: later)

    scheduled.each { |_, score| assert_in_delta later, score, 0.001 }
    HardJob.perform_bulk([[6], [7], [8]], spread_interval: 60)

    assert_equal 8, queued.size + scheduled.size, "3 queued, then 2 and 3 more"
    assert_operator scheduled.size, :>=, 4
  end

  def test_retry_in_counts_from_zero_and_reads_like_sidekiq
    seen = []
    klass = Class.new(OldStyle) do
      sidekiq_retry_in do |count, exception, job|
        seen << [count, exception.message, job["jid"]]
        {0 => 10, 1 => "20", 2 => 0, 3 => -5, 4 => :kill, 5 => :discard, 6 => nil, 7 => "nonsense"}.fetch(count)
      end
    end
    answers = (1..8).map { |failures| klass.robin_retry_in_block.call(failures, RuntimeError.new("boom"), {"jid" => "j"}) }

    assert_equal [10, 20, nil, nil, :kill, :discard, nil, nil], answers
    assert_equal (0..7).map { |n| [n, "boom", "j"] }, seen
    assert_respond_to klass.sidekiq_retry_in_block, :call
    assert_same klass.sidekiq_retry_in_block, Class.new(klass).sidekiq_retry_in_block
    assert_nil OldStyle.sidekiq_retry_in_block
  end

  def test_retry_delay
    assert_equal :kill, Sidekiq::Job.retry_delay(:kill)
    assert_equal :discard, Sidekiq::Job.retry_delay(:discard)
    assert_equal 3, Sidekiq::Job.retry_delay(3.7)
    assert_nil Sidekiq::Job.retry_delay(Object.new)
  end

  def test_retries_exhausted
    block = ->(job, exception) { [job, exception] }
    klass = Class.new(OldStyle) { sidekiq_retries_exhausted(&block) }

    assert_same block, klass.sidekiq_retries_exhausted_block
    assert_same block, klass.robin_retries_exhausted_block
  end

  # Runs the block with Sidekiq's backoff jitter fixed at its lowest
  # (+:low+) or highest.
  def with_jitter(edge)
    Sidekiq::Job.define_singleton_method(:rand) { |n| (edge == :low) ? 0 : n - 1 }
    yield
  ensure
    Sidekiq::Job.singleton_class.remove_method(:rand)
  end

  def test_sidekiq_backoff
    backoff = Sidekiq::Job::RETRY_BACKOFF
    lows = with_jitter(:low) { [backoff.call(1, nil, {}, nil), backoff.call(3, nil, {}, nil), backoff.call(3, nil, {}, 100)] }
    highs = with_jitter(:high) { [backoff.call(1, nil, {}, nil), backoff.call(3, nil, {}, nil), backoff.call(3, nil, {}, 100)] }

    assert_equal [15, 31, 100], lows, "count**4 + 15, or the custom delay"
    assert_equal [24, 60, 129], highs, "plus up to 10 * (count + 1) - 1 seconds"
    assert_includes 15..24, backoff.call(1, nil, {}, nil)
  end

  def test_pool_option_pushes_elsewhere
    other = Robin::Config.new
    other.redis = {url: REDIS_URL}
    other.key_prefix = "#{key_prefix_for_test}other:"
    pools = record_push_pools
    klass = Class.new(OldStyle) { sidekiq_options pool: other.redis_pool }
    klass.define_singleton_method(:name) { "PooledJob" }
    klass.perform_async
    klass.perform_in(60)
    klass.perform_at(Time.now + 60)
    klass.perform_bulk([[]])
    klass.client_push(class: klass, args: [])
    HardJob.client_push(class: HardJob, args: [], pool: other.redis_pool)
    HardJob.client_push(class: HardJob, args: [])

    assert_equal [other.redis_pool] * 6 + [config.redis_pool], pools
    assert_same other.redis_pool, klass.build_client.redis_pool
    assert_nil HardJob.build_client.redis_pool
    assert_equal 3, redis("LLEN", "#{key_prefix_for_test}queue:default"), "the key prefix is the test's"
    pools.clear
    redis("DEL", key("queue:critical"))

    assert_same other.redis_pool, klass.get_sidekiq_options.fetch("pool")
    assert_same other.redis_pool, Class.new(klass).sidekiq_pool
    refute klass.robin_options.key?("pool")
    HardJob.set(pool: other.redis_pool).perform_async(1)
    HardJob.set(pool: other.redis_pool).perform_bulk([[2]])

    assert_equal 2, queued.size
    assert_equal [other.redis_pool] * 2, pools
    refute(queued.any? { |job| job.key?("pool") })
  ensure
    other&.redis_pool&.shutdown
  end

  def test_encrypt_true_means_the_last_argument
    klass = Class.new(OldStyle) { sidekiq_options encrypt: true }

    assert_equal "last", klass.get_sidekiq_options["encrypt"]
    assert_equal "last", Class.new(OldStyle) { sidekiq_options encrypt: "last" }.get_sidekiq_options["encrypt"]
    refute Class.new(OldStyle) { sidekiq_options encrypt: false }.get_sidekiq_options["encrypt"]
  end

  def test_unique_context
    assert_equal [1, 2], HardJob.robin_unique_args([1, 2])
    seen = []
    klass = Class.new(HardJob) do
      define_singleton_method(:sidekiq_unique_context) { |job| (seen << job) && job["args"].first(1) }
    end
    klass.define_singleton_method(:name) { "ContextJob" }

    assert_equal [1], klass.robin_unique_args([1, 2])
    assert_equal [{"class" => "ContextJob", "queue" => "critical", "args" => [1, 2]}], seen
    Sidekiq::Job.default_options = {"retry" => true}
    queueless = Class.new(OldStyle) { define_singleton_method(:sidekiq_unique_context) { |job| job } }

    assert_equal({"class" => nil, "queue" => nil, "args" => [3]}, queueless.robin_unique_args([3]))
  ensure
    Sidekiq::Job.default_options = {"retry" => true, "queue" => "default"}
  end

  def test_instance_methods_come_before_robins
    assert_operator HardJob.ancestors.index(Sidekiq::Job::InstanceMethods), :<, HardJob.ancestors.index(Robin::Job)
  end

  def test_delay_methods_raise_and_queue_as
    assert_equal "Do not call .delay on a Sidekiq::Job class, call .perform_async",
      assert_raises(ArgumentError) { HardJob.delay }.message
    assert_equal "Do not call .delay_for on a Sidekiq::Job class, call .perform_in",
      assert_raises(ArgumentError) { HardJob.delay_for(1) }.message
    assert_equal "Do not call .delay_until on a Sidekiq::Job class, call .perform_at",
      assert_raises(ArgumentError) { HardJob.delay_until(Time.now) }.message
    klass = Class.new(OldStyle) { queue_as :mailers }

    assert_equal "mailers", klass.get_sidekiq_options.fetch("queue")
    assert_instance_of Sidekiq::Client, klass.build_client
  end
end
