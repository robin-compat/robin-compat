# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-pro"

module BatchTestJobs
  LOG = []

  class Work
    include Sidekiq::Job

    def perform(n)
      raise ArgumentError, "bad row #{n}" if n.negative?

      LOG << [:work, n, valid_within_batch?]
    end
  end

  # Adds a batch of its own while running inside another batch's job.
  class Spawner
    include Sidekiq::Job

    def perform
      child = Sidekiq::Batch.new
      child.jobs { Work.perform_async(7) }
      LOG << [:spawned, child.bid]
    end
  end

  class Callbacks
    def on_success(status, options) = LOG << [:success, status.bid, options]

    def on_complete(status, options) = LOG << [:complete, status.bid, options]
  end
end

class SidekiqBatchTest < RobinCompatTest
  include BatchTestJobs

  cover "Sidekiq::Batch*"
  cover "Sidekiq::Job::InstanceMethods*"

  def setup
    super
    LOG.clear
  end

  def queued(queue = "default") = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  # Runs the queue's jobs, as a server would, until it's empty.
  def work_off(queue = "default")
    while (raw = redis("RPOP", key("queue:#{queue}")))
      job = JSON.parse(raw)
      instance = Object.const_get(job.fetch("class")).new
      instance.jid = job.fetch("jid")
      instance.payload = job
      begin
        config.server_middleware.invoke(instance, job, queue) { instance.perform(*job.fetch("args")) }
      rescue ArgumentError
        nil # recorded as the batch job's failure
      end
    end
  end

  def test_jobs_are_pushed_when_the_block_ends
    batch = Sidekiq::Batch.new
    seen = nil
    bid = batch.jobs do
      Work.perform_async(1)
      Work.perform_async(2)
      seen = queued.size
    end

    assert_equal 0, seen
    assert_equal batch.bid, bid
    assert_equal [[1], [2]], queued.map { |job| job.fetch("args") }.sort
    assert_equal [bid], queued.map { |job| job.fetch("bid") }.uniq
    assert_equal 2, batch.status.total
  end

  def test_no_jobs_are_pushed_when_the_block_raises
    batch = Sidekiq::Batch.new
    assert_raises(RuntimeError) do
      batch.jobs do
        Work.perform_async(1)
        raise "boom"
      end
    end

    assert_empty queued
    assert_raises(Sidekiq::Batch::NoSuchBatch) { batch.status }
  end

  def test_a_batch_is_a_child_only_inside_another_batchs_jobs_block
    outer = Sidekiq::Batch.new
    inner = nil
    outer.jobs do
      inner = Sidekiq::Batch.new
      inner.jobs { Work.perform_async(1) }
    end

    assert_equal outer.bid, inner.parent_bid
    assert_instance_of Sidekiq::Batch, inner.parent
    assert_equal outer.bid, inner.parent.bid
    assert_same config, inner.parent.config
    assert_nil outer.parent

    # A batch made by a job in a batch stands alone (unlike Robin's).
    spawner = Sidekiq::Batch.new
    spawner.jobs { Spawner.perform_async }
    work_off
    _, child_bid = LOG.assoc(:spawned)

    assert_nil Sidekiq::Batch.new(child_bid).parent_bid
  end

  def test_callbacks_and_status
    batch = Sidekiq::Batch.new
    batch.description = "import"
    batch.on(:success, Callbacks, "who" => "me")
    batch.on(:complete, Callbacks)
    batch.jobs do
      Work.perform_async(1)
      Work.perform_async(-1)
    end
    work_off
    status = Sidekiq::Batch::Status.new(batch.bid)

    assert_equal [[:work, 1, true], [:complete, batch.bid, {}]], LOG
    assert_equal [2, 1, 1], [status.total, status.pending, status.failures]
    assert_predicate status, :complete?
    refute_predicate status, :success?

    jid = status.failed_jids.first

    assert_equal 1, status.failed_jids.size
    assert_includes status.jids, jid
    info = status.failure_info.first

    assert_instance_of Sidekiq::Batch::Status::FailureInfo, info
    assert_equal [jid, "ArgumentError", "bad row -1", nil], info.to_a
    assert_equal({"bid" => batch.bid, "total" => 2, "pending" => 1, "failures" => 1, "description" => "import",
                  "parent_bid" => nil, "complete" => true, "created_at" => status.created_at.to_f,
                  "fail_info" => [{"jid" => jid, "error_class" => "ArgumentError", "error_message" => "bad row -1"}],
                  "tags" => []}, status.data)
  end

  def test_created_at_is_a_time
    before = Time.now
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    created = batch.status.created_at

    assert_instance_of Time, created
    assert_in_delta before.to_f, created.to_f, 1
    assert_equal Robin::Batch::Status.new(batch.bid).created_at, (created.to_r * 1000).to_i
  end

  def test_status_of_an_unknown_batch
    error = assert_raises(Sidekiq::Batch::NoSuchBatch) { Sidekiq::Batch::Status.new("nope") }
    assert_equal "no batch nope (expired or deleted?)", error.message
    assert_same Robin::Batch::NoSuchBatch, Sidekiq::Batch::NoSuchBatch
  end

  def test_status_uses_the_given_config
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    other = Robin::Config.new
    other.redis = {url: REDIS_URL}
    other.key_prefix = "#{key_prefix_for_test}elsewhere:"

    assert_raises(Sidekiq::Batch::NoSuchBatch) { Sidekiq::Batch::Status.new(batch.bid, config: other) }
  ensure
    other&.redis_pool&.shutdown
  end

  def test_batches_of_another_config
    other = Robin::Config.new
    other.redis = {url: REDIS_URL}
    other.key_prefix = "#{key_prefix_for_test}elsewhere:"
    outer = Sidekiq::Batch.new(config: other)
    inner = nil
    outer.jobs do
      inner = Sidekiq::Batch.new(config: other)
      inner.jobs { Work.perform_async(1) }
    end
    status = outer.status

    assert_equal outer.bid, status.bid
    assert_same other, inner.parent.config
    assert_equal outer.bid, inner.parent.bid
    jid = inner.status.jids.first
    Robin::Batch.job_failed(other, inner.bid, jid, {"error_class" => "E"})
    other.redis { |c| c.call("ZADD", other.key("dead"), 1, JSON.generate("class" => "Work", "args" => [], "jid" => jid)) }

    assert_equal [jid], inner.status.dead_jids
    pending = Sidekiq::Batch.new(config: other)
    pending.jobs { Work.perform_async(2) }
    waiting = pending.status
    waiting.define_singleton_method(:sleep) { |_| Robin::Batch.job_succeeded(other, pending.bid, waiting.jids.first) }

    assert_predicate waiting.join(0), :complete?
    inner.invalidate_jids(jid)

    assert Robin::Cancellation.cancelled?(jid, config: other)
    refute Robin::Cancellation.cancelled?(jid)
  ensure
    other&.redis_pool&.shutdown
  end

  def test_failures_without_details
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    jid = batch.status.jids.first
    Robin::Batch.job_failed(config, batch.bid, jid, {})

    assert_equal [[jid, nil, nil, nil]], batch.status.failure_info.map(&:to_a)
    assert_equal [{"jid" => jid}], batch.status.data.fetch("fail_info")
  end

  def test_child_data
    outer = Sidekiq::Batch.new
    inner = nil
    outer.jobs do
      inner = Sidekiq::Batch.new
      inner.tags = ["t"]
      inner.jobs { Work.perform_async(1) }
    end

    assert_equal [outer.bid, ["t"], 1, false], inner.status.data.values_at("parent_bid", "tags", "pending", "complete")
  end

  def test_delete
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    status = batch.status

    assert status.delete
    assert_nil status.created_at
    assert_nil status.data.fetch("created_at")
    assert_raises(Sidekiq::Batch::NoSuchBatch) { batch.status }
  end

  def test_dead_jids
    batch = Sidekiq::Batch.new
    batch.jobs do
      Work.perform_async(-1)
      Work.perform_async(-2)
    end
    work_off
    jids = batch.status.failed_jids.sort
    redis("ZADD", key("dead"), 1, JSON.generate("class" => "Work", "args" => [], "jid" => jids.first, "queue" => "default"))

    assert_equal [jids.first], batch.status.dead_jids
  end

  def test_join_waits_until_complete
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    status = batch.status
    naps = []
    test = self
    status.define_singleton_method(:sleep) do |seconds|
      naps << seconds
      test.work_off if naps.size == 2
    end

    assert_same status, status.join(0.25)
    assert_equal [0.25, 0.25], naps
    assert_predicate status, :complete?
    assert_same status, status.join
    assert_equal 2, naps.size
  end

  def test_join_checks_every_half_second_by_default
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    status = batch.status
    naps = []
    test = self
    status.define_singleton_method(:sleep) do |seconds|
      naps << seconds
      test.work_off
    end
    status.join

    assert_equal [0.5], naps
  end

  def test_a_jobs_batch_is_a_sidekiq_batch
    job = Work.new

    assert_nil job.batch
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }
    job.payload = {"bid" => batch.bid}

    assert_instance_of Sidekiq::Batch, job.batch
    assert_equal batch.bid, job.batch.bid
  end

  def test_invalidate_all
    batch = Sidekiq::Batch.new
    batch.jobs { Work.perform_async(1) }

    refute_predicate batch, :invalidated?
    refute_predicate batch.status, :invalidated?
    assert_equal [batch.bid], batch.invalidate_all
    assert_predicate batch, :invalidated?
    assert_predicate Sidekiq::Batch.new(batch.bid), :invalidated?
    assert_predicate batch.status, :invalidated?

    job = Work.new
    job.jid = "j"
    job.payload = {"bid" => batch.bid}

    refute_predicate job, :valid_within_batch?
    job.payload = {}

    assert_predicate job, :valid_within_batch?
  end

  def test_invalidate_jids
    batch = Sidekiq::Batch.new
    batch.jobs { 3.times { |i| Work.perform_async(i) } }
    jids = batch.status.jids

    assert_equal jids.first(2), batch.invalidate_jids(jids[0], [jids[1]])
    assert_equal [], batch.invalidate_jids
    refute_predicate batch, :invalidated?
    valid = jids.map do |jid|
      job = Work.new
      job.jid = jid
      job.payload = {"bid" => batch.bid}
      job.valid_within_batch?
    end

    assert_equal [false, false, true], valid
    expires = redis("ZMSCORE", Robin.config.key("cancelled"), jids[0]).first.to_i

    assert_in_delta Robin.now_ms + (Sidekiq::Batch::TTL * 1000), expires, 5_000
  end

  def test_batch_set_lists_batches_not_yet_successful
    done = Sidekiq::Batch.new
    done.jobs { Work.perform_async(1) }
    work_off
    failed = Sidekiq::Batch.new
    failed.jobs { Work.perform_async(-1) }
    work_off
    pending = Sidekiq::Batch.new
    pending.jobs { Work.perform_async(2) }
    set = Sidekiq::BatchSet.new

    assert_same Sidekiq::Batch::BatchSet, Sidekiq::BatchSet
    # Sorted: batches made in the same millisecond have no order.
    assert_equal [pending.bid, failed.bid].sort, set.map(&:bid).sort
    assert_equal [Sidekiq::Batch::Status], set.map(&:class).uniq
    assert_equal 2, set.size
    assert_instance_of Enumerator, set.each
  end

  def test_empty_sets
    assert_equal 0, Sidekiq::BatchSet.new.size
    assert_equal 0, Sidekiq::Batch::DeadSet.new.size
    assert_empty Sidekiq::BatchSet.new.scan_tags("x")
  end

  def test_sets_list_every_batch
    bids = Array.new(170) { Sidekiq::Batch.new.tap { |b| b.tags = ["many"] }.jobs { nil } }
    # Empty batches succeed at once; make them pending again.
    bids.each { |bid| redis("HDEL", config.key("b", bid), "success_at") }

    assert_equal 170, Sidekiq::BatchSet.new.size
    assert_equal bids.sort, Sidekiq::BatchSet.new.scan_tags("many").sort
  end

  def test_scan_tags
    tagged = Sidekiq::Batch.new
    tagged.tags = %w[customer:1 acme]
    tagged.jobs { Work.perform_async(1) }
    other = Sidekiq::Batch.new
    other.tags = %w[acme]
    other.jobs { Work.perform_async(2) }
    set = Sidekiq::BatchSet.new
    yielded = []
    set.scan_tags("customer:1") { |bid| yielded << bid }

    assert_equal [tagged.bid], yielded
    assert_equal [other.bid, tagged.bid].sort, set.scan_tags(:acme).sort
    assert_empty set.scan_tags("nope")
  end

  def test_dead_set
    alive = Sidekiq::Batch.new
    alive.jobs { Work.perform_async(1) }
    dead = Sidekiq::Batch.new
    dead.jobs { Work.perform_async(2) }
    jid = dead.status.jids.first
    Robin::Batch.job_died(config, {"bid" => dead.bid, "jid" => jid}, RuntimeError.new("gone"))
    set = Sidekiq::Batch::DeadSet.new

    assert_equal [dead.bid], set.map(&:bid)
    assert_instance_of Sidekiq::Batch::Status, set.first
    assert_equal 1, set.size
    assert_instance_of Enumerator, set.each
  end

  def test_server_middleware_for_tests
    assert_same Robin::Batch::ServerMiddleware, Sidekiq::Batch::Server
  end
end
