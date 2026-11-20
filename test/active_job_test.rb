# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq/active_job"

ActiveJob::Base.logger = Logger.new(nil)

module ActiveJobTestJobs
  RAN = []

  # (Re)defines the job classes, so each test runs their class bodies
  # (and the sidekiq_* calls in them) afresh.
  def self.define!
    %i[GreetJob FlakyJob FlakierJob PlainJob].each { |name| remove_const(name) if const_defined?(name, false) }
    const_set(:GreetJob, Class.new(ActiveJob::Base) do
      queue_as :mail
      sidekiq_options retry: 3, tags: ["greetings"]

      def perform(name) = RAN << [name, provider_job_id]
    end)
    const_set(:FlakyJob, Class.new(ActiveJob::Base) do
      sidekiq_retry_in { |count, exception, job| (exception.message == "kill") ? :kill : 100 * (count + 1) + job.fetch("args").size - 1 }
      sidekiq_retries_exhausted { |job, _exception| RAN << [:exhausted, job.fetch("wrapped")] && :discard }

      def perform = raise(ArgumentError, "flaky")
    end)
    const_set(:FlakierJob, Class.new(FlakyJob))
    const_set(:PlainJob, Class.new(ActiveJob::Base) { def perform = nil })
  end
end

class ActiveJobTest < RobinCompatTest
  cover "Sidekiq::ActiveJob*"
  cover "Sidekiq::Job::Options*"
  cover "ActiveJob::QueueAdapters::SidekiqAdapter*"

  include ActiveJobTestJobs

  # Stands in for a fetched job: what the retrier acknowledges.
  Unit = Struct.new(:fetcher, :raw, :acknowledged) do
    def acknowledge(_tx = nil) = self.acknowledged = true
  end

  def setup
    super
    RAN.clear
    ActiveJobTestJobs.define!
    [GreetJob, FlakyJob, PlainJob].each { |klass| klass.queue_adapter = :sidekiq }
  end

  def queued(queue) = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  # Runs a job hash as a server would: on the class it names.
  def perform(job)
    instance = Object.const_get(job.fetch("class")).new
    instance.jid = job.fetch("jid")
    instance.payload = job
    instance.perform(*job.fetch("args"))
  end

  def fail_job(job, error = ArgumentError.new("flaky"))
    unit = Unit.new(nil, Robin.dump_json(job))
    [Robin::Retrier.new(config).handle(unit, job, Robin::ActiveJob::Wrapper, error), unit]
  end

  def retried = redis("ZRANGE", key("retry"), 0, -1, "WITHSCORES").each_slice(2).map { |raw, s| [JSON.parse(raw), Float(s)] }

  def test_the_sidekiq_adapter_is_robins
    adapter = ActiveJob::QueueAdapters.lookup(:sidekiq)

    assert_same ActiveJob::QueueAdapters::SidekiqAdapter, adapter
    assert_operator adapter, :<, ActiveJob::QueueAdapters::RobinAdapter
    assert_instance_of adapter, GreetJob.queue_adapter
    assert_predicate adapter.new, :enqueue_after_transaction_commit?
    refute($LOADED_FEATURES.any? { |f| f.end_with?("active_job/queue_adapters/sidekiq_adapter.rb") })
  end

  def test_sidekiqs_wrapper_names_are_robins_wrapper
    assert_same Robin::ActiveJob::Wrapper, Sidekiq::ActiveJob::Wrapper
    assert_same Robin::ActiveJob::Wrapper, ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper
  end

  def test_enqueue_writes_sidekiqs_payload
    job = GreetJob.perform_later("ann")
    pushed = queued("mail").first

    assert_equal "Sidekiq::ActiveJob::Wrapper", pushed.fetch("class")
    assert_equal "ActiveJobTestJobs::GreetJob", pushed.fetch("wrapped")
    assert_equal job.provider_job_id, pushed.fetch("jid")
    assert_equal [3, ["greetings"]], pushed.values_at("retry", "tags")
    assert_equal [job.serialize.except("enqueued_at", "provider_job_id")], pushed.fetch("args").map { |a| a.except("enqueued_at", "provider_job_id") }
    perform(pushed)

    assert_equal [["ann", job.provider_job_id]], RAN
  end

  def test_enqueue_at_and_all
    GreetJob.set(wait: 60).perform_later("later")
    ActiveJob.perform_all_later([GreetJob.new("a"), GreetJob.new("b")])
    scheduled = redis("ZRANGE", key("schedule"), 0, -1).map { |raw| JSON.parse(raw) }

    assert_equal ["Sidekiq::ActiveJob::Wrapper"], scheduled.map { |j| j.fetch("class") }
    assert_equal %w[Sidekiq::ActiveJob::Wrapper Sidekiq::ActiveJob::Wrapper], queued("mail").map { |j| j.fetch("class") }
  end

  def test_jobs_sidekiq_pushed_run_on_robins_wrapper
    %w[Sidekiq::ActiveJob::Wrapper ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper].each_with_index do |wrapper, i|
      job = GreetJob.new("from sidekiq #{i}")
      perform("class" => wrapper, "wrapped" => GreetJob.name, "queue" => "mail", "jid" => "jid#{i}", "args" => [job.serialize])
    end

    assert_equal [["from sidekiq 0", "jid0"], ["from sidekiq 1", "jid1"]], RAN
  end

  def test_including_the_options
    klass = Class.new { include Sidekiq::Job::Options }

    assert_includes klass.ancestors, Robin::ActiveJob::Options
    assert_kind_of Sidekiq::Job::Options::ClassMethods, klass
    assert_equal({"queue" => "low"}, klass.sidekiq_options(queue: :low))
    assert_equal({"queue" => "low"}, klass.sidekiq_options)
  end

  def test_sidekiq_options
    assert_equal({"retry" => 3, "tags" => ["greetings"]}, GreetJob.robin_options)
    assert_equal({"retry" => 3, "queue" => "default", "tags" => ["greetings"]}, GreetJob.get_sidekiq_options)
    assert_equal({"retry" => true, "queue" => "default"}, PlainJob.get_sidekiq_options)
    assert_equal({"retry" => false, "backtrace" => 5, "lock" => {"ttl" => 1}}, Class.new(PlainJob) { sidekiq_options retry: false, backtrace: 5, lock: {ttl: 1} }.robin_options)
  end

  def test_sidekiq_retry_in_drives_the_retry_delay_counting_from_zero
    FlakyJob.perform_later
    job = queued("default").first
    # Scores keep whole milliseconds; allow for the truncation.
    now = Time.now.to_f - 0.001
    outcome, unit = fail_job(job)

    assert_equal :retry, outcome
    assert unit.acknowledged, "in the transaction that records the retry"
    entry, at = retried.first

    assert_equal 0, entry.fetch("retry_count")
    assert_operator at, :>=, now + 100
    assert_operator at, :<, now + 111
    redis("DEL", key("retry"))
    fail_job(job)
    entry, at = retried.first

    assert_equal 1, entry.fetch("retry_count")
    assert_operator at, :>=, now + 200
    assert_operator at, :<, now + 221
  end

  def test_retry_hooks_are_inherited
    block = FlakyJob.sidekiq_retry_in_block

    assert_equal 801, block.call(7, ArgumentError.new("x"), {"args" => [1, 2]})
    assert_same block, FlakierJob.sidekiq_retry_in_block
    assert_same FlakyJob.robin_retry_in_block, FlakierJob.robin_retry_in_block
    assert_same FlakyJob.robin_retries_exhausted_block, FlakierJob.sidekiq_retries_exhausted_block
    assert_nil PlainJob.sidekiq_retry_in_block
    assert_nil PlainJob.robin_retry_in_block
    assert_nil PlainJob.sidekiq_retries_exhausted_block
  end

  def test_retry_in_answers_mean_what_they_do_in_sidekiq
    assert_equal :kill, FlakyJob.robin_retry_in_block.call(1, ArgumentError.new("kill"), {"args" => []})
    assert_equal 199, FlakyJob.robin_retry_in_block.call(2, ArgumentError.new("x"), {"args" => []})
    assert_nil Class.new(PlainJob) { sidekiq_retry_in { 0 } }.robin_retry_in_block.call(1, nil, {})
  end

  def test_retries_exhausted_can_discard
    FlakyJob.perform_later
    job = queued("default").first.merge("retry" => 0)
    _, unit = fail_job(job)

    assert unit.acknowledged
    assert_equal [[:exhausted, "ActiveJobTestJobs::FlakyJob"]], RAN
    assert_equal 0, redis("ZCARD", key("dead"))
  end
end
