# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require "rbconfig"

# Requiring the testing API installs its client interceptor (sending every
# push to memory); keep it out of other tests, installing one only while
# these run. (The file may have been required already, as under mutant, so
# its interceptor can't be captured here.)
previous = Robin::Client.interceptor
require "sidekiq/testing"
Robin::Client.interceptor = previous
SIDEKIQ_TESTING_INTERCEPTOR = ->(payloads) { Sidekiq::Testing.intercept(payloads) }

module TestingTestJobs
  RAN = []
  CALLS = []

  class HardJob
    include Sidekiq::Job

    sidekiq_options queue: "critical"

    def perform(*args) = RAN << [self.class.name, jid, args, payload["queue"]]
  end

  class OtherJob
    include Sidekiq::Job

    def perform(*args) = RAN << [self.class.name, jid, args, payload["queue"]]
  end

  # Pushes itself again until +n+ reaches 0.
  class CountdownJob
    include Sidekiq::Job

    def perform(n)
      RAN << n
      self.class.perform_async(n - 1) if n.positive?
      OtherJob.perform_async(n) if n == 1
    end
  end

  class ConfigRecorder
    include Robin::Middleware::ConfigAware

    def call(*) = (CALLS << config) && yield
  end

  class Recorder
    def call(job, payload, queue)
      CALLS << [job.class.name, job.jid, payload.fetch("jid"), queue]
      yield
    end
  end
end

class TestingTest < RobinCompatTest
  cover "Sidekiq::Testing*"
  cover "Sidekiq::Queues*"
  cover "Sidekiq::Job::TestingMethods*"
  cover "Sidekiq::Job.jobs"
  cover "Sidekiq::Job.clear_all"
  cover "Sidekiq::Job.drain_all"

  include TestingTestJobs

  def setup
    super
    @previous_interceptor = Robin::Client.interceptor
    @previous_mode = Sidekiq::Testing.global_mode
    Robin::Client.interceptor = SIDEKIQ_TESTING_INTERCEPTOR
    Sidekiq::Testing.fake!
    Sidekiq::Job.clear_all
    RAN.clear
    CALLS.clear
  end

  def teardown
    Sidekiq::Testing.global_mode = @previous_mode
    Sidekiq::Testing.server_middleware.clear
    Sidekiq::Job.clear_all
    Robin::Client.interceptor = @previous_interceptor
    super
  end

  def queued(queue) = redis("LLEN", key("queue:#{queue}"))

  def ruby(code)
    Open3.capture2e(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", code)
  end

  def test_requiring_the_testing_api_switches_to_fake_mode
    out, status = ruby('require "sidekiq/testing"; p [Sidekiq::Testing.mode, Robin::Client.interceptor.nil?]')

    assert_predicate status, :success?, out
    assert_equal "[:fake, false]\n", out
  end

  def test_requiring_testing_inline_switches_to_inline_mode
    load File.expand_path("../lib/sidekiq/testing/inline.rb", __dir__)

    assert_predicate Sidekiq::Testing, :inline?
  end

  def test_global_modes_and_predicates
    assert_equal :fake, Sidekiq::Testing.fake!
    assert_equal [true, false, true, false], predicates
    assert_equal :inline, Sidekiq::Testing.inline!
    assert_equal [true, false, false, true], predicates
    assert_equal :disable, Sidekiq::Testing.disable!
    assert_equal [false, true, false, false], predicates
    assert_equal :disable, Sidekiq::Testing.__test_mode
    assert_equal :disable, Sidekiq::Testing.global_mode
  end

  def predicates = %i[enabled? disabled? fake? inline?].map { |m| Sidekiq::Testing.public_send(m) }

  def test_block_modes_last_for_the_block_in_this_thread
    other = nil
    result = Sidekiq::Testing.inline! do
      other = Thread.new { Sidekiq::Testing.mode }.value
      Sidekiq::Testing.mode
    end

    assert_equal :inline, result
    assert_equal :fake, other
    assert_equal :fake, Sidekiq::Testing.mode
    assert_nil Thread.current[Sidekiq::Testing::MODE_KEY]
    assert_equal :disable, Sidekiq::Testing.disable! { Sidekiq::Testing.mode }
    assert_equal :inline, Sidekiq::Testing.__set_test_mode(:inline) { Sidekiq::Testing.mode }
    assert_equal :fake, Sidekiq::Testing.mode
    assert_equal :inline, Sidekiq::Testing.__set_test_mode(:inline)
    assert_equal :fake, Sidekiq::Testing.fake! { Sidekiq::Testing.mode }
  end

  # A thread's mode is set while it's set to something, as #mode reads
  # it: TruffleRuby keeps the key of a fiber-local set to nil.
  def test_a_cleared_thread_mode_is_no_mode
    Thread.current[Sidekiq::Testing::MODE_KEY] = false

    assert_equal :inline, Sidekiq::Testing.inline! { Sidekiq::Testing.mode }
  ensure
    Thread.current[Sidekiq::Testing::MODE_KEY] = nil
  end

  def test_block_modes_nest_and_end_on_errors
    seen = []
    outer = Sidekiq::Testing.inline! do
      seen << Sidekiq::Testing.mode
      seen << Sidekiq::Testing.fake! { (seen << Sidekiq::Testing.mode) && :inner }
      seen << Sidekiq::Testing.disable! { (seen << Sidekiq::Testing.mode) && :off }
      (seen << Sidekiq::Testing.mode) && :outer
    end

    assert_equal [:inline, :fake, :inner, :disable, :off, :inline], seen
    assert_equal :outer, outer
    assert_equal :fake, Sidekiq::Testing.mode
    assert_operator Sidekiq::Testing::TestModeAlreadySetError, :<, RuntimeError
    assert_raises(ZeroDivisionError) { Sidekiq::Testing.disable! { 1 / 0 } }
    assert_equal :fake, Sidekiq::Testing.mode
    Sidekiq::Testing.inline! { Sidekiq::Testing.inline! } # without a block it sets the global mode

    assert_equal :inline, Sidekiq::Testing.global_mode
  end

  def test_fake_mode_keeps_pushed_jobs_in_memory
    args = {"x" => [1]}
    jid = HardJob.perform_async(args, "b")
    args["x"] << 2
    job = HardJob.jobs.first

    assert_equal 1, HardJob.jobs.size
    assert_equal [jid, "TestingTestJobs::HardJob", "critical", [{"x" => [1]}, "b"]], job.values_at("jid", "class", "queue", "args")
    assert_in_delta Robin.now_ms, job.fetch("enqueued_at"), 2_000
    assert_kind_of Integer, job.fetch("enqueued_at")
    Sidekiq.compatible_with = "7.0"
    HardJob.perform_async(1)

    assert_instance_of Float, HardJob.jobs.last.fetch("enqueued_at")
    assert_instance_of Float, HardJob.jobs.last.fetch("created_at")
    assert_in_delta Robin.now_ms / 1000.0, HardJob.jobs.last.fetch("enqueued_at"), 5
    refute job.key?("at")
    assert_same job, Sidekiq::Queues["critical"].first
    assert_equal 0, queued("critical")
    assert_empty redis("KEYS", key("*"))
  end

  def test_fake_mode_round_trips_through_json
    HardJob.perform_async({"sym" => 1.5, "nested" => {"a" => nil}})

    assert_equal [{"sym" => 1.5, "nested" => {"a" => nil}}], HardJob.jobs.first.fetch("args")
    HardJob.jobs.first.fetch("args").first["sym"] = 2

    assert_equal 2, Sidekiq::Queues["critical"].first.fetch("args").first["sym"]
  end

  def test_fake_mode_keeps_at_only_for_jobs_due_later
    HardJob.perform_in(60, 1)
    HardJob.perform_in(0, 2)
    HardJob.perform_at(Time.now - 30, 3)
    later, now, past = HardJob.jobs

    assert_in_delta Time.now.to_f + 60, later.fetch("at"), 2
    refute later.key?("enqueued_at")
    [now, past].each do |job|
      refute job.key?("at")
      assert_in_delta Robin.now_ms, job.fetch("enqueued_at"), 2_000
    end
  end

  def test_queues_by_queue_and_class
    HardJob.perform_async(1)
    OtherJob.perform_async(2)
    OtherJob.set(queue: "critical").perform_async(3)

    assert_equal [[1], [3]], Sidekiq::Queues["critical"].map { |j| j["args"] }
    assert_same Sidekiq::Queues["critical"], Sidekiq::Queues[:critical]
    assert_equal [[2], [3]], OtherJob.jobs.map { |j| j["args"] }
    assert_same Sidekiq::Queues.jobs_by_class, Sidekiq::Queues.jobs_by_worker
    assert_equal %w[critical default], Sidekiq::Queues.jobs_by_queue.keys.sort
    assert_equal [], Sidekiq::Queues["never"]
    assert_equal [[1], [2], [3]], Sidekiq::Job.jobs.map { |j| j["args"] }.sort
  end

  def test_queues_push_delete_for_clear_for_and_clear_all
    Sidekiq::Queues.push(:q, "K", {"jid" => "a"})
    Sidekiq::Queues.push("q", "K", {"jid" => "b"})
    Sidekiq::Queues.push("r", "L", {"jid" => "c"})
    Sidekiq::Queues.delete_for("a", :q, "K")

    assert_equal [{"jid" => "b"}], Sidekiq::Queues["q"]
    assert_equal [{"jid" => "b"}], Sidekiq::Queues.jobs_by_class["K"]
    Sidekiq::Queues.clear_for(:q, "K")

    assert_empty Sidekiq::Queues["q"]
    assert_empty Sidekiq::Queues.jobs_by_class["K"]
    assert_equal [{"jid" => "c"}], Sidekiq::Queues["r"]
    Sidekiq::Queues.clear_for("s", "Nope")
    Sidekiq::Queues.delete_for("c", "r", "Nope2")

    assert_empty Sidekiq::Queues["r"]
    Sidekiq::Queues.clear_all

    assert_empty Sidekiq::Queues.jobs_by_queue
    assert_empty Sidekiq::Queues.jobs_by_class
  end

  def test_class_queue_and_clear
    HardJob.perform_async(1)
    OtherJob.set(queue: "critical").perform_async(2)

    assert_equal "critical", HardJob.queue
    assert_equal "default", OtherJob.queue
    HardJob.clear

    assert_empty HardJob.jobs
    assert_empty Sidekiq::Queues["critical"]
    assert_equal 1, OtherJob.jobs.size
  end

  def test_drain_runs_jobs_in_order_including_ones_they_push
    CountdownJob.perform_async(2)
    CountdownJob.drain

    assert_equal [2, 1, 0], RAN
    assert_empty CountdownJob.jobs
    assert_empty Sidekiq::Queues["default"].select { |j| j["class"] == CountdownJob.name }
    assert_equal 1, OtherJob.jobs.size
    CountdownJob.drain

    assert_equal [2, 1, 0], RAN
  end

  def test_drain_removes_each_job_from_its_own_queue
    first = HardJob.set(queue: "a").perform_async(1)
    second = HardJob.perform_async(2)
    HardJob.drain

    assert_equal [["TestingTestJobs::HardJob", first, [1], "a"], ["TestingTestJobs::HardJob", second, [2], "critical"]], RAN
    assert_empty Sidekiq::Queues["a"]
    assert_empty Sidekiq::Queues["critical"]
  end

  def test_perform_one_runs_only_the_first_job
    first = HardJob.set(queue: "a").perform_async(1)
    HardJob.perform_async(2)
    HardJob.perform_one

    assert_equal [["TestingTestJobs::HardJob", first, [1], "a"]], RAN
    assert_equal [[2]], HardJob.jobs.map { |j| j["args"] }
    assert_empty Sidekiq::Queues["a"]
    HardJob.perform_one
    error = assert_raises(Sidekiq::EmptyQueueError) { HardJob.perform_one }

    assert_equal "perform_one called with empty job queue", error.message
    assert_equal 2, RAN.size
  end

  def test_process_job_runs_the_testing_server_middleware
    chain = Sidekiq::Testing.server_middleware

    assert_empty chain.entries
    assert_instance_of Sidekiq::Middleware::Chain, chain
    assert_same chain, Sidekiq::Testing.server_middleware { |c| c.add(Recorder) }
    job = {"jid" => "j1", "queue" => "q", "args" => [1, 2], "class" => HardJob.name}
    HardJob.process_job(job)

    assert_equal [["TestingTestJobs::HardJob", "j1", "j1", "q"]], CALLS
    assert_equal [["TestingTestJobs::HardJob", "j1", [1, 2], "q"]], RAN
  end

  def test_jobs_without_a_jid_still_run
    HardJob.process_job({"args" => [1]})
    HardJob.process_job({})

    assert_equal ["TestingTestJobs::HardJob", nil, [], nil], RAN.pop
    Sidekiq::Queues.push("q", "K", {"args" => []})
    Sidekiq::Queues.delete_for("x", "q", "K")

    assert_equal [{"args" => []}], Sidekiq::Queues["q"]
    Sidekiq::Queues.clear_all
    Sidekiq::Queues.push("q", HardJob.name, {"args" => [2]})
    HardJob.perform_one

    assert_empty HardJob.jobs
    Sidekiq::Queues.push("q", HardJob.name, {"args" => [3]})
    HardJob.drain

    assert_equal [[nil, [1], nil], [nil, [2], nil], [nil, [3], nil]], RAN.map { |r| r[1, 3] }
    assert_equal 2, Sidekiq::Queues["q"].size, "without a queue they can't be found there"
  end

  def test_testing_middleware_gets_a_config
    Sidekiq::Testing.server_middleware.add(ConfigRecorder)
    HardJob.process_job({"args" => []})

    assert_instance_of Robin::Config, CALLS.first
  end

  def test_execute_job_performs_with_the_arguments
    instance = HardJob.new
    instance.payload = {"queue" => "x"}
    HardJob.execute_job(instance, [1, "a"])

    assert_equal [["TestingTestJobs::HardJob", nil, [1, "a"], "x"]], RAN
  end

  def test_drain_all_runs_every_class_until_none_are_left
    CountdownJob.perform_async(1)
    HardJob.perform_async("x")
    Sidekiq::Job.drain_all

    assert_equal [1, 0, ["TestingTestJobs::HardJob", "x"], ["TestingTestJobs::OtherJob", 1]],
      RAN.map { |r| r.is_a?(Array) ? [r[0], r[2].first] : r }
    assert_empty Sidekiq::Job.jobs
  end

  # Each round drains the classes that had jobs when it started, in the
  # order of their first jobs.
  def test_drain_all_goes_in_rounds
    names = %w[A B C].to_h do |name|
      klass = Class.new do
        include Sidekiq::Job

        def perform(*) = RAN << self.class.name.split("::").last
      end
      [name, TestingTestJobs.const_set("DrainAll#{name}", klass)]
    end
    names["B"].define_method(:perform) do |*|
      RAN << "B"
      names["C"].set(queue: "q1").perform_async
      names["A"].set(queue: "q3").perform_async
    end
    names["A"].set(queue: "q1").perform_async
    names["B"].set(queue: "q2").perform_async
    names["A"].set(queue: "q3").perform_async
    Sidekiq::Job.drain_all

    assert_equal %w[DrainAllA DrainAllA B DrainAllC DrainAllA], RAN
  ensure
    %w[A B C].each { |n| TestingTestJobs.send(:remove_const, "DrainAll#{n}") }
  end

  def test_clear_all
    HardJob.perform_async(1)
    OtherJob.perform_async(1)
    Sidekiq::Job.clear_all

    assert_empty Sidekiq::Job.jobs
    assert_empty HardJob.jobs
  end

  def test_inline_mode_runs_jobs_now_through_json_and_middleware
    Sidekiq::Testing.server_middleware { |c| c.add(Recorder) }
    jid = Sidekiq::Testing.inline! { HardJob.perform_async({"a" => 1.5}) }

    assert_equal [["TestingTestJobs::HardJob", jid, [{"a" => 1.5}], "critical"]], RAN
    assert_equal [["TestingTestJobs::HardJob", jid, jid, "critical"]], CALLS
    assert_empty HardJob.jobs
    assert_empty redis("KEYS", key("*"))
  end

  def test_inline_mode_ignores_at
    Sidekiq::Testing.inline!
    HardJob.perform_in(3600, 1)

    assert_equal [[1]], RAN.map { |r| r[2] }
    assert_empty redis("KEYS", key("*"))
  end

  def test_disabled_mode_pushes_to_redis
    Sidekiq::Testing.disable!
    HardJob.perform_async(1)

    assert_empty HardJob.jobs
    assert_empty RAN
    assert_equal 1, queued("critical")
    assert_nil Sidekiq::Testing.intercept([{"class" => HardJob.name}])
  end

  def test_intercept_returns_the_payloads_it_took
    payloads = [{"class" => HardJob.name, "queue" => "q", "jid" => "x", "args" => []}]

    assert_same payloads, Sidekiq::Testing.intercept(payloads)
    Sidekiq::Testing.inline! { assert_same payloads, Sidekiq::Testing.intercept(payloads) }
    assert_equal 1, RAN.size
  end
end
