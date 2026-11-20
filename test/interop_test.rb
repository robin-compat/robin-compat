# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require "sidekiq/api"

module InteropRuns
  RAN = Thread::Queue.new
end

# Names the real Sidekiq used when it pushed them (see interop/*.rb).
class InteropJob
  include Sidekiq::Job

  def perform(*args) = InteropRuns::RAN << [:interop, args]
end

class FlakyJob
  include Sidekiq::Job

  sidekiq_options retry: 3, backtrace: 5

  FAILED = []

  # Fails the first time it runs on Robin (in the Robin-to-Sidekiq test).
  def perform(*args)
    raise "robin boom" if args == ["robin"] && FAILED.empty? && FAILED << jid

    InteropRuns::RAN << [:flaky, args]
  end
end

class SlowJob
  include Sidekiq::Job

  GATE = Thread::Queue.new

  def perform = GATE.pop
end

# Robin and the real Sidekiq (a separate process, with the sidekiq gem)
# share one Redis database: each reads, runs and acts on what the other
# wrote. Needs ROBIN_INTEROP_REDIS_URL, a database these tests may flush.
class InteropTest < Minitest::Test
  URL = ENV["ROBIN_INTEROP_REDIS_URL"]
  GEMFILE = File.expand_path("interop/Gemfile", __dir__)

  def setup
    skip "set ROBIN_INTEROP_REDIS_URL to a Redis database these tests may flush" unless URL
    Robin.config = Sidekiq.defaults!(Robin::Config.new)
    Robin.config.redis = {url: URL}
    Robin.config.logger = Logger.new(IO::NULL)
    Robin.config.heartbeat_interval = 0.2
    Robin.config.poll_interval = 0.1
    Robin.config.fetch_timeout = 0.2
    Sidekiq.instance_variable_set(:@config, nil)
    Robin.redis { |c| c.call("FLUSHDB") }
    InteropRuns::RAN.clear
    FlakyJob::FAILED.clear
  end

  def teardown
    @launcher&.stop
    Robin.redis { |c| c.call("FLUSHDB") } if URL
    Robin.instance_variable_set(:@server, false)
  end

  # Runs a script with the real Sidekiq; returns what it printed (JSON).
  # The lockfile is named too: `bundle exec` passes this process's own
  # down, and the child would rewrite it for the other Gemfile.
  def sidekiq(script, env = {})
    bundle = {"BUNDLE_GEMFILE" => GEMFILE, "BUNDLE_LOCKFILE" => "#{GEMFILE}.lock"}
    out, err, status = Open3.capture3(bundle.merge("REDIS_URL" => URL).merge(env),
      "bundle", "exec", "ruby", File.expand_path("interop/#{script}", __dir__))

    assert_predicate status, :success?, err
    JSON.parse(out)
  end

  def serve(*queues)
    Robin.server!
    Robin.config.queues = queues
    Robin.config.concurrency = 3
    @launcher = Robin::Launcher.new(Robin.config).run
  end

  def ran(count, timeout: 10)
    Array.new(count) { Timeout.timeout(timeout) { InteropRuns::RAN.pop } }
  end

  def eventually(timeout: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until (result = yield)
      flunk "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.05
    end
    result
  end

  # What Sidekiq's API read of Robin's queues and sets.
  def assert_reads_jobs(read, pushed)
    assert_equal({"enqueued" => 1, "scheduled" => 1, "retries" => 1, "dead" => 0, "processes" => 1, "workers" => 1},
      read.fetch("stats").slice("enqueued", "scheduled", "retries", "dead", "processes", "workers"))
    assert_includes read.fetch("queues"), "default"
    queue = read.fetch("queue")

    assert_equal [1, "InteropJob", "InteropJob", [1, "two"]], queue.values_at("size", "class", "display_class", "args")
    assert_in_delta pushed, queue.fetch("enqueued_at"), 5
    assert_in_delta pushed, queue.fetch("created_at"), 5
    assert_operator queue.fetch("latency"), :<, 30
    assert_equal ["InteropJob", [3]], read.fetch("scheduled").values_at("class", "args")
    assert_in_delta pushed + 3600, read.fetch("scheduled").fetch("at"), 5
    retried = read.fetch("retry")

    assert_equal ["FlakyJob", 0, "RuntimeError", "robin boom"], retried.values_at("class", "retry_count", "error_class", "error_message")
    assert_match(/interop_test\.rb/, retried.fetch("backtrace").first)
    assert_kind_of Integer, retried.fetch("failed_at")
  end

  # What Sidekiq's API read of Robin's process and its running job.
  def assert_reads_the_process(read)
    process = read.fetch("processes").first

    assert_equal [@launcher.identity, 1, Robin.config.total_concurrency, Socket.gethostname, "false"],
      process.values_at("identity", "busy", "concurrency", "hostname", "quiet")
    assert_equal %w[failing busy], process.fetch("queues")
    assert_in_delta Time.now.to_f, process.fetch("beat"), 5
    assert_in_delta Time.now.to_f, process.fetch("started_at"), 30
    identity, _thread, work_queue, klass, run_at = read.fetch("work").first

    assert_equal [@launcher.identity, "busy", "SlowJob"], [identity, work_queue, klass]
    assert_in_delta Time.now.to_f, run_at, 30
  end

  def test_robin_reads_and_runs_what_sidekiq_wrote
    written = sidekiq("sidekiq_writes.rb")
    now = Time.now.to_f
    stats = Sidekiq::Stats.new

    assert_equal [2, 1, 1, 1], [stats.enqueued, stats.scheduled_size, stats.retry_size, stats.dead_size]
    job = Sidekiq::Queue.new("default").first

    assert_equal [written.fetch("queued"), "InteropJob", [1, "two"]], [job.jid, job.klass, job.args]
    assert_in_delta now, job.enqueued_at.to_f, 30
    scheduled = Sidekiq::ScheduledSet.new.first

    assert_equal written.fetch("scheduled"), scheduled.jid
    assert_in_delta now + 3600, scheduled.at.to_f, 30
    retry_entry = Sidekiq::RetrySet.new.first

    assert_equal [written.fetch("retry"), 0, 1, "RuntimeError", "sidekiq boom"],
      [retry_entry.jid, retry_entry["retry_count"], retry_entry.failures, retry_entry["error_class"], retry_entry["error_message"]]
    assert_kind_of Array, retry_entry.error_backtrace
    assert_operator retry_entry.error_backtrace.size, :<=, 5
    assert_match(/sidekiq_writes\.rb/, retry_entry.error_backtrace.first)
    assert_operator retry_entry.at.to_f, :>, now
    dead = Sidekiq::DeadSet.new.first

    assert_equal [written.fetch("dead"), "sidekiq dies"], [dead.jid, dead["error_message"]]

    serve("default", "other", "failing")
    retry_entry.retry
    scheduled.reschedule(Time.now)
    runs = ran(4)

    assert_equal [[:flaky, ["flaky"]], [:interop, [1, "two"]], [:interop, [3]], [:interop, [{"k" => "v"}]]], runs.sort_by(&:inspect)
  end

  def test_sidekiq_runs_what_robin_pushed_after_a_rollback
    InteropJob.set(queue: "back").perform_async(1, {"k" => "v"})
    InteropJob.set(queue: "back").perform_in(1, 2)
    InteropJob.set(queue: "back").perform_bulk([[3], [4]])
    ran = sidekiq("sidekiq_runs.rb", "EXPECTED" => "4")

    assert_equal [[1, {"k" => "v"}], [2], [3], [4]], ran.sort_by(&:first)
  end

  def test_sidekiq_reads_and_acts_on_what_robin_wrote
    serve("failing", "busy")
    FlakyJob.set(queue: "failing").perform_async("robin")
    eventually { Sidekiq::RetrySet.new.size == 1 }
    SlowJob.set(queue: "busy").perform_async
    eventually { Robin::WorkSet.new.any? }
    InteropJob.perform_async(1, "two")
    InteropJob.perform_in(3600, 3)
    pushed = Time.now.to_f
    read = sidekiq("sidekiq_reads.rb")

    assert_reads_jobs(read, pushed)
    assert_reads_the_process(read)

    # What Sidekiq did: retried the failed job (Robin runs it), killed the
    # scheduled one and quieted Robin's process.
    assert_equal [[:flaky, ["robin"]]], ran(1)
    assert_equal [3], Sidekiq::DeadSet.new.first.args
    assert_equal 0, Sidekiq::ScheduledSet.new.size
    eventually { @launcher.instance_variable_get(:@quiet) }
  ensure
    SlowJob::GATE << :go
  end
end
