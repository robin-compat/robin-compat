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
# They run Sidekiq 8.1, or the Sidekiq of ROBIN_INTEROP_GEMFILE
# (test/interop/sidekiq-8.0/Gemfile for 8.0, and so on), and Robin
# follows that version (Sidekiq.compatible_with).
class InteropTest < Minitest::Test
  URL = ENV["ROBIN_INTEROP_REDIS_URL"]
  GEMFILE = File.expand_path(ENV.fetch("ROBIN_INTEROP_GEMFILE", File.join(__dir__, "interop/Gemfile")))
  # The version of Sidekiq the other process runs.
  SIDEKIQ = Bundler::LockfileParser.new(File.read("#{GEMFILE}.lock")).specs.find { |s| s.name == "sidekiq" }.version.to_s

  def setup
    skip "set ROBIN_INTEROP_REDIS_URL to a Redis database these tests may flush" unless URL
    Sidekiq.compatible_with = SIDEKIQ
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
  # Outside this process's bundle: its gem path (vendor/bundle on CI)
  # isn't where the other Gemfile's gems are. The lockfile is named too,
  # or the child would rewrite this bundle's one for the other Gemfile.
  def sidekiq(script, env = {})
    bundle = {"BUNDLE_GEMFILE" => GEMFILE, "BUNDLE_LOCKFILE" => "#{GEMFILE}.lock"}
    out, err, status = Bundler.with_unbundled_env do
      Open3.capture3(bundle.merge("REDIS_URL" => URL).merge(env),
        "bundle", "exec", "ruby", File.expand_path("interop/#{script}", __dir__))
    end

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
    # Sidekiq 8 records times in epoch milliseconds, and Sidekiq 7 in seconds.
    assert_kind_of Sidekiq.compatible_before?("8.0") ? Float : Integer, retried.fetch("failed_at")
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

  # Robin and the real sidekiq-cron share cron jobs: each reads the
  # other's, and neither enqueues a tick the other did.
  def test_cron_jobs_are_shared_with_sidekiq_cron
    claimed = Time.at(((Time.now.to_i / 60) + 1) * 60 + 1).utc # a second into the next minute
    tick = claimed + 60

    assert_true Sidekiq::Cron::Job.create(name: "from robin", cron: "* * * * *", class: "InteropJob", args: [1, "two"],
      queue: "cron", description: "made by Robin")
    Sidekiq::Cron::Job.find("from robin").test_and_enqueue_for_time!(claimed)
    enqueued_at = Time.now.to_i
    read = sidekiq("sidekiq_cron.rb", "CLAIMED" => claimed.to_f.to_s, "TICK" => tick.to_f.to_s)

    assert_equal ["from robin"], read.fetch("names")
    assert_equal({"name" => "from robin", "cron" => "* * * * *", "klass" => "InteropJob", "args" => [1, "two"],
      "status" => "enabled", "source" => "dynamic", "description" => "made by Robin", "queue" => "cron",
      "history" => 1}, read.fetch("job").except("last_enqueue_time"))
    assert_in_delta enqueued_at, read.fetch("job").fetch("last_enqueue_time"), 2
    assert_false read.fetch("claims_robins_tick")
    assert_equal 1, read.fetch("enqueued")
    theirs = Sidekiq::Cron::Job.find("from sidekiq-cron")

    assert_equal ["* * * * *", "InteropJob", ["x"], "enabled", "made by sidekiq-cron", "3"],
      [theirs.cron, theirs.klass, theirs.args, theirs.status, theirs.description, theirs.to_hash.fetch(:retry)]
    assert_equal 1, theirs.jid_history_from_redis.size
    assert_false theirs.should_enqueue?(tick)
    queued = Robin.redis { |c| c.call("LRANGE", "queue:cron", 0, -1) }.map { |raw| JSON.parse(raw).values_at("class", "args", "retry") }

    assert_equal [["InteropJob", ["x"], "3"], ["InteropJob", [1, "two"], true]], queued
  end

  # Robin and the real sidekiq-scheduler share the schedule: each reads
  # the other's jobs and their states, and neither pushes a tick the other
  # did.
  def test_schedules_are_shared_with_sidekiq_scheduler
    tick = Time.at(((Time.now.to_i / 60) + 1) * 60) # the next minute
    Sidekiq.schedule = {"from robin" => {"cron" => "* * * * *", "class" => "InteropJob", "args" => [1, "two"], "queue" => "scheduled"},
                        "disabled by robin" => {"every" => "1h", "class" => "InteropJob"}}
    scheduler = SidekiqScheduler::Scheduler.new(SidekiqScheduler::Config.new)
    scheduler.load_schedule!
    scheduler.clear_schedule! # this test pushes the ticks itself
    scheduler.load_schedule_job("from robin", Sidekiq.schedule.fetch("from robin"))
    scheduler.toggle_job_enabled("disabled by robin")
    scheduler.enqueue_due(tick + 0.5)
    read = sidekiq("sidekiq_scheduler.rb", "CLAIMED" => tick.to_i.to_s, "TICK" => (tick + 60).to_i.to_s)

    assert_equal Sidekiq.get_schedule.except("from sidekiq-scheduler"), read.fetch("schedule")
    assert_equal({"disabled by robin" => false, "from robin" => true}, read.fetch("enabled"))
    assert_false read.fetch("pushes_robins_tick")
    assert_equal [tick.to_i, tick.to_i + 60], read.values_at("last_time", "next_time")
    assert_equal ["disabled by robin", "from robin"], read.fetch("changed")
    Sidekiq.reload_schedule!

    assert_equal({"cron" => "* * * * *", "class" => "InteropJob", "args" => ["x"], "queue" => "scheduled"},
      Sidekiq.get_schedule("from sidekiq-scheduler"))
    assert_false scheduler.job_enabled?("from sidekiq-scheduler")
    assert_false SidekiqScheduler::RedisManager.register_job_instance("from sidekiq-scheduler", tick + 60)
    queued = Robin.redis { |c| c.call("LRANGE", "queue:scheduled", 0, -1) }.map { |raw| JSON.parse(raw).values_at("class", "args") }

    assert_equal [["InteropJob", ["x"]], ["InteropJob", [1, "two"]]], queued
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
