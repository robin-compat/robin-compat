# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq/api"
require "date"

class ApiTest < RobinCompatTest
  cover "Sidekiq::Stats*"
  cover "Sidekiq::JobRecord*"
  cover "Sidekiq::Queue*"
  cover "Sidekiq::SortedEntry*"
  cover "Sidekiq::JobSet*"
  cover "Sidekiq::DeadSet*"
  cover "Sidekiq::Process*"
  cover "Sidekiq::Work*"

  def job(jid, extra = {}) = {"class" => "HardJob", "args" => [jid], "queue" => "default", "jid" => jid}.merge(extra)

  def enqueue(queue, *payloads)
    redis("SADD", key("queues"), queue)
    payloads.each { |p| redis("LPUSH", key("queue:#{queue}"), JSON.generate(p)) }
  end

  def add(set, score, payload) = redis("ZADD", key(set), score, JSON.generate(payload))

  def members(set) = redis("ZRANGE", key(set), 0, -1).map { |raw| JSON.parse(raw) }

  def queued(queue) = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  def capsules = {"default" => {"concurrency" => 5, "mode" => "weighted", "weights" => {"a" => 2, "b" => 1}},
                  "solo" => {"concurrency" => 1, "mode" => "strict", "weights" => {"b" => 3, "c" => 0}}}

  # A process record as Robin's heartbeat writes it.
  def beat(identity, info: {}, busy: 0, beat: 1_700_000_000.25, quiet: false, rss: 0, work: {}, prefix: key_prefix_for_test)
    info = {"identity" => identity, "hostname" => "h", "concurrency" => 6, "tag" => "app", "labels" => ["x"],
            "version" => "1.0", "embedded" => false, "queues" => %w[zz], "weights" => [{"a" => 2}],
            "capsules" => capsules}.merge(info)
    redis("SADD", "#{prefix}processes", identity)
    redis("HSET", "#{prefix}#{identity}", "info", JSON.generate(info), "beat", beat, "busy", busy,
      "concurrency", 6, "quiet", quiet, "rss", rss, "rtt_us", 42, "leader", "0")
    work.each do |tid, (queue, payload, run_at)|
      redis("HSET", "#{prefix}#{identity}:work", tid,
        JSON.generate({"queue" => queue, "payload" => payload, "run_at" => run_at}))
    end
  end

  def with_tz(zone)
    previous = ENV.fetch("TZ", nil)
    ENV["TZ"] = zone
    yield
  ensure
    ENV["TZ"] = previous
  end

  def handlers
    calls = []
    config.death_handlers << ->(job, ex) { calls << [job.fetch("jid"), ex] }
    calls
  end

  # Stats

  def test_stats
    redis("SET", key("stat:processed"), 7)
    redis("SET", key("stat:failed"), 2)
    enqueue("default", job("a"))
    add("retry", 1, job("r"))
    stats = assert_roundtrips(1) { Sidekiq::Stats.new }

    assert_equal [7, 2, 1, 1], [stats.processed, stats.failed, stats.enqueued, stats.retry_size]
    redis("SET", key("stat:processed"), 9)

    assert_same stats, stats.fetch_stats!
    assert_equal 9, stats.processed
    redis("SET", key("stat:processed"), 10)
    stats.fetch_stats_fast!

    assert_equal 10, stats.processed
    redis("SET", key("stat:processed"), 11)
    stats.fetch_stats_slow!

    assert_equal 11, stats.processed
  end

  def test_queue_summaries_largest_first
    assert_empty Sidekiq::Stats.new.queue_summaries
    enqueue("small", job("a", "enqueued_at" => Robin.now_ms - 10_000))
    enqueue("big", job("b", "enqueued_at" => Robin.now_ms - 20_000), job("c"))
    enqueue("also", job("d"), job("e"))
    redis("SADD", key("queues"), "empty")
    redis("SADD", key("paused"), "big")
    summaries = Sidekiq::Stats.new.queue_summaries

    assert_equal [["also", 2, false], ["big", 2, true], ["small", 1, false], ["empty", 0, false]],
      summaries.map { |s| [s.name, s.size, s.paused?] }
    assert_in_delta 10.0, summaries[2].latency, 2
    assert_in_delta 20.0, summaries[1].latency, 2
    assert_in_delta 0.0, summaries[3].latency
    assert_true summaries[1].paused
  end

  def stat(name) = redis("GET", key("stat:#{name}"))

  def set_stats = %w[processed failed other].each { |n| redis("SET", key("stat:#{n}"), 5) }

  def test_reset
    set_stats
    Sidekiq::Stats.new.reset

    assert_equal [nil, nil, "5"], %w[processed failed other].map { |n| stat(n) }
    set_stats
    Sidekiq::Stats.new.reset(:failed, "other")

    assert_equal ["5", nil, "5"], %w[processed failed other].map { |n| stat(n) }
    set_stats
    Sidekiq::Stats.new.reset(["processed"])

    assert_equal [nil, "5", "5"], %w[processed failed other].map { |n| stat(n) }
    set_stats

    assert_nil Sidekiq::Stats.new.reset("other")
    assert_equal %w[5 5 5], %w[processed failed other].map { |n| stat(n) }
  end

  def test_history
    redis("SET", key("stat:processed:2024-03-10"), 3)
    redis("SET", key("stat:processed:2024-03-09"), 2)
    redis("SET", key("stat:failed:2024-03-08"), 1)
    history = with_tz("Pacific/Auckland") { Sidekiq::Stats::History.new(3, Date.new(2024, 3, 10)) }

    assert_equal({"2024-03-10" => 3, "2024-03-09" => 2, "2024-03-08" => 0}, history.processed)
    assert_equal({"2024-03-10" => 0, "2024-03-09" => 0, "2024-03-08" => 1}, history.failed)
    today = Time.now.utc.strftime("%Y-%m-%d")
    redis("SET", key("stat:failed:#{today}"), 4)

    assert_equal({today => 4}, Sidekiq::Stats::History.new(1).failed)
    assert_equal 1825, Sidekiq::Stats::History.new(1825, Date.new(2024, 1, 10)).processed.size
  end

  def test_history_range
    [0, 1826].each do |days|
      error = assert_raises(ArgumentError) { Sidekiq::Stats::History.new(days) }

      assert_equal "days_previous must be between 1 and 1825", error.message
    end
  end

  # JobRecord

  def test_iterable_state
    record = Sidekiq::JobRecord.new(job("it"))

    assert_nil record.iterable_state
    redis("HSET", "#{key_prefix_for_test}robin:iter:it", "cursor", JSON.generate({"c" => [1, "x"]}), "iterations", "3")
    state = record.iterable_state

    assert_equal ["it", [1, "x"], 3], [state.jid, state.cursor, state.iterations]
    assert_equal "3", state.raw.fetch("iterations")
  end

  def test_created_at_falls_back_to_enqueued_at_then_the_epoch
    assert_equal Time.at(1.5), Sidekiq::JobRecord.new(job("a", "created_at" => 1500, "enqueued_at" => 2500)).created_at
    assert_equal Time.at(2.5), Sidekiq::JobRecord.new(job("a", "enqueued_at" => 2500)).created_at
    assert_equal Time.at(0), Sidekiq::JobRecord.new(job("a")).created_at
  end

  # Queue

  def test_queue_all_and_id
    enqueue("b", job("x"))
    enqueue("a", job("y"), job("z"))
    other = Robin::Config.new
    other.key_prefix = "#{key_prefix_for_test}other:"
    other.redis = {url: REDIS_URL}
    redis("SADD", "#{other.key_prefix}queues", "c")
    redis("LPUSH", "#{other.key_prefix}queue:c", "{}", "{}", "{}")
    queues = Sidekiq::Queue.all

    assert_equal %w[a b], queues.map(&:id)
    assert_equal [Sidekiq::Queue], queues.map(&:class).uniq
    assert_equal [2, 1], queues.map(&:size)
    assert_equal [["c", 3]], Sidekiq::Queue.all(config: other).map { |q| [q.name, q.size] }
  ensure
    other.redis_pool.shutdown
  end

  def test_queue_each_starts_with_the_newest_job
    enqueue("default", job("first"), job("second"), job("third", "queue" => "elsewhere"))
    queue = Sidekiq::Queue.new
    seen = []
    queue.each { |j| seen << j }

    assert_equal %w[third second first], seen.map(&:jid)
    assert_equal [Sidekiq::JobRecord], seen.map(&:class).uniq
    assert_equal ["default"], seen.map(&:queue).uniq
    assert_equal "elsewhere", seen.first["queue"]
    assert_equal %w[third second first], queue.each.map(&:jid)
    assert_equal "first", queue.find_job("first").jid
    assert_instance_of Sidekiq::JobRecord, queue.find_job("first")
    assert_equal "third", queue.first.jid
  end

  def test_queue_each_lets_the_block_delete
    enqueue("default", *(1..5).map { |n| job("j#{n}") })
    Sidekiq::Queue.new.each { |j| j.delete if j.jid != "j3" }

    assert_equal %w[j3], queued("default").map { |j| j["jid"] }
  end

  # Sorted sets

  def test_set_names
    assert_equal %w[schedule retry dead retry], [Sidekiq::ScheduledSet.new, Sidekiq::RetrySet.new, Sidekiq::DeadSet.new,
      Sidekiq::SortedSet.new("retry")].map(&:name)
    assert_same Sidekiq::JobSet, Sidekiq::SortedSet
    assert_equal key("retry"), Sidekiq::JobSet.new("retry").key
  end

  def test_each_starts_with_the_latest_score
    add("retry", 10, job("a"))
    add("retry", 30, job("c"))
    add("retry", 20, job("b"))
    set = Sidekiq::RetrySet.new
    seen = []
    set.each { |e| seen << e }

    assert_equal %w[c b a], seen.map(&:jid)
    assert_equal [Sidekiq::SortedEntry], seen.map(&:class).uniq
    assert_equal %w[c b a], set.each.map(&:jid)
    assert_equal [30.0, 20.0, 10.0], set.map(&:score)
  end

  def test_sorted_entry_at_and_error
    add("retry", 1_700_000_000.5, job("a", "error_class" => "Boom"))
    add("retry", 1_700_000_001, job("b"))
    a, b = Sidekiq::RetrySet.new.to_a.reverse

    assert_equal Time.at(1_700_000_000.5).utc, a.at
    assert_predicate a.at, :utc?
    assert_equal [true, false], [a.error?, b.error?]
    assert_nil a.iterable_state
    assert_equal Time.at(0), b.created_at
  end

  def test_scan
    add("retry", 1, job("abc"))
    add("retry", 2, job("abd"))
    add("retry", 3, job("xyz"))
    set = Sidekiq::RetrySet.new

    assert_equal %w[abc abd], set.scan("ab").map(&:jid).sort
    assert_equal %w[abc], set.scan("*\"abc\"*").map(&:jid)
    assert_empty set.scan("ab*")
    assert_equal [Sidekiq::SortedEntry], set.scan("ab", 1).map(&:class).uniq
    yielded = []
    result = set.scan("x") { |e| yielded << e.jid }

    assert_equal %w[xyz], yielded
    assert_equal [3.0], result.map(&:score)
  end

  def test_scan_pages_through_large_sets
    300.times { |n| add("retry", n, job("j#{n}")) }

    assert_equal 300, Sidekiq::RetrySet.new.scan("j", 10).size
    # COUNT is only a hint to Redis; a bad one shows it gets there.
    error = assert_raises(StandardError) { Sidekiq::RetrySet.new.scan("j", "many") }

    assert_match(/not an integer/, error.message)
    assert_equal 11, Sidekiq::RetrySet.new.scan("j1", 10).count { |e| e.jid.start_with?("j1") && e.jid.size <= 3 }
  end

  def test_fetch
    add("schedule", 100, job("a"))
    add("schedule", 100, job("b"))
    add("schedule", 200.5, job("c"))
    add("schedule", 300, job("d"))
    add("schedule", -5, job("neg"))
    set = Sidekiq::ScheduledSet.new

    assert_equal %w[a b], set.fetch(100).map(&:jid)
    assert_equal %w[b], set.fetch(100, "b").map(&:jid)
    assert_empty set.fetch(100, "c")
    assert_equal %w[c], set.fetch(Time.at(200.5)).map(&:jid)
    assert_equal %w[c], set.fetch(Class.new(Time).at(200.5)).map(&:jid)
    assert_equal %w[c], set.fetch("200.5").map(&:jid)
    assert_equal %w[c d], set.fetch(150..300).map(&:jid)
    assert_equal %w[a b c], set.fetch(Time.at(100)..Time.at(250)).map(&:jid)
    assert_equal %w[c d], set.fetch(150..).map(&:jid)
    assert_equal %w[neg a b c], set.fetch(..250).map(&:jid)
    assert_equal %w[neg], set.fetch(-10..-1).map(&:jid)
    assert_equal [Sidekiq::SortedEntry], set.fetch(100).map(&:class).uniq
  end

  def test_delete_by_value_and_jid
    add("retry", 5, job("a"))
    add("retry", 5, job("b"))
    set = Sidekiq::RetrySet.new

    assert set.delete_by_value("retry", JSON.generate(job("a")))
    refute set.delete_by_value("retry", JSON.generate(job("a")))
    refute set.delete_by_jid(6, "b")
    assert set.delete(5, "b")
    refute set.delete_by_jid(5, "b")
    assert_equal 0, set.size
  end

  def test_retry_pushes_back_without_counting_a_retry
    add("retry", 5, job("a", "retry_count" => 2, "at" => 4, "enqueued_at" => 1))
    add("retry", 6, job("b"))
    a, b = Sidekiq::RetrySet.new.fetch(5..6)

    assert a.retry
    refute a.retry
    assert b.retry
    pushed = queued("default").reverse

    assert_equal [["a", 1], ["b", nil]], pushed.map { |j| j.values_at("jid", "retry_count") }
    refute pushed.first.key?("at")
    refute_equal 1, pushed.first.fetch("enqueued_at")
    assert_equal 0, Sidekiq::RetrySet.new.size
  end

  def test_retry_all
    add("retry", 1, job("a", "retry_count" => 0))
    add("retry", 2, job("b", "retry_count" => "3"))

    # The set's size, the page, the jobs taken, the jobs pushed.
    assert_equal 2, assert_roundtrips(4) { Sidekiq::RetrySet.new.retry_all }
    assert_equal [["a", -1], ["b", 2]], queued("default").reverse.map { |j| j.values_at("jid", "retry_count") }
    assert_equal 0, Sidekiq::RetrySet.new.retry_all
  end

  def test_kill_runs_the_death_handlers
    calls = handlers
    add("retry", 1, job("a"))
    add("retry", 2, job("b"))
    a, b = Sidekiq::RetrySet.new.fetch(1..2)

    assert a.kill
    refute a.kill
    error = RuntimeError.new("x")

    assert b.kill(notify_failure: false, ex: error)
    assert_equal %w[a b], members("dead").map { |j| j["jid"] }
    assert_equal ["a"], calls.map(&:first)
    ex = calls.first.last

    assert_instance_of RuntimeError, ex
    assert_equal "Job killed by API", ex.message
    refute_nil ex.backtrace
    assert_equal 0, Sidekiq::RetrySet.new.size
  end

  def test_kill_passes_its_error
    calls = handlers
    add("retry", 1, job("a"))
    error = RuntimeError.new("x")
    Sidekiq::RetrySet.new.first.kill(ex: error)

    assert_same error, calls.first.last
  end

  def test_kill_all
    calls = handlers
    add("retry", 1, job("a"))
    add("retry", 2, job("b"))

    assert_equal 2, assert_roundtrips(4) { Sidekiq::RetrySet.new.kill_all }
    assert_empty calls
    add("schedule", 3, job("c"))
    error = RuntimeError.new("x")

    assert_equal 1, Sidekiq::ScheduledSet.new.kill_all(notify_failure: true, ex: error)
    assert_equal [["c", error]], calls
    assert_equal %w[a b c], members("dead").map { |j| j["jid"] }.sort
  end

  def test_dead_set_kill
    calls = handlers
    dead = Sidekiq::DeadSet.new

    assert dead.kill(JSON.generate(job("a")))
    assert dead.kill(JSON.generate(job("b")), notify_failure: false)
    error = RuntimeError.new("x")

    assert dead.kill(JSON.generate(job("c")), {notify_failure: true, ex: error})
    assert_equal %w[a c], calls.map(&:first)
    assert_equal "Job killed by API", calls.first.last.message
    assert_same error, calls.last.last
    assert_in_delta Time.now.to_f, dead.fetch(0..).first.score, 2
    assert_equal 3, dead.size
  end

  def test_schedule
    set = Sidekiq::ScheduledSet.new
    at = Time.now.to_f + 600
    set.schedule(at, job("a", "at" => 1))
    entry = set.first

    assert_in_delta at, entry.score, 0.01
    assert_equal job("a"), entry.item
  end

  # Processes

  def test_process_readers
    beat("host:1", busy: 3, rss: 2048, quiet: true)
    process = Sidekiq::ProcessSet.new.first

    assert_instance_of Sidekiq::Process, process
    assert_equal [3, 1_700_000_000.25, "true", 2048, 42], %w[busy beat quiet rss rtt_us].map { |k| process[k] }
    assert_equal [6, "host:1", "h"], %w[concurrency identity hostname].map { |k| process[k] }
    assert_nil process["nope"]
    assert_equal ["host:1", "host:1", "app", ["x"], "1.0"], [process.id, process.identity, process.tag, process.labels, process.version]
    assert_equal %w[a b c], process.queues
    assert_equal({"a" => 2, "b" => 3, "c" => 0}, process.weights)
    assert_equal capsules, process.capsules
    assert_equal [false, true], [process.embedded?, process.stopping?]
  end

  def test_process_without_capsules_or_labels
    beat("host:2", info: {"capsules" => nil, "labels" => nil, "embedded" => true, "queues" => %w[q], "weights" => [{"q" => 1}]})
    process = Sidekiq::ProcessSet.new.first

    assert_equal "false", process["quiet"]
    assert_equal [[], %w[q], [{"q" => 1}], {}], [process.labels, process.queues, process.weights, process.capsules]
    assert_equal [true, false], [process.embedded?, process.stopping?]
    beat("host:3", info: {"capsules" => nil, "queues" => nil, "weights" => nil, "embedded" => nil})
    bare = Sidekiq::ProcessSet["host:3"]

    assert_equal [[], [], false], [bare.queues, bare.weights, bare.embedded?]
  end

  def signals(identity) = redis("LRANGE", "#{key_prefix_for_test}#{identity}-signals", 0, -1)

  def test_quiet_and_stop
    beat("host:1")
    beat("host:2", info: {"embedded" => true})
    process, embedded = Sidekiq::ProcessSet.new.to_a

    assert process.quiet!
    assert process.stop!
    assert_equal %w[TERM TSTP], signals("host:1")
    assert_equal "Can't quiet an embedded process", assert_raises(RuntimeError) { embedded.quiet! }.message
    assert_equal "Can't stop an embedded process", assert_raises(RuntimeError) { embedded.stop! }.message
    assert_empty signals("host:2")
  end

  def test_process_set_lookup
    beat("host:1")
    beat("host:2")
    redis("SADD", key("processes"), "gone")

    assert_equal "host:2", Sidekiq::ProcessSet["host:2"].identity
    assert_instance_of Sidekiq::Process, Sidekiq::ProcessSet["host:2"]
    assert_nil Sidekiq::ProcessSet["gone"]
    assert_equal 1, redis("SISMEMBER", key("processes"), "gone"), "lookup doesn't clean up"
  end

  def test_process_set_cleans_up_once_a_minute
    beat("host:1")
    redis("SADD", key("processes"), "gone", "gone2")
    Sidekiq::ProcessSet.new(false)

    assert_equal 3, redis("SCARD", key("processes"))
    set = Sidekiq::ProcessSet.new

    assert_equal ["host:1"], redis("SMEMBERS", key("processes"))
    assert_equal ["host:1"], set.map(&:identity)
    redis("SADD", key("processes"), "gone")

    assert_equal 0, set.cleanup
    redis("DEL", "#{key_prefix_for_test}robin:process_cleanup")

    assert_equal 1, set.cleanup
    redis("DEL", "#{key_prefix_for_test}robin:process_cleanup")

    assert_equal 0, set.cleanup
    assert_equal "1", redis("GET", "#{key_prefix_for_test}robin:process_cleanup")
    assert_equal 60, redis("TTL", "#{key_prefix_for_test}robin:process_cleanup")
  end

  def test_process_set_totals_and_leader
    beat("host:1", rss: 100)
    beat("host:2", rss: 50)
    set = Sidekiq::ProcessSet.new

    assert_equal [150, 150], [set.total_rss_in_kb, set.total_rss]
    assert_equal "", set.leader
    redis("SET", "#{key_prefix_for_test}robin:leader", "host:2")

    assert_equal "host:2", set.leader
  end

  # Work

  def test_work_set_longest_running_first
    a = job("a")
    beat("host:1", busy: 2, work: {"t1" => ["default", a, 1_700_000_100.5], "t2" => ["low", JSON.generate(job("b")), 1_700_000_000]})
    beat("host:2", busy: 1, work: {"t3" => ["default", job("c"), 1_700_000_050]})
    works = Sidekiq::WorkSet.new
    seen = []
    works.each { |pid, tid, work| seen << [pid, tid, work.process_id, work.thread_id, work.job.jid] }

    assert_equal [%w[host:1 t2 host:1 t2 b], %w[host:2 t3 host:2 t3 c], %w[host:1 t1 host:1 t1 a]], seen
    work = works.find_work("a")

    assert_instance_of Sidekiq::Work, work
    assert_equal ["default", a, Time.at(1_700_000_100.5)], [work.queue, JSON.parse(work.payload), work.run_at]
    assert_instance_of String, work.payload
    assert_instance_of Sidekiq::JobRecord, work.job
    assert_equal "a", work.job.jid
    assert_equal "low", works.find_work_by_jid("b").job.queue
    assert_nil works.find_work("nope")
    assert_equal %w[b c a], works.each.map { |*, w| w.job.jid }
    works.each { |*args| assert_equal 3, args.size }
    assert_equal 3, works.size
    assert_same Sidekiq::WorkSet, Sidekiq::Workers
  end

  # Sidekiq 7's work, a Hash: Sidekiq 7.0 and 7.1 yield it, 7.2 and 7.3
  # deprecate it, and 8.0 drops it.
  def test_work_is_also_sidekiq_7s_hash
    a = job("a")
    beat("host:1", busy: 1, work: {"t1" => ["default", a, 1_700_000_100.5]})
    work = Sidekiq::WorkSet.new.first.last
    hash = {"queue" => "default", "payload" => work.payload, "run_at" => 1_700_000_100}

    assert_equal hash, work.to_h
    assert_equal [hash["queue"], hash["payload"], 1_700_000_100], [work["queue"], work["payload"], work.raw("run_at")]
    assert_kind_of Integer, work["run_at"]
    assert_equal a, JSON.parse(work.fetch("payload"))
    assert_equal %w[queue payload run_at], work.keys
    assert_equal "default", work.dig("queue")
    assert_equal "payload", work.min_by(&:first).first # Enumerable's methods too
    refute_equal work.to_h, work # a Work, not a Hash, for == and inspect
    assert_match(/\A#<Sidekiq::Work/, work.inspect)
    assert_nil work["missing"]
    assert_respond_to work, :each_pair
    refute_respond_to work, :no_such_method
    assert_raises(NoMethodError) { work.no_such_method }
    assert_same work.to_h, work.to_h
    assert_same work.payload, work.payload
  end

  def test_work_set_size_sums_busy_counts
    assert_equal 0, Sidekiq::WorkSet.new.size
    beat("host:1", busy: 4)
    beat("host:2", busy: 2)
    redis("SADD", key("processes"), "gone")

    assert_equal 6, Sidekiq::WorkSet.new.size
  end

  # Another cluster

  def test_everything_works_on_another_config
    o = "#{key_prefix_for_test}other:"
    other = Robin::Config.new
    other.redis = {url: REDIS_URL}
    other.key_prefix = o
    other.internal_key_prefix = "#{o}robin:"
    ours = handlers
    theirs = []
    other.death_handlers << ->(job, _ex) { theirs << job.fetch("jid") }
    redis("ZADD", "#{o}retry", 1, JSON.generate(job("a", "retry_count" => 1)), 2, JSON.generate(job("b")))
    a, b = Sidekiq::RetrySet.new(config: other).fetch(1..2)
    a.retry
    b.kill
    Sidekiq::DeadSet.new(config: other).kill(JSON.generate(job("c")))

    assert_equal [["a", 0]], redis("LRANGE", "#{o}queue:default", 0, -1).map { |r| JSON.parse(r).values_at("jid", "retry_count") }
    assert_equal %w[b c], redis("ZRANGE", "#{o}dead", 0, -1).map { |r| JSON.parse(r).fetch("jid") }
    assert_equal [%w[b c], []], [theirs, ours]
    assert_empty redis("KEYS", key("*")).reject { |k| k.start_with?(o) }
    redis("SADD", "#{o}queues", "q")
    redis("RPUSH", "#{o}queue:q", JSON.generate(job("q")))

    assert Sidekiq::Queue.new("q", config: other).first.delete
    redis("RPUSH", "#{o}queue:q", JSON.generate(job("q")))

    assert_equal [["default", 1], ["q", 1]], Sidekiq::Stats.new(config: other).queue_summaries.map { |q| [q.name, q.size] }.sort
    redis("SET", "#{o}stat:processed:#{Time.now.utc.strftime("%Y-%m-%d")}", 8)

    assert_equal [8], Sidekiq::Stats::History.new(1, config: other).processed.values
    beat("p1", busy: 2, prefix: o)
    redis("SADD", "#{o}processes", "gone")

    assert_equal "p1", Sidekiq::ProcessSet["p1", config: other].identity
    process = Sidekiq::ProcessSet.new(config: other).first

    assert_equal ["p1"], redis("SMEMBERS", "#{o}processes")
    assert process.quiet!
    assert_equal %w[TSTP], redis("LRANGE", "#{o}p1-signals", 0, -1)
    assert_equal 2, Sidekiq::WorkSet.new(config: other).size
    assert_empty redis("KEYS", key("*")).reject { |k| k.start_with?(o) }
  ensure
    other.redis_pool.shutdown
  end
end
