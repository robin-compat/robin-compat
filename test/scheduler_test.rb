# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-scheduler"

module SchedulerTestJobs
  class Report
    include Sidekiq::Job

    sidekiq_options queue: "low"

    def perform(*) = nil
  end

  # A Sidekiq job without a queue of its own.
  class NoQueue
    include Sidekiq::Job

    def perform(*) = nil
  end

  # A job class with Sidekiq's options, but none set.
  class Bare
    def self.sidekiq_options = {}
  end

  # A job class that isn't Sidekiq's.
  class Plain
    def self.perform(*) = nil
  end
end

# Shared by the scheduler tests: a clean slate for sidekiq-scheduler's
# globals.
module SchedulerTestHelpers
  def setup
    super
    reset_scheduler
    @log = StringIO.new
    config.logger = Logger.new(@log)
  end

  def teardown
    SidekiqScheduler::Scheduler.instance.clear_schedule!
    reset_scheduler
    super
  end

  def reset_scheduler
    SidekiqScheduler::Scheduler.instance = nil
    SidekiqScheduler::RedisManager.key_prefix = nil
    Sidekiq.instance_variable_set(:@schedule, nil)
  end

  def manager = SidekiqScheduler::RedisManager

  # Runs the block with +object+'s +name+ doing +impl+, then puts back the
  # method it had (if its own) or removes the stand-in.
  def replacing(object, name, impl)
    singleton = object.singleton_class
    private = singleton.private_method_defined?(name, false)
    original = singleton.instance_method(name) if private || singleton.method_defined?(name, false)
    object.define_singleton_method(name, &impl)
    yield
  ensure
    if original
      singleton.define_method(name, original)
      singleton.send(:private, name) if private
    else
      singleton.remove_method(name)
    end
  end

  # The queue's jobs, the first pushed first.
  def queued(queue) = redis("LRANGE", key("queue:#{queue}"), 0, -1).reverse.map { |raw| JSON.parse(raw) }

  # +time+ as sidekiq-scheduler saves it.
  def local(time) = time.getlocal.strftime("%Y-%m-%d %H:%M:%S %z")
end

# sidekiq-scheduler's Redis data, settings, schedule methods and jobs.
class SidekiqSchedulerTest < RobinCompatTest
  include SchedulerTestJobs
  include SchedulerTestHelpers

  cover "SidekiqScheduler::RedisManager*"
  cover "SidekiqScheduler::Config*"
  cover "SidekiqScheduler::Schedule*"
  cover "SidekiqScheduler::Utils*"
  cover "SidekiqScheduler::ScheduledJob*"

  T = Time.utc(2030, 1, 1, 12, 0, 30)

  # -- RedisManager --------------------------------------------------------------

  def test_keys_are_sidekiq_schedulers
    assert_equal [key("schedules"), key("schedules_changed"), key("sidekiq-scheduler:states"), key("sidekiq-scheduler:next_times"),
      key("sidekiq-scheduler:last_times"), key("sidekiq-scheduler:pushed:hourly")],
      [manager.schedules_key, manager.schedules_changed_key, manager.schedules_state_key, manager.next_times_key,
        manager.last_times_key, manager.pushed_job_key("hourly")]
  end

  def test_a_key_prefix_scopes_the_keys
    {"app" => "app:", "app:" => "app:", "app." => "app."}.each do |given, prefix|
      manager.key_prefix = given

      assert_equal prefix, manager.key_prefix
      assert_equal key("#{prefix}schedules"), manager.schedules_key
    end
    manager.key_prefix = nil

    assert_nil manager.key_prefix
    assert_equal key("schedules"), manager.schedules_key
  end

  def test_job_fields_are_saved_read_and_removed
    manager.set_job_schedule("hourly", {"cron" => "0 * * * *"})
    manager.set_job_state("hourly", {"enabled" => false})
    manager.set_job_next_time("hourly", T)
    manager.set_job_last_time("hourly", 5)

    assert_equal ['{"cron":"0 * * * *"}', '{"enabled":false}', T.to_s, "5"],
      [manager.get_job_schedule("hourly"), manager.get_job_state("hourly"), manager.get_job_next_time("hourly"), manager.get_job_last_time("hourly")]
    assert_equal({"hourly" => '{"cron":"0 * * * *"}'}, manager.get_all_schedules)
    assert_true manager.schedule_exist?
    manager.remove_job_schedule("hourly")
    manager.remove_job_next_time("hourly")

    assert_equal [nil, nil, {}, false], [manager.get_job_schedule("hourly"), manager.get_job_next_time("hourly"), manager.get_all_schedules, manager.schedule_exist?]
  end

  def test_schedule_changes_are_scored_by_time
    replacing(Time, :now, -> { Time.at(10.5) }) { manager.add_schedule_change("a") }

    assert_equal "10.5", redis("ZSCORE", manager.schedules_changed_key, "a")
    redis("DEL", manager.schedules_changed_key)
    redis("ZADD", manager.schedules_changed_key, 10, "b", 20, "c")

    assert_equal ["b"], manager.get_schedule_changes(10, 20)
    assert_equal %w[b c], manager.get_schedule_changes(0, 21)
    assert_empty manager.get_schedule_changes(11, 20)
  end

  def test_an_old_set_of_changes_is_dropped
    redis("SADD", manager.schedules_changed_key, "a")
    manager.clean_schedules_changed

    assert_equal 0, redis("EXISTS", manager.schedules_changed_key)
    manager.add_schedule_change("a")
    manager.clean_schedules_changed

    assert_equal 1, redis("EXISTS", manager.schedules_changed_key)
  end

  def test_a_time_is_registered_once_and_forgotten_after_a_day
    assert_true manager.register_job_instance("hourly", T)
    assert_false manager.register_job_instance("hourly", T + 0.5)
    assert_equal [[T.to_i.to_s, T.to_i.to_s]], redis("ZRANGE", manager.pushed_job_key("hourly"), 0, -1, "WITHSCORES").each_slice(2).to_a
    assert_includes 86_399..86_400, redis("TTL", manager.pushed_job_key("hourly"))
    now = Time.now.to_i
    redis("ZADD", manager.pushed_job_key("hourly"), now - 86_401, "old", now - 86_400, "edge", now - 86_399, "recent", 0, "zero", -1, "negative")
    manager.remove_elder_job_instances("hourly")

    assert_equal ["recent", T.to_i.to_s], redis("ZRANGE", manager.pushed_job_key("hourly"), 0, -1)
    redis("PERSIST", manager.pushed_job_key("hourly"))
    manager.register_job_instance("hourly", T + 60)

    assert_operator redis("TTL", manager.pushed_job_key("hourly")), :>, 0
  end

  # -- Config ------------------------------------------------------------------

  def test_config_defaults
    settings = SidekiqScheduler::Config.new

    assert_equal({enabled: true, dynamic: false, dynamic_every: "5s", schedule: {}, listened_queues_only: nil, rufus_scheduler_options: {}},
      settings.to_hash)
    assert_equal({enabled: nil, dynamic: nil, dynamic_every: nil, schedule: nil, listened_queues_only: nil, rufus_scheduler_options: nil},
      SidekiqScheduler::Config.new(without_defaults: true).to_hash)
  end

  def test_config_reads_the_scheduler_section
    sidekiq = Sidekiq::Config.new(config)
    sidekiq[:scheduler] = {"dynamic" => true, :schedule => {"a" => {}}, :listened_queues_only => true}
    settings = SidekiqScheduler::Config.new(sidekiq_config: sidekiq)

    assert_equal [true, true, "5s", {"a" => {}}, true], [settings.enabled?, settings.dynamic?, settings.dynamic_every?, settings.schedule, settings.listened_queues_only?]
    assert_equal({dynamic: true}, SidekiqScheduler::Config.new(sidekiq_config: sidekiq.tap { |s| s[:scheduler] = {dynamic: true} }, without_defaults: true).to_hash.compact)
    assert_equal({}, SidekiqScheduler::Config.new(sidekiq_config: Sidekiq::Config.new(config), without_defaults: true).to_hash.compact)
  end

  def test_config_settings_can_be_changed
    settings = SidekiqScheduler::Config.new
    settings.enabled = false
    settings.dynamic = true
    settings.dynamic_every = "10s"
    settings.schedule = {"a" => {}}
    settings.listened_queues_only = true
    settings.rufus_scheduler_options = {max_work_threads: 2}

    assert_equal({enabled: false, dynamic: true, dynamic_every: "10s", schedule: {"a" => {}}, listened_queues_only: true,
                  rufus_scheduler_options: {max_work_threads: 2}}, settings.to_hash)
  end

  def test_config_queues_are_the_sidekiq_configs
    config.queues = %w[a b]
    other = Robin::Config.new
    other.queues = [:c]

    assert_equal %w[a b], SidekiqScheduler::Config.new.sidekiq_queues
    assert_equal %w[c], SidekiqScheduler::Config.new(sidekiq_config: Sidekiq::Config.new(other)).sidekiq_queues
  end

  # -- Sidekiq.schedule ------------------------------------------------------------

  def test_setting_the_schedule_saves_it_and_removes_the_rest
    Sidekiq.set_schedule("gone", {"every" => "1h"})
    Sidekiq.set_schedule("plain", {"every" => "9m"}) # replaced, not removed
    given = {Report: {every: "1h"}}.merge({"plain" => {"cron" => "* * * * *", "class" => Plain, "args" => [1]},
                                           "queued" => {"every" => "1m", "class" => "SchedulerTestJobs::Report", "queue" => "high"},
                                           "unknown" => {"every" => "1m", "class" => "NoSuchJob"}, "classless" => {"class" => nil}})
    schedule = Sidekiq.public_send(:schedule=, given)

    assert_equal({"Report" => {"class" => "Report", "every" => "1h"}, "plain" => {"class" => Plain, "cron" => "* * * * *", "args" => [1]},
                  "queued" => {"class" => "SchedulerTestJobs::Report", "every" => "1m", "queue" => "high"},
                  "unknown" => {"class" => "NoSuchJob", "every" => "1m"}, "classless" => {"class" => nil}}, schedule)
    assert_same schedule, Sidekiq.schedule
    assert_equal({"class" => "SchedulerTestJobs::Plain", "cron" => "* * * * *", "args" => [1]}, Sidekiq.get_schedule("plain"))
    assert_equal %w[Report classless plain queued unknown], Sidekiq.get_schedule.keys.sort
    Sidekiq.schedule = {"q" => {"every" => "1m", "class" => "SchedulerTestJobs::Report"}}

    assert_equal({"class" => "SchedulerTestJobs::Report", "every" => "1m", "queue" => "low"}, Sidekiq.schedule.fetch("q"))
    assert_equal %w[q], Sidekiq.get_all_schedules.keys
  end

  def test_the_schedule_is_reloaded_from_redis
    Sidekiq.set_schedule("hourly", {"cron" => "0 * * * *"})

    assert_nil Sidekiq.schedule
    assert_equal({"hourly" => {"cron" => "0 * * * *"}}, Sidekiq.reload_schedule!)
    assert_equal({"hourly" => {"cron" => "0 * * * *"}}, Sidekiq.schedule)
    Sidekiq.remove_schedule("hourly")

    assert_empty Sidekiq.schedule!
    assert_nil Sidekiq.get_schedule("hourly")
  end

  def test_saving_a_schedule_notes_a_change_unless_its_the_same
    config_hash = {"cron" => "0 * * * *"}

    assert_same config_hash, Sidekiq.set_schedule("hourly", config_hash)
    redis("DEL", manager.schedules_changed_key)
    Sidekiq.set_schedule("hourly", {"cron" => "0 * * * *"})

    assert_equal 0, redis("EXISTS", manager.schedules_changed_key)
    Sidekiq.set_schedule("hourly", {"cron" => "1 * * * *"})

    assert_equal ["hourly"], redis("ZRANGE", manager.schedules_changed_key, 0, -1)
    Sidekiq.remove_schedule("daily")

    assert_equal %w[daily hourly], redis("ZRANGE", manager.schedules_changed_key, 0, -1).sort
  end

  # -- Utils -------------------------------------------------------------------

  def test_keys_are_stringified_and_symbolized_deeply
    nested = {a: [{b: Set[{c: 1}]}]}.merge({"d" => {e: "f"}})

    assert_equal({"a" => [{"b" => [{"c" => 1}]}], "d" => {"e" => "f"}}, SidekiqScheduler::Utils.stringify_keys(nested))
    assert_equal({a: [{b: [{c: 1}]}], d: {e: "f"}}, SidekiqScheduler::Utils.symbolize_keys(nested))
    assert_equal "x", SidekiqScheduler::Utils.stringify_keys("x")
  end

  def test_classes_are_found_by_name
    assert_equal [Report, "NoSuchJob", Plain, :Report], %w[SchedulerTestJobs::Report NoSuchJob].map { |n| SidekiqScheduler::Utils.try_to_constantize(n) } +
      [SidekiqScheduler::Utils.try_to_constantize(Plain), SidekiqScheduler::Utils.try_to_constantize(:Report)]
  end

  def test_sidekiq_jobs_are_pushed_with_the_client
    SidekiqScheduler::Utils.enqueue_with_sidekiq({"class" => Report, "args" => [1], "queue" => "low", "cron" => "* * * * *", "description" => "x"})
    job = queued("low").first

    assert_equal [Report.name, [1]], job.values_at("class", "args")
    refute job.key?("cron")
    refute job.key?("description")
    assert_equal({"class" => "X"}, SidekiqScheduler::Utils.sanitize_job_config(
      {"class" => "X", "description" => 1, "at" => 1, "cron" => 1, "every" => 1, "in" => 1, "interval" => 1, "enabled" => 1}
    ))
  end

  def test_times_are_saved_as_rufus_writes_them
    SidekiqScheduler::Utils.update_job_next_time("hourly", Time.new(2030, 1, 2, 3, 4, 5, "+05:30"))
    SidekiqScheduler::Utils.update_job_last_time("hourly", T)
    SidekiqScheduler::Utils.update_job_last_time("other", nil)

    assert_equal [local(Time.new(2030, 1, 2, 3, 4, 5, "+05:30")), local(T), nil],
      [manager.get_job_next_time("hourly"), manager.get_job_last_time("hourly"), manager.get_job_last_time("other")]
    SidekiqScheduler::Utils.update_job_next_time("hourly", nil)

    assert_nil manager.get_job_next_time("hourly")
  end

  def test_a_sidekiq_jobs_queue_is_its_classs
    assert_equal ["low", "low", nil, nil, "default"], [Report, Report.name, "NoSuchJob", Plain, NoQueue].map { |klass| SidekiqScheduler::Utils.sidekiq_queue(klass) }
    assert_equal "DURATION", SidekiqScheduler::Utils.try_to_constantize("DURATION") # not Utils' own
    assert_nil SidekiqScheduler::Utils.sidekiq_queue(Bare)
  end

  def test_durations_without_fugit
    replacing(Robin::Compat::CronLine, :fugit, -> {}) do
      {"30" => 30, " 45m " => 2_700, "010" => 10, "08m" => 480, "1h1m1s" => 3_661}.each do |given, seconds|
        assert_equal seconds, SidekiqScheduler::Utils.duration(given), given
      end
    end

    assert_instance_of Rational, SidekiqScheduler::Utils.duration(Rational(3, 2))
    assert_equal 2_700, SidekiqScheduler::Utils.duration(:"45m")
  end

  def test_durations
    {5 => 5, 1.5 => 1.5, "30" => 30, " 45m " => 2_700, "1h30m" => 5_400, "2d" => 172_800, "1w" => 604_800, "1h1m1s" => 3_661,
     "2 hours and 30 minutes" => 9_000, "1.5 hours" => 5_400}.each do |given, seconds|
      assert_equal seconds, SidekiqScheduler::Utils.duration(given), given
    end
    error = assert_raises(ArgumentError) { SidekiqScheduler::Utils.duration("soon") }
    assert_equal '"soon" isn\'t a duration', error.message
  end

  def test_durations_in_words_need_fugit
    replacing(Robin::Compat::CronLine, :fugit, -> {}) do
      assert_equal 2_700, SidekiqScheduler::Utils.duration("45m")
      error = assert_raises(ArgumentError) { SidekiqScheduler::Utils.duration("2 hours") }
      assert_equal '"2 hours" isn\'t a duration like "45m" or "1h30m" (others need the fugit gem)', error.message
    end
  end

  # -- ScheduledJob ----------------------------------------------------------------

  def scheduled(config, now = T) = SidekiqScheduler::ScheduledJob.build("job", config, now)

  def test_a_job_is_scheduled_by_its_first_schedule
    assert_equal %w[cron every at in interval every], [
      {"cron" => "* * * * *", "every" => "1m"}, {"every" => "1m", "at" => "2031-01-01"}, {"at" => "2031-01-01", "in" => "1m"},
      {"in" => "1m", "interval" => "1m"}, {"interval" => "1m"}, {"cron" => "", "every" => ["1m", {"first_in" => "5m"}]}
    ].map { |config| scheduled(config).type }
    assert_nil scheduled({"class" => "X"})
    assert_nil scheduled({"cron" => nil, "every" => ""})
    job = scheduled({"every" => ["1m", {"first_in" => "5m"}], "class" => "X"})

    assert_equal ["job", {"every" => ["1m", {"first_in" => "5m"}], "class" => "X"}, ["1m", {"first_in" => "5m"}]], [job.name, job.config, job.schedule]
  end

  def test_a_cron_job_is_due_at_its_latest_tick_since_it_was_scheduled
    job = scheduled({"cron" => "*/10 * * * * * UTC"}, T - 1) # 12:00:29

    assert_equal [T - 0.5, T + 0.5, T + 10].map { |now| job.due(now) }, [nil, T - 1 + 1, T + 10]
    assert_nil scheduled({"cron" => "0 * * * * UTC"}, T).due(T + 60)
    assert_equal Time.utc(2030, 1, 1, 13), scheduled({"cron" => "0 * * * * UTC"}, T).next_time(T)
    assert_equal T + 10, job.next_time(T)
    assert_equal "America/Chicago", scheduled({"cron" => "0 22 * * * America/Chicago"}).instance_variable_get(:@recurring).tz
    assert_equal "0 22 * * *", scheduled({"cron" => :"0 22 * * * UTC"}).instance_variable_get(:@recurring).to_s
  end

  def test_jobs_are_scheduled_and_timed_from_now_by_default
    replacing(Time, :now, -> { T }) do
      job = SidekiqScheduler::ScheduledJob.build("job", {"every" => "10s"})

      assert_equal T + 10, job.next_time
      assert_equal T, job.due(T)
    end
  end

  def test_a_cron_job_is_due_at_the_tick_it_was_scheduled
    assert_equal T, scheduled({"cron" => "*/10 * * * * * UTC"}, T).due(T + 0.5)
    assert_nil scheduled({"cron" => "0 0 30 2 * UTC"}).due(T) # never
  end

  def test_every_and_interval_tick_at_multiples_of_their_interval
    job = scheduled({"every" => "45m"}, T)

    assert_equal [Time.utc(2030, 1, 1, 12, 45), Time.utc(2030, 1, 1, 12, 45)], [job.next_time(T), job.due(Time.utc(2030, 1, 1, 12, 50))]
    assert_nil job.due(T + 60)
    assert_equal 2_700, job.instance_variable_get(:@recurring).interval
    assert_equal 90, scheduled({"interval" => 90}).instance_variable_get(:@recurring).interval
    assert_equal 5_400, scheduled({"every" => "1.5 hours"}).instance_variable_get(:@recurring).interval
    assert_equal 45, scheduled({"every" => "45s"}).instance_variable_get(:@recurring).interval
    error = assert_raises(ArgumentError) { scheduled({"every" => "1.5s"}) }
    assert_equal %(job: "1.5s" isn't a whole number of seconds), error.message
    assert_equal %(job: "0m" isn't a whole number of seconds), assert_raises(ArgumentError) { scheduled({"every" => "0m"}) }.message
  end

  def test_at_and_in_jobs_are_due_once_their_time_comes
    at = scheduled({"at" => "2030-01-01 12:05:00 UTC"})

    assert_equal [nil, T + 270, T + 270], [at.due(T + 269), at.due(T + 270), at.due(T + 999)]
    # Times, not the Strings they were parsed from (which Active Support
    # would compare equal).
    assert_instance_of Time, at.due(T + 270)
    assert_instance_of Time, scheduled({"at" => :"2030-01-01 12:05:00 UTC"}).due(T + 270)
    assert_instance_of Time, scheduled({"at" => Date.new(2031, 1, 1)}).next_time(T)
    precise = Class.new(Time).at(T.to_r + Rational(1, 4))

    assert_same precise, scheduled({"at" => precise}).next_time(T)
    assert_equal [T + 270, nil], [at.next_time(T), at.next_time(T + 270)]
    assert_equal T + 1, scheduled({"at" => T + 1}).due(T + 1)
    assert_equal T, scheduled({"at" => T}, T + 0.5).due(T + 0.5)
    past = scheduled({"at" => "2030-01-01 12:00:00 UTC"})

    assert_nil past.due(T + 999)
    assert_nil past.next_time(T)
    later = scheduled({"in" => "1m"}, T + 0.5)

    assert_equal [nil, T + 60], [later.due(T + 59), later.due(T + 60)]
  end
end
