# frozen_string_literal: true

require_relative "scheduler_test"
require "open3"
require "rbconfig"
require "sidekiq/active_job"
require "sidekiq/api"
require "sidekiq/cli"

ActiveJob::Base.logger = Logger.new(nil)

module SchedulerTestJobs
  class Mailer < ActiveJob::Base
    queue_as :mail

    def perform(*) = nil
  end

  # An Active Job class with Sidekiq options (whose queue isn't its own).
  class Optioned < ActiveJob::Base
    sidekiq_options queue: "sidekiq"

    def perform(*) = nil
  end

  # An Active Job class taking keyword arguments.
  class Notifier < ActiveJob::Base
    def perform(to:) = to
  end
end

# sidekiq-scheduler's scheduler: scheduling, pushing, and how servers run it.
class SidekiqSchedulerRunTest < RobinCompatTest
  include SchedulerTestJobs
  include SchedulerTestHelpers

  cover "SidekiqScheduler.boot"
  cover "SidekiqScheduler::Scheduler*"
  cover "SidekiqScheduler::Poller*"
  cover "SidekiqScheduler::Manager*"
  cover "SidekiqScheduler::Utils*"
  # As broad as sidekiq_test's and cli_test's, so mutant runs those too.
  cover "Sidekiq."
  cover "Sidekiq::CLI*"

  T = Time.utc(2030, 1, 1, 12, 0, 30)

  def setup
    super
    Mailer.queue_adapter = :sidekiq
    Notifier.queue_adapter = :sidekiq
  end

  def scheduler(settings = {}) = SidekiqScheduler::Scheduler.new(SidekiqScheduler::Config.new.tap { |c| settings.each { |k, v| c.public_send(:"#{k}=", v) } })

  # A scheduler with +schedule+ loaded (and not polling).
  def loaded(schedule, now: T, **settings)
    Sidekiq.schedule = schedule
    scheduler(settings).tap do |s|
      s.instance_variable_set(:@poller, :not_polling)
      replacing(Time, :now, -> { now }) { s.load_schedule! }
      s.instance_variable_set(:@poller, nil)
    end
  end

  def leading(config = self.config) = Robin::Leader.register(config, Struct.new(:leader?).new(true))

  # -- Scheduler.instance --------------------------------------------------------

  def test_the_class_forwards_to_its_instance
    instance = SidekiqScheduler::Scheduler.instance

    assert_instance_of SidekiqScheduler::Scheduler, instance
    assert_same instance, SidekiqScheduler::Scheduler.instance
    assert_same SidekiqScheduler::Scheduler, Sidekiq::Scheduler
    SidekiqScheduler::Scheduler.dynamic = true

    assert_true instance.dynamic
    assert_true SidekiqScheduler::Scheduler.dynamic
    assert_respond_to SidekiqScheduler::Scheduler, :reload_schedule!
    refute_respond_to SidekiqScheduler::Scheduler, :schedule_state
    [-> { SidekiqScheduler::Scheduler.schedule_state("x") }, -> { SidekiqScheduler::Scheduler.nonsense }].each do |call|
      assert_same SidekiqScheduler::Scheduler, assert_raises(NoMethodError, &call).receiver
    end
    assert_equal({enabled: nil, dynamic: nil, dynamic_every: nil, listened_queues_only: nil, rufus_scheduler_options: {}},
      %i[enabled dynamic dynamic_every listened_queues_only rufus_scheduler_options].to_h { |name| [name, SidekiqScheduler::Scheduler.new.public_send(name)] })
  end

  def test_settings_come_from_the_config
    settings = SidekiqScheduler::Config.new
    settings.listened_queues_only = true
    settings.rufus_scheduler_options = {"max_work_threads" => 2}
    s = SidekiqScheduler::Scheduler.new(settings)

    assert_equal [true, false, "5s", true, {"max_work_threads" => 2}],
      [s.enabled, s.dynamic, s.dynamic_every, s.listened_queues_only, s.rufus_scheduler_options]
    assert_equal({scheduler_config: settings.to_hash}, s.to_hash)
    assert_equal '#<SidekiqScheduler::Scheduler enabled=true dynamic=false dynamic_every=5s listened_queues_only=true rufus_scheduler_options={"max_work_threads" => 2}>', s.inspect
    assert_empty s.scheduled_jobs
  end

  # -- loading -------------------------------------------------------------------

  def test_loading_schedules_the_jobs_and_saves_their_next_times
    s = loaded({"hourly" => {"cron" => "0 * * * * UTC", "class" => Report.name}, "nothing" => {"class" => Report.name}})

    assert_equal ["hourly"], s.scheduled_jobs.keys
    assert_equal local(Time.utc(2030, 1, 1, 13)), manager.get_job_next_time("hourly")
    assert_includes @log.string, "Loading Schedule"
    assert_includes @log.string, %(Scheduling hourly {"class" => "SchedulerTestJobs::Report", "cron" => "0 * * * * UTC", "queue" => "low"})
    assert_includes @log.string, "no cron / every / at / in / interval found for SchedulerTestJobs::Report (nothing) - skipping"
    assert_includes @log.string, "Schedules Loaded"
    refute_includes @log.string, "Schedule empty!"
  end

  def test_loading_takes_the_schedule_as_set_and_starts_over
    Sidekiq.set_schedule("in redis only", {"every" => "1m"})
    Sidekiq.instance_variable_set(:@schedule, {"set" => {"every" => "1m"}})
    s = scheduler
    s.instance_variable_set(:@poller, :not_polling)
    s.load_schedule!

    assert_equal ["set"], s.scheduled_jobs.keys
    assert_nil s.instance_variable_get(:@current_changed_score)
    Sidekiq.instance_variable_set(:@schedule, {"other" => {"every" => "1m"}})
    s.load_schedule!

    assert_equal ["other"], s.scheduled_jobs.keys
  ensure
    s&.instance_variable_set(:@poller, nil)
  end

  def test_a_job_without_a_schedule_or_class_is_skipped
    scheduler.load_schedule_job("bare", {})

    assert_includes @log.string, "no cron / every / at / in / interval found for  (bare) - skipping"
  end

  def test_reloading_a_disabled_scheduler_keeps_its_jobs
    s = scheduler(enabled: false)
    s.scheduled_jobs["kept"] = :job
    s.reload_schedule!

    assert_equal({"kept" => :job}, s.scheduled_jobs)
  end

  def test_the_poller_runs_on_robins_config
    Sidekiq.schedule = {}
    s = scheduler
    s.load_schedule!

    assert_same config, s.instance_variable_get(:@poller).instance_variable_get(:@config)
  ensure
    s&.clear_schedule!
  end

  def test_loading_an_empty_or_disabled_schedule
    loaded({})

    assert_includes @log.string, "Schedule empty! Set Sidekiq.schedule"
    Sidekiq.instance_variable_set(:@schedule, nil)
    s = scheduler

    s.instance_variable_set(:@poller, :not_polling)
    s.load_schedule!

    assert_empty s.scheduled_jobs
    s.instance_variable_set(:@poller, nil)
    disabled = scheduler(enabled: false)

    disabled.load_schedule!
    disabled.reload_schedule!

    assert_equal 2, @log.string.scan("SidekiqScheduler is disabled").size
    assert_nil disabled.instance_variable_get(:@poller)
  end

  def test_a_dynamic_schedule_is_loaded_from_redis
    Sidekiq.set_schedule("hourly", {"cron" => "0 * * * *", "class" => Report.name})
    s = scheduler(dynamic: true)
    s.instance_variable_set(:@poller, :not_polling)
    replacing(Time, :now, -> { T + 0.25 }) { s.load_schedule! }

    assert_equal ["hourly"], s.scheduled_jobs.keys
    assert_in_delta (T + 0.25).to_f, s.instance_variable_get(:@current_changed_score)
  ensure
    s&.instance_variable_set(:@poller, nil)
  end

  def test_loading_starts_polling_once
    s = scheduler
    Sidekiq.schedule = {}
    s.load_schedule!
    poller = s.instance_variable_get(:@poller)

    assert_instance_of SidekiqScheduler::Poller, poller
    s.load_schedule!

    assert_same poller, s.instance_variable_get(:@poller)
    s.reload_schedule!

    refute_predicate poller.instance_variable_get(:@thread), :alive?
    refute_same poller, s.instance_variable_get(:@poller)
    assert_includes @log.string, "Reloading Schedule"
    reloaded = s.instance_variable_get(:@poller)
    s.clear_schedule!

    assert_nil s.instance_variable_get(:@poller)
    refute_predicate reloaded.instance_variable_get(:@thread), :alive?
  end

  def test_rails_env_limits_where_a_job_is_scheduled
    s = scheduler
    previous = ENV["RAILS_ENV"]
    ENV["RAILS_ENV"] = "production"
    s.load_schedule_job("a", {"every" => "1m", "rails_env" => "staging, production"})
    s.load_schedule_job("b", {"every" => "1m", "rails_env" => "staging"})
    s.load_schedule_job("b2", {"every" => "1m", "rails_env" => "preproduction"})
    s.load_schedule_job("c", {"every" => "1m"})
    ENV.delete("RAILS_ENV")
    s.load_schedule_job("d", {"every" => "1m", "rails_env" => "production"})

    assert_equal %w[a c], s.scheduled_jobs.keys
  ensure
    ENV["RAILS_ENV"] = previous
  end

  # -- pushing -------------------------------------------------------------------

  def test_a_job_is_pushed_once_for_a_time
    s = scheduler
    s.idempotent_job_enqueue("report", T, {"class" => Report.name, "args" => [1], "queue" => "low"})
    s.idempotent_job_enqueue("report", T, {"class" => Report.name, "args" => [1], "queue" => "low"})

    assert_equal [[Report.name, [1]]], queued("low").map { |job| job.values_at("class", "args") }
    assert_includes @log.string, "queueing SchedulerTestJobs::Report (report)"
    assert_includes @log.string, "Ignoring report job as it has been already enqueued"
  end

  def test_pushes_forget_their_old_times_and_log_errors
    redis("ZADD", manager.pushed_job_key("broken"), 1, "1")
    s = scheduler
    s.define_singleton_method(:enqueue_job) { |*| raise "boom" }
    s.idempotent_job_enqueue("broken", T, {"class" => Report.name})

    assert_includes @log.string, "queueing SchedulerTestJobs::Report (broken)"
    assert_includes @log.string, "INFO -- : RuntimeError: boom"
    assert_equal [T.to_i.to_s], redis("ZRANGE", manager.pushed_job_key("broken"), 0, -1)
  end

  def test_arguments_are_an_array_or_a_hash
    s = scheduler
    s.enqueue_job({"class" => Report.name, "queue" => "low"})
    s.enqueue_job({"class" => Report, "queue" => "low", "args" => "one"})
    s.enqueue_job({"class" => Report.name, "queue" => "low", "args" => [1, 2]})

    assert_equal [[], ["one"], [1, 2]], queued("low").map { |job| job.fetch("args") }
  end

  def test_include_metadata_adds_the_time
    s = scheduler
    s.enqueue_job({"class" => Report.name, "queue" => "low", "args" => [1], "include_metadata" => true}, Time.at(1.23456))
    s.enqueue_job({"class" => Report.name, "queue" => "low", "include_metadata" => true}, Time.at(2))
    s.enqueue_job({"class" => Mailer.name, "args" => {"a" => 1}, "include_metadata" => true}, Time.at(3))
    s.enqueue_job({"class" => Report.name, "queue" => "low", "include_metadata" => false})

    assert_equal [[1, {"scheduled_at" => 1.235}], [{"scheduled_at" => 2.0}], []], queued("low").map { |job| job.fetch("args") }
    refute queued("low").first.key?("include_metadata")
    assert_equal [{"a" => 1, "_aj_symbol_keys" => []}, {"scheduled_at" => 3.0, "_aj_symbol_keys" => []}],
      queued("mail").first.fetch("args").first.fetch("arguments")
  end

  def test_active_jobs_are_enqueued_with_active_job
    s = scheduler
    s.enqueue_job({"class" => Mailer.name, "args" => [1, "a"]})
    s.enqueue_job({"class" => Mailer.name, "args" => [2], "queue" => "other"})
    s.enqueue_job({"class" => Notifier.name, "args" => {"to" => "x"}, "keyword_argument" => true, "queue" => "notes"})
    s.enqueue_job({"class" => Mailer.name, "args" => {"to" => "y"}})

    assert_equal [[Mailer.name, [1, "a"]], [Mailer.name, [{"to" => "y", "_aj_symbol_keys" => []}]]],
      queued("mail").map { |job| [job.fetch("wrapped"), job.fetch("args").first.fetch("arguments")] }
    assert_equal [[2]], queued("other").map { |job| job.fetch("args").first.fetch("arguments") }
    assert_equal [[{"to" => "x", "_aj_ruby2_keywords" => ["to"]}]], queued("notes").map { |job| job.fetch("args").first.fetch("arguments") }
  end

  # sidekiq-scheduler 5 (Sidekiq 7.2 and earlier) symbolizes a Hash of
  # arguments with Active Support.
  def test_hash_arguments_are_symbolized_for_sidekiq_before_7_3
    s = scheduler
    Sidekiq.compatible_with = "7.2"
    s.enqueue_job({"class" => Mailer.name, "args" => {"to" => "y"}})
    plain = {"to" => "p"}
    plain.singleton_class.undef_method(:symbolize_keys)
    s.enqueue_job({"class" => Mailer.name, "args" => plain})
    s.enqueue_job({"class" => Mailer.name, "args" => ["a"]})
    Sidekiq.compatible_with = "7.3"
    s.enqueue_job({"class" => Mailer.name, "args" => {"to" => "z"}})

    assert_equal [[{"to" => "y", "_aj_symbol_keys" => ["to"]}], [{"to" => "p", "_aj_symbol_keys" => []}], ["a"],
      [{"to" => "z", "_aj_symbol_keys" => []}]], queued("mail").map { |job| job.fetch("args").first.fetch("arguments") }
  end

  def test_active_job_arguments
    SidekiqScheduler::Utils.enqueue_with_active_job({"class" => Mailer})
    SidekiqScheduler::Utils.enqueue_with_active_job({"class" => Mailer, "args" => {"to" => "z"}, "keyword_argument" => false})

    assert_equal [[nil], [{"to" => "z", "_aj_symbol_keys" => []}]], queued("mail").map { |job| job.fetch("args").first.fetch("arguments") }
    assert_equal [{"to" => "x"}], SidekiqScheduler::Utils.initialize_active_job(Notifier, {"to" => "x"}).arguments
    assert_equal [{to: "x"}], SidekiqScheduler::Utils.initialize_active_job(Notifier, {"to" => "x"}, true).arguments
    assert_equal ["x"], SidekiqScheduler::Utils.initialize_active_job(Mailer, "x", true).arguments
  end

  def test_active_job_is_told_apart
    assert_true SidekiqScheduler::Utils.active_job_enqueue?(Mailer)
    assert_false SidekiqScheduler::Utils.active_job_enqueue?(Report)
    assert_false SidekiqScheduler::Utils.active_job_enqueue?(Mailer.name)
    assert_false SidekiqScheduler::Utils.active_job_enqueue?(ActiveJob::Base)
    assert_nil SidekiqScheduler::Utils.sidekiq_queue(Mailer)
    assert_nil SidekiqScheduler::Utils.sidekiq_queue(Optioned)
    replacing(SidekiqScheduler::Utils, :active_job_base, -> {}) do
      assert_false SidekiqScheduler::Utils.active_job_enqueue?(Mailer)
    end
  end

  # -- due jobs ------------------------------------------------------------------

  def test_due_jobs_are_pushed_once_for_each_time
    s = loaded({"later" => {"cron" => "0 0 1 1 * UTC", "class" => Report.name},
                "tens" => {"cron" => "*/10 * * * * * UTC", "class" => Report.name, "args" => [1], "description" => "x"}}, now: T - 1)
    replacing(Sidekiq::ProcessSet, :new, -> { raise "only for listened_queues_only" }) do
      s.enqueue_due(T + 0.5)
      s.enqueue_due(T + 5) # the same tick: not even tried again
    end

    assert_equal 1, queued("low").size
    refute queued("low").first.key?("cron")
    refute_includes @log.string, "Ignoring"
    assert_equal [T.to_i.to_s], redis("ZRANGE", manager.pushed_job_key("tens"), 0, -1)
    assert_equal [local(T), local(T + 10)], [manager.get_job_last_time("tens"), manager.get_job_next_time("tens")]
    s.enqueue_due(T + 10)

    assert_equal 2, queued("low").size
  end

  def test_a_tick_tried_before_a_reload_is_tried_again
    s = loaded({"tens" => {"cron" => "*/10 * * * * * UTC", "class" => Report.name}}, now: T - 1)
    s.enqueue_due(T + 0.5)
    s.clear_schedule!

    assert_empty s.scheduled_jobs
    replacing(Time, :now, -> { T - 1 }) { s.instance_variable_set(:@poller, :not_polling) && s.load_schedule! }
    s.instance_variable_set(:@poller, nil)
    s.enqueue_due(T + 0.5)

    assert_includes @log.string, "Ignoring tens job as it has been already enqueued"
  end

  def test_enqueue_job_leaves_the_schedule_alone
    schedule = {"class" => Report.name, "queue" => "low", "include_metadata" => true}
    scheduler.enqueue_job(schedule)

    assert_equal({"class" => Report.name, "queue" => "low", "include_metadata" => true}, schedule)
  end

  def test_disabled_jobs_keep_their_times_but_arent_pushed
    s = loaded({"off" => {"every" => "10s", "class" => Report.name, "enabled" => false}}, now: T - 1)
    s.enqueue_due(T + 0.5)

    assert_empty queued("low")
    assert_equal local(T), manager.get_job_last_time("off")
  end

  def test_listened_queues_only_pushes_jobs_live_processes_take
    errors = []
    config.error_handlers << ->(error, *) { errors << error }
    s = loaded({"high" => {"every" => "10s", "class" => Report.name, "queue" => "high"}, "none" => {"every" => "10s", "class" => "NoSuchJob"},
                "low" => {"every" => "10s", "class" => Report.name, "queue" => :low}}, now: T - 1, listened_queues_only: true)
    replacing(Sidekiq::ProcessSet, :new, -> { [Struct.new(:queues).new(%w[low default]), Struct.new(:queues).new(%w[low])] }) do
      s.enqueue_due(T + 0.5)
    end

    assert_empty errors
    assert_equal [1, 0, 0], [queued("low").size, queued("high").size, queued("default").size]
    assert_equal local(T), manager.get_job_last_time("low")
    assert_nil manager.get_job_last_time("high")
    replacing(Sidekiq::ProcessSet, :new, -> { [] }) { s.enqueue_due(T + 10) }

    assert_equal [2, 1, 1], [queued("low").size, queued("high").size, queued("default").size]
  end

  def test_a_failing_job_doesnt_stop_the_others
    errors = []
    config.error_handlers << ->(error, context, *) { errors << [error.message, context] }
    s = loaded({"bad" => {"every" => "10s", "class" => Report.name}, "good" => {"every" => "10s", "class" => Report.name}}, now: T - 1)
    s.scheduled_jobs.fetch("bad").define_singleton_method(:next_time) { |*| raise "boom" }
    s.enqueue_due(T + 0.5)

    assert_equal [["boom", {context: "sidekiq-scheduler job bad"}]], errors
    assert_equal 2, queued("low").size
  end

  def test_the_wait_is_until_the_next_job_at_most_a_second
    assert_in_delta 1.0, scheduler.seconds_until_next
    s = loaded({"tens" => {"cron" => "*/10 * * * * * UTC", "class" => Report.name}}, now: T - 1)

    assert_in_delta 0.5, s.seconds_until_next(T + 9.5)
    assert_in_delta 1.0, s.seconds_until_next(T + 5)
    assert_in_delta 0.05, s.seconds_until_next(T + 9.99)
    assert_operator s.seconds_until_next, :<=, 1.0
    several = loaded({"done" => {"at" => "2030-01-01 12:00:29 UTC", "class" => Report.name}, "late" => {"every" => "1m", "class" => Report.name},
                      "soon" => {"every" => "10s", "class" => Report.name}, "later" => {"every" => "1h", "class" => Report.name}}, now: T - 1)

    assert_in_delta 0.5, several.seconds_until_next(T + 9.5)
  end

  def test_due_jobs_and_metadata_are_timed_from_now_by_default
    s = loaded({"tens" => {"every" => "10s", "class" => Report.name}}, now: T - 1)
    replacing(Time, :now, -> { T + 0.5 }) do
      s.enqueue_due
      s.enqueue_job({"class" => Report.name, "queue" => "other", "include_metadata" => true})
    end

    assert_equal [T.to_i.to_s], redis("ZRANGE", manager.pushed_job_key("tens"), 0, -1)
    assert_equal [[{"scheduled_at" => (T + 0.5).to_f}]], queued("other").map { |job| job.fetch("args") }
  end

  def test_a_pushed_job_gets_its_times_metadata
    scheduler.idempotent_job_enqueue("meta", T, {"class" => Report.name, "queue" => "low", "include_metadata" => true})

    assert_equal [[{"scheduled_at" => T.to_f}]], queued("low").map { |job| job.fetch("args") }
  end

  # -- dynamic schedules and states ------------------------------------------------

  def test_dynamic_changes_are_rescheduled
    s = loaded({"kept" => {"every" => "1m", "class" => Report.name}, "changed" => {"every" => "1m", "class" => Report.name},
                "removed" => {"every" => "1m", "class" => Report.name}}, dynamic: true)
    s.instance_variable_get(:@tried).merge!("changed" => T, "kept" => T)
    s.instance_variable_set(:@current_changed_score, Time.now.to_f) # after loaded's changes
    Sidekiq.set_schedule("changed", {"every" => "2m", "class" => Report.name})
    Sidekiq.remove_schedule("removed")
    s.update_schedule

    assert_equal %w[changed kept], s.scheduled_jobs.keys.sort
    assert_equal "2m", s.scheduled_jobs.fetch("changed").schedule
    assert_equal({"kept" => T}, s.instance_variable_get(:@tried))
    assert_nil s.instance_variable_get(:@poller)
    assert_includes @log.string, "Updating schedule"
    assert_includes @log.string, "Schedule updated"
    @log.truncate(0)
    s.update_schedule

    refute_includes @log.string, "Updating schedule"
  end

  def test_jobs_are_enabled_unless_their_state_or_schedule_says
    Sidekiq.schedule = {"on" => {"every" => "1m"}, "off" => {"every" => "1m", "enabled" => false}}
    s = scheduler

    assert_equal [true, false, nil], %w[on off missing].map { |name| s.job_enabled?(name) }
    s.toggle_job_enabled("on")
    s.toggle_job_enabled("off")

    assert_equal [false, true], %w[on off].map { |name| s.job_enabled?(name) }
    assert_equal({"enabled" => false}, JSON.parse(manager.get_job_state("on")))
    manager.set_job_state("on", {"enabled" => false, "other" => 1})
    s.toggle_job_enabled("on")

    assert_equal({"enabled" => true, "other" => 1}, JSON.parse(manager.get_job_state("on")))
    s.toggle_all_jobs(false)

    assert_equal [false, false], %w[on off].map { |name| s.job_enabled?(name) }
    assert_equal 1, JSON.parse(manager.get_job_state("on")).fetch("other")
    Sidekiq.instance_variable_set(:@schedule, nil)

    assert_nil s.job_enabled?("on")
  end

  def test_print_schedule_logs_each_jobs_last_run
    s = loaded({"hourly" => {"cron" => "0 * * * *", "class" => Report.name}})
    manager.set_job_last_time("hourly", "then")
    s.print_schedule

    assert_includes @log.string, "Scheduling Info\tLast Run"
    assert_includes @log.string, "0 * * * *\tthen\t"
  end

  # -- Poller --------------------------------------------------------------------

  def test_the_poller_pushes_only_on_the_leader
    s = loaded({"tens" => {"every" => "10s", "class" => Report.name}}, now: T - 1)
    poller = SidekiqScheduler::Poller.new(s, config)
    other = Robin::Config.new
    Robin::Leader.register(other, Struct.new(:leader?).new(true))
    Robin::Leader.register(config, Struct.new(:leader?).new(false))

    assert_equal T, poller.poll(T + 0.5, T)
    assert_empty queued("low")
    assert_equal T, SidekiqScheduler::Poller.new(s, other).poll(T + 0.5, T) # its own config's leader
    assert_equal 1, queued("low").size
  end

  def test_the_poller_reloads_a_dynamic_schedule_every_dynamic_every
    s = loaded({}, dynamic: true, dynamic_every: "2s")
    updates = []
    s.define_singleton_method(:update_schedule) { updates << :update }
    poller = SidekiqScheduler::Poller.new(s, config)

    assert_equal T + 2, poller.poll(T, T)
    assert_equal T, poller.poll(T - 1, T)
    s.dynamic_every = nil

    assert_equal T + 5, poller.poll(T, T - 1)
    assert_equal 2, updates.size
    s.dynamic = false

    assert_equal T, poller.poll(T + 9, T)
    assert_equal 2, updates.size
  end

  def test_poller_errors_are_reported_and_polling_goes_on
    errors = []
    config.error_handlers << ->(error, context, *) { errors << [error.message, context] }
    s = scheduler(dynamic: true)
    s.define_singleton_method(:update_schedule) { raise "boom" }

    assert_equal T, SidekiqScheduler::Poller.new(s, config).poll(T, T)
    assert_equal [["boom", {context: "sidekiq-scheduler"}]], errors
  end

  def test_the_poller_thread_polls_until_terminated
    leading
    s = loaded({"seconds" => {"every" => "1s", "class" => Report.name}}, now: Time.now - 1)
    poller = SidekiqScheduler::Poller.new(s, config).start
    thread = poller.instance_variable_get(:@thread)

    assert_equal "sidekiq-scheduler", thread.name
    Timeout.timeout(5) { sleep 0.05 until queued("low").size >= 2 }
    poller.terminate

    refute_predicate thread, :alive?
    assert_nil SidekiqScheduler::Poller.new(s, config).terminate
  end

  def test_the_poller_thread_waits_between_polls_and_reloads_on_time
    errors = []
    config.error_handlers << ->(error, *) { errors << error }
    leading
    s = scheduler(dynamic: true, dynamic_every: "1s")
    calls = Hash.new(0)
    s.define_singleton_method(:update_schedule) { calls[:update] += 1 }
    s.define_singleton_method(:enqueue_due) { |_now| calls[:poll] += 1 }
    s.define_singleton_method(:seconds_until_next) { |*| 0.1 }
    poller = SidekiqScheduler::Poller.new(s, config).start
    sleep 1.5
    poller.terminate

    assert_empty errors
    assert_includes 1..3, calls[:update] # at the start, then each second
    assert_includes 2..40, calls[:poll] # about every 0.1 seconds, not in a busy loop
  end

  def test_the_poller_can_be_stopped_from_its_own_thread
    poller = SidekiqScheduler::Poller.new(scheduler, config)
    poller.instance_variable_set(:@thread, Thread.current)

    assert_nil poller.terminate
  end

  # -- Manager and boot ----------------------------------------------------------

  def test_the_manager_saves_a_schedule_that_isnt_dynamic
    Sidekiq.set_schedule("old", {"every" => "1m"})
    settings = SidekiqScheduler::Config.new
    settings.schedule = {"new" => {"every" => "1m", "class" => Report.name}}
    manager_ = SidekiqScheduler::Manager.new(settings)

    assert_same SidekiqScheduler::Scheduler.instance, manager_.instance_variable_get(:@scheduler_instance)
    assert_equal ["new"], Sidekiq.get_schedule.keys
    assert_equal({scheduler: SidekiqScheduler::Scheduler.instance.to_hash}, manager_.to_hash)
  end

  def test_the_manager_leaves_a_dynamic_or_disabled_schedule_in_redis
    Sidekiq.set_schedule("old", {"every" => "1m"})
    [{dynamic: true}, {enabled: false}].each do |settings|
      SidekiqScheduler::Scheduler.instance = nil
      config_ = SidekiqScheduler::Config.new
      config_.schedule = {"new" => {"every" => "1m"}}
      settings.each { |k, v| config_.public_send(:"#{k}=", v) }
      SidekiqScheduler::Manager.new(config_)

      assert_equal ["old"], Sidekiq.get_schedule.keys
    end
  end

  def test_the_manager_takes_settings_given_beforehand
    SidekiqScheduler::Scheduler.enabled = false
    SidekiqScheduler::Scheduler.dynamic = true
    SidekiqScheduler::Scheduler.dynamic_every = "9s"
    SidekiqScheduler::Scheduler.listened_queues_only = true
    Sidekiq.instance_variable_set(:@schedule, {"given" => {"every" => "1m"}})
    settings = SidekiqScheduler::Config.new
    SidekiqScheduler::Manager.new(settings)

    assert_equal({enabled: false, dynamic: true, dynamic_every: "9s", schedule: {"given" => {"every" => "1m"}}, listened_queues_only: true,
                  rufus_scheduler_options: {}}, settings.to_hash)
  end

  def test_the_manager_keeps_settings_not_given
    Sidekiq.instance_variable_set(:@schedule, {})
    settings = SidekiqScheduler::Config.new
    settings.schedule = {"mine" => {}}
    settings.listened_queues_only = true
    SidekiqScheduler::Manager.new(settings)

    assert_equal({enabled: true, dynamic: false, dynamic_every: "5s", schedule: {"mine" => {}}, listened_queues_only: true,
                  rufus_scheduler_options: {}}, settings.to_hash)
  end

  def test_the_manager_starts_and_stops_the_scheduler
    settings = SidekiqScheduler::Config.new
    manager_ = SidekiqScheduler::Manager.new(settings)
    scheduler_ = SidekiqScheduler::Scheduler.instance
    manager_.start

    assert_instance_of SidekiqScheduler::Poller, scheduler_.instance_variable_get(:@poller)
    manager_.stop

    assert_nil scheduler_.instance_variable_get(:@poller)
  end

  def test_boot_runs_the_scheduler_until_quiet
    redis("SADD", manager.schedules_changed_key, "a")
    sidekiq = Sidekiq::Config.new(config)
    sidekiq[:scheduler] = {schedule: {"hourly" => {"cron" => "0 * * * *", "class" => Report.name}}}
    shutdowns = config.lifecycle.fetch(:shutdown).dup
    SidekiqScheduler.boot(sidekiq)
    manager_ = sidekiq[:schedule_manager]

    assert_instance_of SidekiqScheduler::Manager, manager_
    assert_equal ["hourly"], redis("ZRANGE", manager.schedules_changed_key, 0, -1) # the old set's "a" is gone
    assert_equal ["hourly"], SidekiqScheduler::Scheduler.instance.scheduled_jobs.keys
    assert_instance_of SidekiqScheduler::Poller, SidekiqScheduler::Scheduler.instance.instance_variable_get(:@poller)
    config.lifecycle.fetch(:quiet).each(&:call)

    assert_nil SidekiqScheduler::Scheduler.instance.instance_variable_get(:@poller)
    manager_.start
    (config.lifecycle.fetch(:shutdown) - shutdowns).each(&:call)

    assert_nil SidekiqScheduler::Scheduler.instance.instance_variable_get(:@poller)
  end

  # -- loading it ---------------------------------------------------------------

  def test_servers_boot_it_when_its_used_or_configured
    booted = []
    replacing(SidekiqScheduler, :boot, ->(sidekiq) { booted << sidekiq }) do
      replacing(Sidekiq, :scheduler?, -> { false }) { Sidekiq::SCHEDULER_START.call }
      replacing(Sidekiq, :scheduler?, -> { true }) { Sidekiq::SCHEDULER_START.call }
    end

    assert_equal [Sidekiq.default_configuration], booted
    assert_equal 1, config.lifecycle.fetch(:startup).count { |hook| hook.equal?(Sidekiq::SCHEDULER_START) }
    Sidekiq.defaults!(config)

    assert_equal 1, config.lifecycle.fetch(:startup).count { |hook| hook.equal?(Sidekiq::SCHEDULER_START) }
    assert_equal 1, config.lifecycle.fetch(:startup).count { |hook| hook.equal?(Sidekiq::CRON_START) }
  end

  def test_the_scheduler_section_says_its_configured
    assert_false Sidekiq.scheduler_configured?
    Sidekiq.default_configuration[:scheduler] = {}

    assert_true Sidekiq.scheduler_configured?
  end

  def test_the_cli_keeps_the_scheduler_section
    Sidekiq::CLI.apply_settings({scheduler: {dynamic: true}, production: {concurrency: 2}, other: {x: 1}, max_retries: 3}, config)

    assert_equal({dynamic: true}, Sidekiq.default_configuration[:scheduler])
    assert_nil Sidekiq.default_configuration[:production]
    assert_nil Sidekiq.default_configuration[:other]
  end

  def test_the_schedule_methods_load_it_on_first_use
    loader = Sidekiq.const_get(:ScheduleLoader)
    target = Object.new.extend(loader)
    asked = []
    target.define_singleton_method(:require) { |path| (asked << path) && extend(SidekiqScheduler::Schedule) }
    Sidekiq.set_schedule("hourly", {"cron" => "0 * * * *"})

    assert_equal({"cron" => "0 * * * *"}, target.get_schedule("hourly"))
    assert_equal ["sidekiq-scheduler"], asked
    assert_equal %i[get_all_schedules get_schedule reload_schedule! remove_schedule schedule schedule! schedule= set_schedule],
      loader.instance_methods.sort
  end

  def test_its_files_load_it
    assert_equal "6.0.2", SidekiqScheduler::VERSION
    assert_true require("sidekiq/scheduler") || defined?(Sidekiq::Scheduler)
    load File.expand_path("../lib/sidekiq-scheduler/web.rb", __dir__)

    assert_includes @log.string, "sidekiq-scheduler/web: Robin's dashboard doesn't show sidekiq-scheduler jobs"
  end

  # sidekiq-scheduler's web file loads Sidekiq::Web, so routes that
  # require only it (as Mastodon's do) can mount the dashboard.
  def test_its_web_file_loads_sidekiq_web
    code = 'require "sidekiq-scheduler/web"; print defined?(Sidekiq::Web).inspect'
    out, status = Open3.capture2e(RbConfig.ruby, "-rbundler/setup", "-I", File.expand_path("../lib", __dir__), "-e", code)

    assert_predicate status, :success?, out
    assert_equal '"constant"', out.lines.last
  end
end
