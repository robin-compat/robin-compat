# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-cron"
require "timeout"
require "tmpdir"

module CronTestJobs
  class Report
    include Sidekiq::Job

    sidekiq_options queue: "low", retry: 2

    def perform(*) = nil
  end

  # A job class that isn't Sidekiq's (no get_sidekiq_options).
  class Plain
    def self.set(options) = Struct.new(:options) { def perform_async(*args) = "plain-#{options}-#{args}" }.new(options)
  end

  # A constant that isn't a class.
  NOT_A_CLASS = "not a class"
end

class SidekiqCronTest < RobinCompatTest
  include CronTestJobs

  cover "Sidekiq::Cron*"
  cover "Sidekiq.cron_found?"
  cover "Robin::Compat::CronLine*"

  # 30 seconds past a minute, far from now (so saving a job, which claims
  # the current tick, claims another).
  T = Time.utc(2030, 1, 1, 12, 0, 30)

  def setup
    super
    Sidekiq::Cron.reset!
    @log = StringIO.new
    config.logger = Logger.new(@log)
  end

  def teardown
    Sidekiq::Cron.halt
    Sidekiq::Cron.reset!
    super
  end

  def job(attrs = {}) = Sidekiq::Cron::Job.new({"name" => "report", "cron" => "* * * * *", "class" => Report.name}.merge(attrs))

  def saved(attrs = {})
    job(attrs).tap { |j| assert_true j.save }
  end

  def queued(queue) = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  def claims(name = "report", namespace = "default") = redis("ZRANGE", key("cron_job:#{namespace}:#{name}:enqueued"), 0, -1)

  def billing! = Sidekiq::Cron.configuration.available_namespaces = %w[billing]

  # Runs the block with +object+'s +name+ returning +value+.
  def stubbing(object, name, value, &) = replacing(object, name, ->(*) { value }, &)

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

  def in_app(files)
    Dir.mktmpdir do |dir|
      files.each do |name, content|
        FileUtils.mkdir_p(File.dirname(File.join(dir, name)))
        File.write(File.join(dir, name), content)
      end
      Dir.chdir(dir) { yield }
    end
  end

  # -- configuration -------------------------------------------------------

  def test_configuration_defaults_are_sidekiq_crons
    Sidekiq::Cron.instance_variable_set(:@configuration, nil)
    settings = Sidekiq::Cron.configuration

    assert_instance_of Sidekiq::Cron::Configuration, settings
    assert_same settings, Sidekiq::Cron.configuration
    assert_equal [true, 30, "config/schedule.yml", 10, "default", ["default"], :single, 60, nil],
      %i[enabled cron_poll_interval cron_schedule_file cron_history_size default_namespace available_namespaces
        natural_cron_parsing_mode reschedule_grace_period cron_poll_process_count].map { |name| settings.public_send(name) }
  end

  def test_configure_yields_the_configuration_and_reset_starts_over
    Sidekiq::Cron.configure { |c| c.cron_poll_interval = 5 }
    Sidekiq::Cron.configure
    before = Sidekiq::Cron.configuration

    assert_equal 5, before.cron_poll_interval
    reset = Sidekiq::Cron.reset!

    assert_same reset, Sidekiq::Cron.configuration
    refute_same before, reset
    assert_equal 30, Sidekiq::Cron.configuration.cron_poll_interval
  end

  def test_parsing_mode_and_process_count_are_checked
    settings = Sidekiq::Cron.configuration
    settings.natural_cron_parsing_mode = :strict
    settings.natural_cron_parsing_mode = :single
    settings.cron_poll_process_count = 2

    assert_equal [:single, 2], [settings.natural_cron_parsing_mode, settings.cron_poll_process_count]
    settings.natural_cron_parsing_mode = :strict

    assert_equal :strict, settings.natural_cron_parsing_mode
    error = assert_raises(ArgumentError) { settings.natural_cron_parsing_mode = :loose }

    assert_equal "Unknown natural cron parsing mode: :loose", error.message
    assert_raises(ArgumentError) { settings.natural_cron_parsing_mode = nil }
    assert_raises(ArgumentError) { settings.cron_poll_process_count = 0 }
    error = assert_raises(ArgumentError) { settings.cron_poll_process_count = "2" }

    assert_equal 'invalid cron process count: "2"', error.message
    assert_raises(ArgumentError) { settings.cron_poll_process_count = 1.5 }
  end

  # -- namespaces ------------------------------------------------------------

  def test_namespaces_are_the_available_ones_and_the_default
    settings = Sidekiq::Cron.configuration

    assert_equal ["default"], Sidekiq::Cron::Namespace.all
    billing!

    assert_equal %w[billing default], Sidekiq::Cron::Namespace.all
    assert_true Sidekiq::Cron::Namespace.available_namespaces_provided?
    settings.available_namespaces = nil

    assert_equal ["default"], Sidekiq::Cron::Namespace.all
    assert_false Sidekiq::Cron::Namespace.available_namespaces_provided?
    settings.available_namespaces = :auto

    assert_false Sidekiq::Cron::Namespace.available_namespaces_provided?
    saved("namespace" => "billing")
    redis("SADD", "cron_jobs:stray#{Process.pid}", "x") # without this test's prefix

    assert_equal %w[billing default], Sidekiq::Cron::Namespace.all
    assert_equal [{count: 1, name: "billing"}, {count: 0, name: "default"}], Sidekiq::Cron::Namespace.all_with_count
    assert_equal [1, 0], [Sidekiq::Cron::Namespace.count("billing"), Sidekiq::Cron::Namespace.count]
    settings.available_namespaces = "billing"
    error = assert_raises(ArgumentError) { Sidekiq::Cron::Namespace.all }

    assert_equal 'Unexpected value provided for `available_namespaces`: "billing"', error.message
  ensure
    redis("DEL", "cron_jobs:stray#{Process.pid}")
  end

  def test_an_unlisted_namespace_is_the_default
    billing!

    assert_equal %w[billing default default], [job("namespace" => "billing"), job("namespace" => "other"), job].map(&:namespace)
    assert_includes @log.string, "Cron Jobs - unexpected namespace other encountered. Assigning to default namespace."
    refute_includes @log.string, "namespace billing"
    refute_includes @log.string, "namespace default"
  end

  # -- schedules ---------------------------------------------------------------

  def test_a_schedule_can_end_with_a_time_zone
    parsed = %w[America/Chicago Etc/GMT+5 UTC GMT Z +05:30].map { |zone| Sidekiq::Cron.parse("0 22 * * 1-5 #{zone}") }

    assert_equal %w[America/Chicago Etc/GMT+5 UTC GMT Z +05:30], parsed.map { |cron| cron.instance_variable_get(:@tz) }
    assert_equal ["0 22 * * 1-5"], parsed.map(&:to_s).uniq
    assert_equal Time.utc(2030, 1, 1, 4), Sidekiq::Cron.parse("0 22 * * * America/Chicago").previous_time(T)
    assert_equal "30 0 22 * * *", Sidekiq::Cron.parse("30 0 22 * * * UTC").to_s
  end

  def test_a_schedule_without_a_zone_has_none
    ["0 0 * * *", "0 0 * * */2", "0 0 * * MON-FRI", "0 0 * * 6", "*/30 * * * * *", "@hourly"].each do |line|
      assert_nil Sidekiq::Cron.parse(line).instance_variable_get(:@tz), line
    end
    assert_equal "*/30 * * * * *", Sidekiq::Cron.parse("*/30 * * * * *").to_s
    assert_equal "@hourly", Sidekiq::Cron.parse("@hourly").to_s
    assert_raises(Robin::Cron::ParseError) { Sidekiq::Cron.parse("nonsense") }
    assert_raises(Robin::Cron::ParseError) { Sidekiq::Cron.parse("0 0 * * 1-5 a/") }
    assert_equal "* * * * *", Sidekiq::Cron.parse("UTC").to_s # as fugit reads it, so as sidekiq-cron does
    assert_equal ["@hourly", "UTC"], Sidekiq::Cron.parse("@hourly UTC").then { |cron| [cron.to_s, cron.instance_variable_get(:@tz)] }
    assert_equal "* * * * *", Sidekiq::Cron.parse("").to_s # fugit's reading too (a job's blank cron is invalid before this)
  end

  def test_natural_language_schedules_go_through_fugit
    assert_equal ["0 5 * * *", nil], Sidekiq::Cron.parse("every day at five").then { |cron| [cron.to_s, cron.instance_variable_get(:@tz)] }
    assert_equal "15 3 * * *", Sidekiq::Cron.parse("every day at 3:15 and 4:30").to_s
    assert_equal "America/Chicago", Sidekiq::Cron.parse("every day at five in America/Chicago").instance_variable_get(:@tz)
    Sidekiq::Cron.configuration.natural_cron_parsing_mode = :strict

    assert_equal "0 5 * * *", Sidekiq::Cron.parse("every day at five").to_s
    assert_raises(ArgumentError) { Sidekiq::Cron.parse("every day at 3:15 and 4:30") }
    error = assert_raises(Robin::Cron::ParseError) { Sidekiq::Cron.parse("61 * * * *") }

    assert_equal '"61" is out of range 0-59', error.message
  end

  def test_natural_language_needs_fugit
    asked = []
    replacing(Robin::Compat::CronLine, :require, ->(path) { (asked << path) && raise(LoadError, path) }) do
      error = assert_raises(Robin::Cron::ParseError) { Sidekiq::Cron.parse("every day at five") }

      assert_equal 'cron expression must have 5 or 6 fields: "every day at five" (natural-language schedules need the fugit gem)',
        error.message
      assert_equal "* * * * *", Sidekiq::Cron.parse("* * * * *").to_s
    end

    assert_equal ["fugit"], asked
  end

  # -- a job's attributes --------------------------------------------------------

  def test_a_job_takes_string_or_symbol_keys_and_class_or_klass
    built = [job, Sidekiq::Cron::Job.new(name: "report", cron: "* * * * *", klass: Report)]

    assert_equal [["report", "* * * * *", Report.name, "default", "dynamic", "enabled", nil, nil],
      ["report", "* * * * *", Report, "default", "dynamic", "enabled", nil, nil]],
      built.map { |j| [j.name, j.cron, j.klass, j.namespace, j.source, j.status, j.description, j.last_enqueue_time] }
    assert_equal %w[schedule dynamic], [job("source" => "schedule").source, job("source" => "other").source]
    assert_equal ["d", "low"], [job("description" => "d").description, Sidekiq::Cron::Job.new(klass: Report).message.fetch("queue")]
    assert_false Sidekiq::Cron::Job.new.valid?
  end

  def test_a_jobs_message_has_its_class_options_queue_retry_and_args
    assert_equal({"queue" => "low", "retry" => 2, "class" => Report.name, "args" => []}, job.message)
    assert_equal({"queue" => "cron", "retry" => "5", "class" => Report.name, "args" => [1]},
      job("queue" => "cron", "retry" => "5", "args" => [1]).message)
    assert_equal({"queue" => "default", "class" => "Missing", "args" => []}, job("class" => "Missing").message)
    assert_equal({"class" => Plain.name, "args" => []}, job("class" => Plain).message)
    assert_equal({"queue" => "low", "retry" => false, "class" => Report.name, "args" => []}, job("retry" => false).message)
    assert_equal({"queue" => "low", "retry" => 2, "class" => Report.name, "args" => []}, job("queue" => nil, "retry" => nil, "message" => nil).message)
    assert_equal({"queue" => "low", "retry" => 2}, Report.get_sidekiq_options)
    assert_nil job("class" => nil).message
  end

  def test_a_stored_message_gives_the_queue_and_retry
    stored = job("message" => JSON.generate({"queue" => "q", "retry" => 3}))

    assert_equal ['{"queue":"q","retry":3}', 3, "q"], [stored.message, stored.to_hash.fetch(:retry), stored.queue_name_with_prefix]
    assert_nil job("message" => "null").to_hash.fetch(:retry)
    assert_equal %w[default default h], [job("message" => "null"), job("message" => "{}"), job("message" => {"queue" => "h"})].map(&:queue_name_with_prefix)
    assert_equal({"queue" => "h", "args" => []}, job("message" => {"queue" => "h"}).sidekiq_worker_message)
  end

  def test_args_are_taken_as_sidekiq_cron_takes_them
    assert_equal [[1, "a"], ["not json"], [{"a" => 1}], [1, 2], [], [7], [1, 2, 3], ["x"]],
      ['[1,"a"]', "not json", {"a" => 1}, [1, 2], nil, 7, (1..3), '"x"'].map { |args| Array(job("args" => args).args) }
    assert_equal({"b" => 2}, job("args" => '{"b":2}').args)
    assert_equal ["not json"], job("args" => "not json").args
    changed = job
    changed.args = [3]

    assert_equal [3], changed.args
  end

  def test_symbolize_args_is_for_active_jobs_only
    plain = job("symbolize_args" => true, "args" => [{"a" => 1}])

    assert_equal [{a: 1}], plain.args
    assert_includes @log.string, "Cron Jobs - 'symbolize_args' is gonna be ignored, as it is only used when 'active_job' is true"
    @log.truncate(0)
    @log.rewind
    active = {"class" => "MissingJob", "active_job" => "1"}

    assert_equal [{a: 1}, 2], job(active.merge("symbolize_args" => "yes", "args" => [{"a" => 1}, 2])).args
    assert_equal({a: 1}, job(active.merge("symbolize_args" => "t", "args" => '{"a":1}')).args)
    assert_equal [{a: 1}], job(active.merge("symbolize_args" => "1", "args" => {"a" => 1})).args
    assert_equal "x", job(active.merge("symbolize_args" => "1", "args" => '"x"')).args
    assert_equal [{"a" => 1}], job("symbolize_args" => "0", "args" => [{"a" => 1}]).args
    assert_equal [{"a" => 1}], job(active.merge("args" => [{"a" => 1}])).args
    assert_empty @log.string
  end

  def test_flags_read_like_sidekiq_crons
    values = [true, "true", "T", "yes", "Y", "1", false, "0", "no", nil, "", "truthy"]

    assert_equal [true] * 6 + [false] * 6, values.map { |value| job("date_as_argument" => value).date_as_argument? }
  end

  def test_status_and_last_enqueue_time_come_from_redis_unless_given
    saved("status" => "disabled", "last_enqueue_time" => "2030-01-01 11:59:00 +0000")
    reread = job

    assert_equal ["disabled", Time.utc(2030, 1, 1, 11, 59)], [reread.status, reread.last_enqueue_time]
    assert_predicate reread.last_enqueue_time, :utc?
    given = job("status" => "enabled", "last_enqueue_time" => "2030-01-01T06:58:00-05:00")

    assert_equal ["enabled", Time.utc(2030, 1, 1, 11, 58)], [given.status, given.last_enqueue_time]
    assert_predicate given.last_enqueue_time, :utc?
    unfetched = job("fetch_missing_args" => false)

    assert_equal ["enabled", nil, false], [unfetched.status, unfetched.last_enqueue_time, unfetched.fetch_missing_args]
    assert_true job("fetch_missing_args" => nil).fetch_missing_args
    redis("HSET", key("cron_job:default:report"), "last_enqueue_time", "garbage")

    assert_nil job.last_enqueue_time
    redis("HSET", key("cron_job:default:report"), "last_enqueue_time", "")

    assert_nil job.last_enqueue_time
    assert_true job.fetch_missing_args
  end

  def test_validity_and_errors
    assert_empty job.errors
    assert_true job.valid?
    assert_empty job.tap(&:valid?).errors
    Sidekiq::Cron.configuration.available_namespaces = nil
    bad = Sidekiq::Cron::Job.new("namespace" => "*", "cron" => "", "class" => "")

    assert_false bad.valid?
    assert_equal ["'name' must be set", "'namespace' cannot be '*'", "'cron' must be set", "'klass' (or class) must be set"], bad.errors
    bad.name = "fixed"
    bad.namespace = "x"
    bad.cron = "* * * * *"
    bad.klass = "X"

    assert_true bad.valid?
    assert_empty bad.errors
    assert_equal ["'name' must be set", "'cron' must be set", "'klass' (or class) must be set"],
      Sidekiq::Cron::Job.new.tap(&:valid?).errors
    assert_equal ["'cron' -> \"61 * * * *\" -> Robin::Cron::ParseError: \"61\" is out of range 0-59"],
      job("cron" => "61 * * * *").tap(&:valid?).errors
    assert_equal [true, true, false, false], [Report, "Report", 7, ""].map { |klass| job("class" => klass).klass_valid }
    [nil, ""].each do |namespace|
      blank = job
      blank.namespace = namespace

      assert_equal ["'namespace' must be set"], blank.tap(&:valid?).errors
    end
  end

  def test_to_hash_is_what_redis_stores
    full = job("class" => "MissingJob", "description" => "d", "args" => [1], "date_as_argument" => true, "active_job" => true,
      "queue_name_prefix" => "p", "queue_name_delimiter" => ".", "symbolize_args" => true,
      "last_enqueue_time" => "2030-01-01 11:59:00 +0000")

    assert_equal({name: "report", namespace: "default", klass: "MissingJob", cron: "* * * * *", description: "d", source: "dynamic",
                  args: "[1]", date_as_argument: "1", message: JSON.generate(full.message), status: "enabled", active_job: "1",
                  queue_name_prefix: "p", queue_name_delimiter: ".", retry: nil, last_enqueue_time: "2030-01-01 11:59:00 +0000",
                  symbolize_args: "1"}, full.to_hash)
    assert_equal({args: "[]", date_as_argument: "0", active_job: "0", symbolize_args: "0", last_enqueue_time: nil, message: "{}", klass: ""},
      Sidekiq::Cron::Job.new("args" => []).to_hash.slice(:args, :date_as_argument, :active_job, :symbolize_args, :last_enqueue_time, :message, :klass))
    assert_equal [nil, "true", "false", 3, 2.5], [nil, true, false, 3, 2.5].map { |value| job("message" => JSON.generate({"retry" => value})).to_hash.fetch(:retry) }
    raw = job
    raw.message = "x"
    raw.instance_variable_set(:@args, "raw")

    assert_equal ["x", "raw"], raw.to_hash.values_at(:message, :args)
  end

  # -- saving and finding --------------------------------------------------------

  def test_save_stores_the_job_as_sidekiq_cron_does
    now = Time.now.utc

    assert_true job("args" => [1], "description" => nil).save
    assert_equal ["cron_job:default:report"], redis("SMEMBERS", key("cron_jobs:default"))
    stored = redis("HGETALL", key("cron_job:default:report")).each_slice(2).to_h

    assert_equal({"name" => "report", "namespace" => "default", "klass" => Report.name, "cron" => "* * * * *", "description" => "",
                  "source" => "dynamic", "args" => "[1]", "date_as_argument" => "0", "status" => "enabled", "active_job" => "0",
                  "queue_name_prefix" => "", "queue_name_delimiter" => "", "retry" => "2", "last_enqueue_time" => "",
                  "symbolize_args" => "0", "message" => JSON.generate(job("args" => [1]).message)}, stored)
    claim, score = redis("ZRANGE", key("cron_job:default:report:enqueued"), 0, -1, "WITHSCORES")

    assert_equal job.formatted_last_time(Time.now), claim
    assert_in_delta now.to_f, Float(score), 2
    refute_equal Float(score).floor, Float(score), "the time, to the microsecond"
    assert_includes @log.string, "Cron Jobs - added job with name report in the namespace default"
  end

  def test_saving_again_keeps_the_claimed_ticks
    saved
    redis("DEL", key("cron_job:default:report:enqueued"))
    redis("ZADD", key("cron_job:default:report:enqueued"), 1, "old")
    @log.truncate(0)
    @log.rewind

    assert_true job("args" => [2]).save
    assert_equal ["old"], claims
    assert_equal [2], Sidekiq::Cron::Job.find("report").args
    assert_empty @log.string
    assert_false job("cron" => "").save
  end

  def test_find_all_count_and_exists
    billing!
    saved
    saved("name" => "b", "namespace" => "billing")
    redis("SADD", key("cron_jobs:default"), "cron_job:default:gone")

    assert_equal "report", Sidekiq::Cron::Job.find("report").name
    assert_equal "report", Sidekiq::Cron::Job.find({name: "report"}).name
    assert_equal "report", Sidekiq::Cron::Job.find({"name" => "report"}).name
    assert_nil Sidekiq::Cron::Job.find({})
    assert_equal "b", Sidekiq::Cron::Job.find("b", "billing").name
    assert_nil Sidekiq::Cron::Job.find("b")
    assert_equal ["report"], Sidekiq::Cron::Job.all.map(&:name)
    assert_equal ["b"], Sidekiq::Cron::Job.all("billing").map(&:name)
    assert_empty Sidekiq::Cron::Job.all("none")
    assert_equal [false], Sidekiq::Cron::Job.all.map(&:fetch_missing_args)
    assert_equal %w[b report], Sidekiq::Cron::Job.all("*").map(&:name).sort
    assert_equal [2, 3, 1], [Sidekiq::Cron::Job.count, Sidekiq::Cron::Job.count("*"), Sidekiq::Cron::Job.count("billing")]
    assert_equal [true, false, true], [Sidekiq::Cron::Job.exists?("report"), Sidekiq::Cron::Job.exists?("b"), Sidekiq::Cron::Job.exists?("b", "billing")]
    assert_equal [true, true, false], [job.exists?, job("name" => "b", "namespace" => "billing").exists?, job("name" => "b").exists?]
  end

  def test_find_skips_an_invalid_stored_job
    saved
    redis("HSET", key("cron_job:default:report"), "cron", "nonsense")

    assert_nil Sidekiq::Cron::Job.find("report")
  end

  def test_create_and_destroy
    billing!

    assert_true Sidekiq::Cron::Job.create("name" => "report", "cron" => "* * * * *", "class" => Report.name)
    assert_false Sidekiq::Cron::Job.create("name" => "bad")
    Sidekiq::Cron::Job.find("report").enqueue!(T)

    assert_true Sidekiq::Cron::Job.destroy({name: "report"})
    assert_empty redis("KEYS", key("cron*"))
    assert_includes @log.string, "Cron Jobs - deleted job with name report from namespace default"
    assert_false Sidekiq::Cron::Job.destroy("report")
    saved("namespace" => "billing")
    saved

    assert_true Sidekiq::Cron::Job.destroy("report", "billing")
    assert_equal [[], ["cron_job:default:report"]], [redis("SMEMBERS", key("cron_jobs:billing")), redis("SMEMBERS", key("cron_jobs:default"))]
  end

  def test_destroy_all_destroys_the_default_namespaces_jobs
    billing!
    saved
    saved("name" => "b", "namespace" => "billing")
    Sidekiq::Cron::Job.destroy_all!

    assert_equal [0, 1], [Sidekiq::Cron::Job.count, Sidekiq::Cron::Job.count("billing")]
    assert_includes @log.string, "Cron Jobs - deleted all jobs"
  end

  def test_keys_are_sidekiq_crons
    assert_equal %w[cron_jobs:default cron_jobs:billing cron_job:default:r cron_job:billing:r cron_job:default:r:enqueued
      cron_job:billing:r:enqueued cron_job:default:r:jid_history cron_job:billing:r:jid_history cron_jobs:default],
      [Sidekiq::Cron::Job.jobs_key, Sidekiq::Cron::Job.jobs_key("billing"), Sidekiq::Cron::Job.redis_key("r"),
        Sidekiq::Cron::Job.redis_key("r", "billing"), Sidekiq::Cron::Job.job_enqueued_key("r", ""),
        Sidekiq::Cron::Job.job_enqueued_key("r", "billing"), Sidekiq::Cron::Job.jid_history_key("r"),
        Sidekiq::Cron::Job.jid_history_key("r", "billing"), Sidekiq::Cron::Job.jobs_key("")]
    assert_equal "cron_job:default:r:enqueued", Sidekiq::Cron::Job.job_enqueued_key("r")
    billing!

    assert_equal %w[cron_job:billing:report cron_job:billing:report:enqueued cron_job:billing:report:jid_history],
      job("namespace" => "billing").then { |j| [j.redis_key, j.job_enqueued_key, j.jid_history_key] }
  end

  # -- loading many --------------------------------------------------------------

  def test_load_from_hash_and_array
    errors = Sidekiq::Cron::Job.load_from_hash({"a" => {"cron" => "* * * * *", "class" => Report.name}, "bad" => {"class" => "X"}})

    assert_equal({"bad" => ["'cron' must be set"]}, errors)
    assert_equal({}, Sidekiq::Cron::Job.load_from_array([{name: "b", cron: "@hourly", class: Report.name}], source: "schedule"))
    assert_equal({}, Sidekiq::Cron::Job.load_from_array([{name: "c", cron: "@hourly", class: Report.name}]))
    assert_equal [%w[a dynamic], %w[b schedule], %w[c dynamic]], Sidekiq::Cron::Job.all.map { |j| [j.name, j.source] }.sort
  end

  # The schedule's jobs that are loaded again keep their history; the ones
  # it no longer has go.
  def test_bang_loads_destroy_the_schedules_jobs_they_lack
    schedule = {"kept" => {"cron" => "* * * * *", "class" => Report.name}, "dropped" => {"cron" => "* * * * *", "class" => Report.name}}
    Sidekiq::Cron::Job.load_from_hash(schedule, source: "schedule")
    Sidekiq::Cron::Job.find("kept").enqueue!(T)
    saved("name" => "dynamic")

    assert_equal({}, Sidekiq::Cron::Job.load_from_hash!({"kept" => {"cron" => "@daily", "class" => Report.name}}, source: "schedule"))
    assert_equal %w[dynamic kept], Sidekiq::Cron::Job.all.map(&:name).sort
    assert_equal ["@daily", T], Sidekiq::Cron::Job.find("kept").then { |j| [j.cron, j.last_enqueue_time] }
    Sidekiq::Cron::Job.load_from_array!([{"name" => "kept", "cron" => "@daily", "class" => Report.name, "source" => "schedule"}])

    assert_equal [T, 1], Sidekiq::Cron::Job.find("kept").then { |j| [j.last_enqueue_time, j.jid_history_from_redis.size] }
    Sidekiq::Cron::Job.load_from_array!([{name: "kept", cron: "@daily", class: Report.name}], source: "schedule")

    assert_equal [T, 1], Sidekiq::Cron::Job.find("kept").then { |j| [j.last_enqueue_time, j.jid_history_from_redis.size] }
    Sidekiq::Cron::Job.load_from_hash!({"kept" => {"cron" => "@daily", "class" => Report.name, "source" => "schedule"}})

    assert_equal [T, 1], Sidekiq::Cron::Job.find("kept").then { |j| [j.last_enqueue_time, j.jid_history_from_redis.size] }
    Sidekiq::Cron::Job.load_from_array!([{"name" => "new", "cron" => "@daily", "class" => Report.name}], source: "schedule")

    assert_equal %w[dynamic new], Sidekiq::Cron::Job.all.map(&:name).sort
    assert_equal({nil => ["'name' must be set"]}, Sidekiq::Cron::Job.load_from_array!([{cron: "@daily", class: Report.name}]))
  end

  def test_destroy_removed_jobs_returns_their_names
    billing!
    Sidekiq::Cron::Job.load_from_hash({"a" => {"cron" => "* * * * *", "class" => Report.name},
      "b" => {"cron" => "* * * * *", "class" => Report.name, "namespace" => "billing"}}, source: "schedule")

    assert_equal %w[b], Sidekiq::Cron::Job.destroy_removed_jobs(%w[a])
    assert_equal [["a"], []], [Sidekiq::Cron::Job.all.map(&:name), Sidekiq::Cron::Job.all("billing")]
  end

  def test_jobs_from_before_namespaces_move_to_the_default_one
    Sidekiq::Cron.configuration.available_namespaces = nil
    redis("SADD", key("cron_jobs"), "cron_job:old")
    redis("HSET", key("cron_job:old"), "name", "old", "cron", "* * * * *", "klass", Report.name, "namespace", "x")
    Sidekiq::Cron::Job.migrate_old_jobs_if_needed!

    assert_equal [[], "default"], [redis("SMEMBERS", key("cron_jobs")), Sidekiq::Cron::Job.find("old").namespace]
    assert_equal ["cron_job:default:old"], redis("SMEMBERS", key("cron_jobs:default"))
  end

  # -- when jobs are due ---------------------------------------------------------

  def test_the_last_tick_and_its_formats
    assert_equal Time.utc(2030, 1, 1, 12), job.last_time(T)
    assert_equal Time.utc(2030, 1, 1, 12), job.last_time(Time.new(2030, 1, 1, 7, 0, 30, "-05:00"))
    assert_equal [Time.utc(2030, 1, 1, 12).to_f.to_s, "2030-01-01T12:00:00Z"], [job.formatted_enqueue_time(T), job.formatted_last_time(T)]
    assert_operator job.last_time, :<=, Time.now.utc
    assert_match(/\A\d+\.0\z/, job.formatted_enqueue_time)
    assert_match(/Z\z/, job.formatted_last_time)
  end

  def test_should_enqueue_claims_each_tick_once
    due = saved

    assert_true due.should_enqueue?(T)
    assert_false due.should_enqueue?(T)
    assert_true due.should_enqueue?(T + 60)
    assert_equal ["2030-01-01T12:00:00Z", "2030-01-01T12:01:00Z"], redis("ZRANGEBYSCORE", key("cron_job:default:report:enqueued"), T.to_f - 60, "+inf")
  end

  def test_should_enqueue_skips_disabled_late_and_already_enqueued_ticks
    assert_false job("status" => "disabled").should_enqueue?(T)
    hourly = job("name" => "hourly", "cron" => "0 * * * *", "status" => "enabled")

    assert_false hourly.should_enqueue?(T + 120), "2.5 minutes late"
    Sidekiq::Cron.configuration.reschedule_grace_period = 150

    assert_true hourly.should_enqueue?(T + 120)
    assert_false job("status" => "enabled", "last_enqueue_time" => "2030-01-01 12:00:00 +0000").should_enqueue?(T)
    assert_false job("status" => "enabled", "last_enqueue_time" => "2030-01-01 12:00:20 +0000").should_enqueue?(T)
    assert_true job("status" => "enabled", "last_enqueue_time" => "2030-01-01 11:59:59 +0000").should_enqueue?(T)
  end

  def test_test_and_enqueue_enqueues_due_jobs_and_forgets_old_ticks
    due = saved("args" => [1])
    at = T + 0.5
    redis("ZADD", key("cron_job:default:report:enqueued"), -5, "negative", 0.5, "first", at.to_f - 86_400.25, "ancient",
      at.to_f - 86_399, "recent")
    due.test_and_enqueue_for_time!(at)

    assert_equal [[Report.name, [1], 2]], queued("low").map { |j| j.values_at("class", "args", "retry") }
    due.test_and_enqueue_for_time!(at)

    assert_equal 1, queued("low").size
    assert_includes claims, "recent"
    refute_includes claims, "ancient"
    refute_includes claims, "first"
    refute_includes claims, "negative"
    due.remove_previous_enqueues(T + 86_400)

    refute_includes claims, "recent"
  end

  # -- enqueueing ----------------------------------------------------------------

  def test_enqueue_pushes_a_sidekiq_job_with_its_queue_and_retry
    now = Time.now.utc
    (report = job("args" => [1, "a"], "queue" => "cron", "retry" => "15")).enqueue!

    assert_predicate report.last_enqueue_time, :utc? # in UTC, not a local zone that might be UTC
    %w[true false some].each { |value| job("name" => value, "retry" => value).enqueue! }

    assert_equal [[[1, "a"], 15]], queued("cron").map { |j| j.values_at("args", "retry") }
    assert_equal [true, false, "some"], queued("low").map { |j| j.fetch("retry") }.reverse
    stored = redis("HGET", key("cron_job:default:report"), "last_enqueue_time")

    assert_match(/ \+0000\z/, stored)
    assert_in_delta now.to_i, Time.parse(stored).to_i, 2
  end

  def test_enqueue_without_a_retry_leaves_the_classs
    pushed = job("class" => Plain).tap { |j| j.instance_variable_set(:@retry, nil) }
    pushed.enqueue!(T)

    assert_equal "plain-{queue: \"default\"}-[]", pushed.jid_history_from_redis.first.fetch("jid")
    assert_equal T, pushed.last_enqueue_time
  end

  def test_enqueue_pushes_the_message_of_a_class_it_cant_load
    missing = job("class" => "Missing", "args" => [1], "retry" => "3")
    missing.enqueue!
    job("class" => "Missing", "name" => "b", "message" => JSON.generate({"class" => "Missing", "queue" => "q"})).enqueue!

    assert_equal [["Missing", [1], 3]], queued("default").map { |j| j.values_at("class", "args", "retry") }
    assert_equal [["Missing", [], true]], queued("q").map { |j| j.values_at("class", "args", "retry") }
    assert_equal "3", missing.message.fetch("retry"), "the message isn't changed"
  end

  def test_enqueue_pushes_an_active_jobs_wrapper_for_a_class_it_cant_load
    missing = job("class" => "MissingJob", "active_job" => "1", "args" => [1], "description" => "d")
    missing.enqueue!
    wrapper = queued("default").first

    assert_equal ["ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper", "MissingJob", "default", "d"],
      wrapper.values_at("class", "wrapped", "queue", "description")
    arguments = wrapper.fetch("args").first

    assert_equal ["MissingJob", "default", [1]], arguments.values_at("job_class", "queue_name", "arguments")
    assert_match(/\A\h{8}-/, arguments.fetch("job_id"))
    prefixed = job("class" => "MissingJob", "active_job" => "1", "queue_name_prefix" => "p")

    stubbing(prefixed, :active_job_base, nil) do
      assert_equal ["p_default", "p_default"], prefixed.active_job_message.then { |m| [m.fetch("queue"), m.fetch("args").first.fetch("queue_name")] }
    end
  end

  def test_date_as_argument_adds_the_time
    job("args" => [1], "date_as_argument" => "1").enqueue!
    args = queued("low").first.fetch("args")

    assert_equal 1, args.first
    assert_kind_of Float, args.last
    assert_in_delta Time.now.to_f, args.last, 5
    assert_equal [1], job("args" => [1]).enqueue_args
  end

  def test_the_history_keeps_cron_history_size_enqueues
    Sidekiq::Cron.configuration.cron_history_size = "3"
    pushed = saved
    4.times { |n| pushed.enqueue!(T + n) }
    history = pushed.jid_history_from_redis

    assert_equal 3, history.size
    assert_equal (T + 3).to_s, history.first.fetch("enqueued")
    assert_match(/\A\h{24}\z/, history.first.fetch("jid"))
    redis("RPUSH", key("cron_job:default:report:jid_history"), *Array.new(200) { JSON.generate({"jid" => "x"}) })

    assert_equal 203, pushed.jid_history_from_redis.size
  end

  def test_enqueue_logs_at_debug_level
    config.logger.level = Logger::DEBUG
    job.enqueue!

    assert_includes @log.string, "enqueued report: #{job.message}"
  end

  def test_a_single_key_hash_argument_that_isnt_a_global_id_stays
    assert_equal [{"b" => 2}], job("args" => [{"b" => 2}]).enqueue_args
  end

  # -- Active Job without Active Job loaded ------------------------------------------

  def test_without_active_job_only_the_flag_makes_one
    plain = job

    stubbing(plain, :active_job_base, nil) do
      assert_false plain.is_active_job?
      assert_false plain.is_active_job?(Report)
      assert_equal "low", plain.queue_name_with_prefix
    end
    flagged = job("class" => "MissingJob", "active_job" => "1", "queue_name_prefix" => "p")

    stubbing(flagged, :active_job_base, nil) do
      assert_true flagged.is_active_job?
      assert_equal "p_default", flagged.queue_name_with_prefix
    end
    unprefixed = job("class" => "MissingJob", "active_job" => "1")

    stubbing(unprefixed, :active_job_base, nil) { assert_equal "default", unprefixed.queue_name_with_prefix }
    assert_false job("class" => "CronTestJobs::NOT_A_CLASS").is_active_job?
    assert_equal "low", job("queue_name_prefix" => "p", "queue_name_delimiter" => "-").queue_name_with_prefix
  end

  def test_the_active_job_queue_prefix_and_delimiter
    base = Struct.new(:queue_name_delimiter, :queue_name_prefix)
    flagged = job("class" => "MissingJob", "active_job" => "1")

    stubbing(flagged, :active_job_base, base.new(".", "app")) { assert_equal "app.default", flagged.queue_name_with_prefix }
    stubbing(flagged, :active_job_base, base.new("", nil)) { assert_equal "default", flagged.queue_name_with_prefix }
    own = job("class" => "MissingJob", "active_job" => "1", "queue_name_prefix" => "mine", "queue_name_delimiter" => "-")

    stubbing(own, :active_job_base, base.new(".", "app")) { assert_equal "mine-default", own.queue_name_with_prefix }
    plain = job

    stubbing(plain, :active_job_base, Class.new { def self.queue_name_prefix = "app" }) { assert_equal "low", plain.queue_name_with_prefix }
  end

  def test_without_global_id_arguments_stay_as_they_are
    plain = job

    stubbing(plain, :global_id_identification, nil) do
      plain.args = 7

      assert_equal [7], plain.args
    end
    identified = Class.new { def to_global_id = "gid://app/Thing/1" }
    found = job

    stubbing(found, :global_id_identification, identified) do
      found.args = identified.new

      assert_equal [{"_sc_globalid" => "gid://app/Thing/1"}], found.args
      found.args = [{"a" => [identified.new]}]

      assert_equal [{"a" => [{"_sc_globalid" => "gid://app/Thing/1"}]}], found.args
      found.args = {"a" => identified.new}

      assert_equal [{"a" => {"_sc_globalid" => "gid://app/Thing/1"}}], found.args
    end
  end

  # -- state ---------------------------------------------------------------------

  def test_enable_and_disable
    toggled = saved

    assert_true toggled.disable!
    assert_equal ["disabled", true, false], [job.status, toggled.disabled?, toggled.enabled?]
    assert_true toggled.enable!
    assert_equal ["enabled", false, true], [job.status, toggled.disabled?, toggled.enabled?]
  end

  def test_descriptions_and_sorting
    assert_equal ["every minute", "nonsense"], [job.human_cron, job("cron" => "nonsense").human_cron]
    assert_equal "* * * * *", job.cron_expression_string
    assert_equal ["0_report", "1_report"], [job.sort_name, job("status" => "disabled").sort_name]
    assert_equal "0_r", job("name" => "R").sort_name
    assert_equal JSON.pretty_generate(job.message), job("message" => JSON.generate(job.message)).pretty_message
    raw = job
    raw.message = "not json"

    assert_equal ["not json", job.message], [raw.pretty_message, job.pretty_message]
  end

  # -- polling -------------------------------------------------------------------

  # Polls on its config's leader, waiting the interval between polls, in a
  # thread of its own.
  def test_the_poller_enqueues_due_jobs_while_leading
    other = Sidekiq.defaults!(Robin::Config.new)
    leader = Struct.new(:leading) { def leader? = leading }.new(false)
    Robin::Leader.register(other, leader)
    polls = Thread::Queue.new
    poller = Sidekiq::Cron::Poller.new(other, interval: 60)
    poller.define_singleton_method(:enqueue) { |*| polls << :poll }
    poller.start
    sleep 0.1

    assert_empty polls
    leader.leading = true
    poller.instance_variable_get(:@wake) << :wake
    Timeout.timeout(5) { polls.pop }
    sleep 0.1

    assert_empty polls, "one poll an interval"
    thread = poller.instance_variable_get(:@thread)

    assert_equal "sidekiq.cron", thread.name
    poller.terminate

    refute_predicate thread, :alive?
  ensure
    Robin::Leader.unregister(other)
  end

  # Every namespace's jobs.
  def test_the_poller_enqueues_the_jobs_due
    billing!
    saved("args" => [1])
    saved("name" => "b", "namespace" => "billing", "args" => [2])
    redis("DEL", key("cron_job:default:report:enqueued"), key("cron_job:billing:b:enqueued"))
    Sidekiq::Cron::Poller.new(config, interval: 1).enqueue

    assert_equal [[1], [2]], queued("low").map { |j| j.fetch("args") }.sort
  end

  # A job that's invalid (its schedule, here) is skipped.
  def test_the_poller_reports_errors_and_skips_invalid_jobs
    errors = []
    config.error_handlers << ->(error, context, _config) { errors << [error.message, context] }
    saved("name" => "broken")
    redis("HSET", key("cron_job:default:broken"), "cron", "nonsense")
    redis("DEL", key("cron_job:default:broken:enqueued"))
    poller = Sidekiq::Cron::Poller.new(config, interval: 1)
    poller.enqueue(T)

    assert_empty errors
    assert_empty queued("low")
    saved
    failing = Sidekiq::Cron::Job.find("report")
    failing.define_singleton_method(:test_and_enqueue_for_time!) { |*| raise "boom" }
    stubbing(Sidekiq::Cron::Job, :all, [failing]) { poller.enqueue(T) }
    replacing(Sidekiq::Cron::Job, :all, ->(*) { raise "redis down" }) { poller.enqueue(T) }

    assert_equal [["boom", {context: "cron job report"}], ["redis down", {context: "cron jobs"}]], errors
  end

  def test_the_poller_polls_every_interval
    Robin::Leader.register(config, Struct.new(:leader?).new(true))
    polls = Thread::Queue.new
    poller = Sidekiq::Cron::Poller.new(config, interval: 0.02)
    poller.define_singleton_method(:enqueue) { |*| polls << :poll }
    poller.start
    3.times { Timeout.timeout(5) { polls.pop } }
    poller.terminate
  ensure
    Robin::Leader.unregister(config)
  end

  def test_terminate_before_start
    assert_nil Sidekiq::Cron::Poller.new(config, interval: 1).terminate
  end

  # -- the schedule file -----------------------------------------------------------

  def test_the_schedule_file_is_a_hash_or_a_list_after_erb
    yaml = "base: &base\n  class: #{Report.name}\n  queue: :sym\nreport:\n  <<: *base\n  cron: \"<%= '* * * * *' %>\"\n  args: [a]\n"
    in_app("config/schedule.yml" => yaml) do
      loader = Sidekiq::Cron::ScheduleLoader.new

      assert_predicate loader, :has_schedule_file?
      assert_equal({"base" => ["'cron' must be set"]}, loader.load_schedule)
      assert_equal [["report", "schedule", ["a"], "sym"]], Sidekiq::Cron::Job.all.map { |j| [j.name, j.source, j.args, Sidekiq.load_json(j.message).fetch("queue")] }
    end
    in_app("config/schedule.yaml" => "- name: listed\n  cron: \"@daily\"\n  class: #{Report.name}\n") do
      loader = Sidekiq::Cron::ScheduleLoader.new

      assert_equal %w[erb yaml], recording_requires(loader) { assert_equal({}, loader.load_schedule) }
      assert_equal [%w[listed schedule]], Sidekiq::Cron::Job.all("*").select { |j| j.name == "listed" }.map { |j| [j.name, j.source] }
    end
  end

  def test_a_schedule_file_of_another_shape_is_refused
    in_app("config/schedule.yml" => "just text\n") do
      error = assert_raises(RuntimeError) { Sidekiq::Cron::ScheduleLoader.new.load_schedule }

      assert_equal "Not supported schedule format. Confirm your config/schedule.yml", error.message
    end
    in_app({}) { refute_predicate Sidekiq::Cron::ScheduleLoader.new, :has_schedule_file? }
  end

  # -- servers -------------------------------------------------------------------

  def test_boot_loads_the_schedule_and_polls_until_quiet
    Sidekiq::Cron.configuration.cron_poll_interval = "5"
    in_app("config/schedule.yml" => "good:\n  cron: \"* * * * *\"\n  class: #{Report.name}\nbad:\n  name: x\n") do
      Sidekiq::Cron.boot(config)
    end

    assert_equal ["good"], Sidekiq::Cron::Job.all.map(&:name)
    assert_includes @log.string, "Cron Jobs - bad in the schedule file is invalid: 'cron' must be set, 'klass' (or class) must be set"
    poller = Sidekiq::Cron.instance_variable_get(:@poller)

    assert_instance_of Sidekiq::Cron::Poller, poller
    assert_equal [config, 5], [poller.instance_variable_get(:@config), poller.instance_variable_get(:@interval)]
    thread = poller.instance_variable_get(:@thread)

    assert_predicate thread, :alive?
    config.fire_event(:quiet)

    assert_nil Sidekiq::Cron.instance_variable_get(:@poller)
    refute_predicate thread, :alive?
    in_app({}) { Sidekiq::Cron.boot(config) }
    config.fire_event(:shutdown)

    assert_nil Sidekiq::Cron.instance_variable_get(:@poller)
  end

  def test_boot_migrates_old_jobs_and_skips_polling_when_told
    redis("SADD", key("cron_jobs"), "cron_job:old")
    redis("HSET", key("cron_job:old"), "name", "old", "cron", "* * * * *", "klass", Report.name)
    Sidekiq::Cron.configuration.cron_poll_interval = 0
    in_app({}) { Sidekiq::Cron.boot(config) }

    assert_equal ["old"], Sidekiq::Cron::Job.all.map(&:name)
    assert_nil Sidekiq::Cron.instance_variable_get(:@poller)
    assert_empty config.lifecycle.fetch(:quiet)
  end

  def test_a_disabled_engine_does_nothing
    Sidekiq::Cron.configuration.enabled = false
    redis("SADD", key("cron_jobs"), "cron_job:old")
    in_app("config/schedule.yml" => "x:\n  cron: \"* * * * *\"\n  class: X\n") { Sidekiq::Cron.boot(config) }

    assert_empty Sidekiq::Cron::Job.all
    assert_nil Sidekiq::Cron.instance_variable_get(:@poller)
  end

  def test_servers_boot_it_when_its_used_or_found
    booted = []

    assert_includes config.lifecycle.fetch(:startup), Sidekiq::CRON_START
    Sidekiq.defaults!(config)

    assert_equal 1, config.lifecycle.fetch(:startup).count(Sidekiq::CRON_START)
    replacing(Sidekiq::Cron, :boot, ->(robin) { booted << robin }) do
      stubbing(Sidekiq, :cron?, false) { Sidekiq::CRON_START.call }

      assert_empty booted
      stubbing(Sidekiq, :cron?, true) { Sidekiq::CRON_START.call }
    end

    assert_equal [Robin.config], booted
    assert_true Sidekiq.cron?
  end

  def test_cron_is_found_by_its_schedule_file_or_its_jobs
    in_app({}) { assert_false Sidekiq.cron_found? }
    in_app("config/schedule.yml" => "") { assert_true Sidekiq.cron_found? }
    in_app("config/schedule.yaml" => "") { assert_true Sidekiq.cron_found? }
    redis("SADD", key("cron_jobs"), "x")

    in_app({}) { assert_true Sidekiq.cron_found? }
    redis("DEL", key("cron_jobs"))
    redis("SADD", key("cron_jobs:default"), "x")

    in_app({}) { assert_true Sidekiq.cron_found? }
    redis("DEL", key("cron_jobs:default"))
    redis("SADD", "cron_jobs:default", "x") # another prefix's

    in_app({}) { assert_false Sidekiq.cron_found? }
  ensure
    redis("DEL", "cron_jobs:default")
  end

  def test_its_files_load_it
    assert_true load(File.expand_path("../lib/sidekiq-cron.rb", __dir__))
    assert_true load(File.expand_path("../lib/sidekiq/cron/web.rb", __dir__))
    assert_includes @log.string, "Robin's dashboard doesn't show sidekiq-cron jobs; Sidekiq::Cron::Job.all lists them"
  end
end
