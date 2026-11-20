# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq/active_job"
require "sidekiq-cron"
require "active_support/time"
require "global_id"

ActiveJob::Base.logger = Logger.new(nil)
GlobalID.app = "robin-compat-test"

module CronActiveJobTestJobs
  class Mailer < ActiveJob::Base
    queue_as :mail

    def perform(*) = nil
  end

  # Its queue depends on its arguments.
  class Triage < ActiveJob::Base
    queue_as { (arguments.first == "urgent") ? :urgent : :routine }

    def perform(*) = nil
  end

  class Report
    include Sidekiq::Job

    sidekiq_options queue: "low"

    def perform(*) = nil
  end

  # A record GlobalID finds by id.
  class Record
    include GlobalID::Identification

    attr_reader :id

    def initialize(id) = @id = id

    def self.find(id) = new(Integer(id))

    def ==(other) = other.is_a?(Record) && other.id == id
  end
end

# sidekiq-cron's jobs with Active Job, GlobalID and Rails' Time.zone.
class SidekiqCronActiveJobTest < RobinCompatTest
  include CronActiveJobTestJobs

  cover "Sidekiq::Cron*"
  cover "Robin::Compat::CronLine*"

  def setup
    super
    Sidekiq::Cron.reset!
    Mailer.queue_adapter = :sidekiq
  end

  def teardown
    Time.zone = nil
    ActiveJob::Base.queue_name_prefix = nil
    Sidekiq::Cron.reset!
    super
  end

  def job(attrs = {}) = Sidekiq::Cron::Job.new({"name" => "mail", "cron" => "* * * * *", "class" => Mailer.name}.merge(attrs))

  def queued(queue) = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  def test_an_active_job_is_enqueued_with_perform_later
    mail = job("args" => [1, "a"])

    assert_predicate mail, :is_active_job?
    assert_equal({"queue" => "mail", "class" => Mailer.name, "args" => [1, "a"]}, mail.message)
    mail.enqueue!
    pushed = queued("mail").first

    assert_equal [Mailer.name, [1, "a"]], [pushed.fetch("wrapped"), pushed.fetch("args").first.fetch("arguments")]
    assert_equal pushed.fetch("jid"), mail.jid_history_from_redis.first.fetch("jid")
    assert_equal "other", job("queue" => "other").tap(&:enqueue!).then { queued("other").first.fetch("queue") }
  end

  def test_a_job_that_isnt_enqueued_has_no_jid
    Mailer.define_singleton_method(:set) { |*| Struct.new(:x) { def perform_later(*) = false }.new }
    mail = job
    mail.enqueue!

    assert_nil mail.jid_history_from_redis.first.fetch("jid")
  ensure
    Mailer.singleton_class.remove_method(:set)
  end

  def test_active_jobs_queues_take_active_jobs_prefix
    ActiveJob::Base.queue_name_prefix = "app"

    assert_equal ["app_mail", "own-mail"],
      [job.queue_name_with_prefix, job("queue_name_prefix" => "own", "queue_name_delimiter" => "-").queue_name_with_prefix]
    refute_predicate job("class" => "CronActiveJobTestJobs::Record"), :is_active_job?
    assert_equal "low", job("class" => Report.name).queue_name_with_prefix
    assert_true job("class" => "MissingJob").is_active_job?(Mailer)
    assert_false job("class" => "MissingJob").is_active_job?
    assert_predicate job("class" => Record), :klass_valid
  end

  def test_an_active_jobs_queue_can_depend_on_its_arguments
    assert_equal %w[urgent routine], [["urgent"], ["later"]].map { |args| job("class" => Triage.name, "args" => args).message.fetch("queue") }
  end

  def test_symbolize_args_symbolizes_an_active_jobs_hash_arguments
    assert_equal [{to: "x"}], job("symbolize_args" => true, "args" => [{"to" => "x"}]).args
  end

  def test_global_ids_are_stored_and_found
    stored = job("args" => Record.new(7))

    assert_equal [{"_sc_globalid" => "gid://robin-compat-test/CronActiveJobTestJobs::Record/7"}], stored.args
    assert_equal [Record.new(7)], stored.enqueue_args
    assert_equal [[Record.new(1)], {"a" => [Record.new(2)], "b" => 3}],
      [job("args" => [Record.new(1)]).enqueue_args, job("args" => {"a" => [Record.new(2)], "b" => 3}).enqueue_args.first]
    assert_equal [{"_sc_globalid" => "x", "other" => 1}], job("args" => [{"_sc_globalid" => "x", "other" => 1}]).enqueue_args
    assert_equal [{"a" => {"_sc_globalid" => "gid://robin-compat-test/CronActiveJobTestJobs::Record/3"}}], job("args" => {"a" => Record.new(3)}).args
  end

  def test_the_default_zone_is_rails
    Time.zone = "America/Chicago"

    assert_equal "America/Chicago", Sidekiq::Cron.parse("0 22 * * *").instance_variable_get(:@tz)
    assert_equal "UTC", Sidekiq::Cron.parse("0 22 * * * UTC").instance_variable_get(:@tz)
    Time.zone = "Central Time (US & Canada)"

    assert_equal "America/Chicago", Sidekiq::Cron.parse("0 22 * * *").instance_variable_get(:@tz)
    Time.zone = nil

    assert_nil Sidekiq::Cron.parse("0 22 * * *").instance_variable_get(:@tz)
  end
end
