# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq/deploy"

class SidekiqDeployTest < RobinCompatTest
  cover "Sidekiq::Deploy*"

  T0 = Time.utc(2027, 1, 15, 10, 30, 45.25r)

  # A pool that counts the connections it hands out.
  class CountingPool
    attr_reader :uses

    def initialize(pool)
      @pool = pool
      @uses = 0
    end

    def with
      @uses += 1
      @pool.with { |conn| yield conn }
    end
  end

  def marks = Robin::Deploy.list(from: Time.at(0), to: T0 + 86_400, config: config)

  def with_git_label(label)
    maker = Sidekiq::Deploy::LABEL_MAKER
    maker.define_singleton_method(:call) { label }
    yield
  ensure
    maker.singleton_class.remove_method(:call)
  end

  def test_mark_records_a_robin_deploy_mark
    assert_true Sidekiq::Deploy.new.mark!(at: T0, label: "v42")
    assert_equal [{"at" => 1_800_009_045_250, "label" => "v42"}], marks
  end

  def test_a_label_is_marked_once_an_hour
    deploy = Sidekiq::Deploy.new

    assert deploy.mark!(at: T0, label: "v42")
    assert_false deploy.mark!(at: T0 + 60, label: "v42")
    assert_equal ["v42"], marks.map { |mark| mark["label"] }
  end

  def test_mark_without_a_label_uses_the_latest_git_commit
    with_git_label("abc1234 Fix the thing") do
      Sidekiq::Deploy.new.mark!(at: T0)
      Sidekiq::Deploy.mark!
    end

    assert_equal ["abc1234 Fix the thing"], marks.map { |mark| mark["label"] }
  end

  def test_the_class_method_marks_now_with_the_label
    before = Robin.now_ms

    assert Sidekiq::Deploy.mark!("v42")
    (mark,) = marks

    assert_equal "v42", mark["label"]
    assert_includes before..Robin.now_ms, mark["at"]
  end

  def test_the_label_maker_names_the_latest_commit
    label, commands = recording_backticks { Sidekiq::Deploy::LABEL_MAKER.call }

    assert_equal "abc1234 Fix", label
    assert_equal ["git log -1 --format=\"%h %s\""], commands
  end

  def test_fetch_lists_a_days_marks_to_the_minute
    deploy = Sidekiq::Deploy.new
    deploy.mark!(at: Time.utc(2027, 1, 14, 23, 59, 59.999r), label: "yesterday")
    deploy.mark!(at: Time.utc(2027, 1, 15), label: "midnight")
    deploy.mark!(at: T0, label: "first")
    deploy.mark!(at: T0 + 10, label: "same minute")
    deploy.mark!(at: Time.utc(2027, 1, 15, 23, 59, 59.999r), label: "last")
    deploy.mark!(at: Time.utc(2027, 1, 16), label: "tomorrow")

    assert_equal({"2027-01-15T00:00:00Z" => "midnight", "2027-01-15T10:30:00Z" => "first", "2027-01-15T23:59:00Z" => "last"},
      deploy.fetch(Date.new(2027, 1, 15)))
  end

  def test_fetch_gives_an_unlabelled_mark_an_empty_label
    Robin::Deploy.mark!(at: T0, config: config)

    assert_equal({"2027-01-15T10:30:00Z" => ""}, Sidekiq::Deploy.new.fetch(Date.new(2027, 1, 15)))
  end

  def test_fetch_defaults_to_today_in_utc
    Sidekiq::Deploy.mark!("now")

    assert_equal ["now"], Sidekiq::Deploy.new.fetch.values
  end

  def test_marks_go_through_the_pool_given
    pool = CountingPool.new(Sidekiq.redis_pool)
    deploy = Sidekiq::Deploy.new(pool)
    deploy.mark!(at: T0, label: "v42")

    assert_equal({"2027-01-15T10:30:00Z" => "v42"}, deploy.fetch(Date.new(2027, 1, 15)))
    assert_equal 2, pool.uses
    assert_equal ["v42"], marks.map { |mark| mark["label"] }
  end

  def test_the_mark_ttl_is_sidekiqs
    assert_equal 7_776_000, Sidekiq::Deploy::MARK_TTL
  end

  private

  def recording_backticks
    commands = []
    maker = Sidekiq::Deploy::LABEL_MAKER
    receiver = maker.binding.receiver
    receiver.define_singleton_method(:`) { |command| (commands << command) && " abc1234 Fix \n" }
    [yield, commands]
  ensure
    receiver.singleton_class.remove_method(:`)
  end
end
