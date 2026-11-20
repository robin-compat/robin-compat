# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-ent"
require "tzinfo"

module PeriodicTestJobs
  class Hourly
    include Sidekiq::Job

    def perform(*) = nil
  end
end

class SidekiqPeriodicTest < RobinCompatTest
  include PeriodicTestJobs

  cover "Sidekiq::Periodic*"
  cover "Sidekiq::CronParser*"

  # Stands in for an ActiveSupport::TimeZone.
  ASZone = Struct.new(:tzinfo)

  def registry = @registry ||= Robin::Periodic::Registry.new

  def manager = @manager ||= Sidekiq::Periodic::Manager.new(registry)

  def entries = registry.entries.values

  def test_register_takes_sidekiqs_options
    manager.register("0 * * * *", Hourly)
    manager.register("* * * * *", Hourly.name, retry: 2, queue: :foo)
    manager.register("1 * * * *", "Hourly", "args" => ["one"], "retry" => false, "description" => "first minute")

    assert_equal [
      {"cron" => "0 * * * *", "class" => Hourly.name, "queue" => nil, "args" => [], "tz" => nil,
       "description" => nil, "options" => {}},
      {"cron" => "* * * * *", "class" => Hourly.name, "queue" => "foo", "args" => [], "tz" => nil,
       "description" => nil, "options" => {"retry" => 2}},
      {"cron" => "1 * * * *", "class" => "Hourly", "queue" => nil, "args" => ["one"], "tz" => nil,
       "description" => "first minute", "options" => {"retry" => false}}
    ], entries
  end

  def test_register_returns_the_id
    id = manager.register("0 * * * *", Hourly.name)

    assert_equal [id], registry.entries.keys
  end

  def test_time_zones
    manager.register("0 4 * * *", "Local")
    manager.register("0 4 * * *", "Tokyo", tz: ASZone.new(TZInfo::Timezone.get("Asia/Tokyo")))
    manager.tz = TZInfo::Timezone.get("Asia/Kathmandu")

    assert_equal "Asia/Kathmandu", manager.tz.identifier
    manager.register("0 4 * * *", "Kathmandu")
    manager.register("0 5 * * *", "Berlin", "tz" => "Europe/Berlin")
    manager.tz = "UTC"
    manager.register("0 6 * * *", "Utc")

    assert_equal [nil, "Asia/Tokyo", "Asia/Kathmandu", "Europe/Berlin", "UTC"], entries.map { |e| e.fetch("tz") }
    assert_equal [{}], entries.map { |e| e.fetch("options") }.uniq
  end

  def test_zone_name
    assert_equal "Asia/Tokyo", Sidekiq::Periodic.zone_name(ASZone.new(TZInfo::Timezone.get("Asia/Tokyo")))
    assert_equal "Europe/Paris", Sidekiq::Periodic.zone_name(TZInfo::Timezone.get("Europe/Paris"))
    assert_equal "UTC", Sidekiq::Periodic.zone_name(:UTC)
    assert_nil Sidekiq::Periodic.zone_name(nil)
  end

  def test_config_periodic_yields_a_manager
    Sidekiq::Config.new(config).periodic { |mgr| mgr.register("0 * * * *", Hourly.name, "retry" => 1) }
    config.periodic_blocks.each { |block| block.call(registry) }

    assert_equal [{"retry" => 1}], entries.map { |e| e.fetch("options") }
  end

  def test_loop_set
    manager.register("0 * * * *", Hourly.name, retry: 2, queue: "low", args: [1])
    manager.register("0 0 30 2 *", Hourly.name, tz: "Asia/Tokyo") # never runs
    redis("HSET", config.key("periodic"), *registry.entries.flat_map { |id, e| [id, JSON.generate(e)] })
    hourly, never = registry.entries.keys
    jid = Robin::Periodic.enqueue_now(hourly)
    loops = assert_roundtrips(2) { Sidekiq::Periodic::LoopSet.new.to_h { |l| [l.lid, l] } }
    lop = loops.fetch(hourly)

    assert_equal ["0 * * * *", Hourly.name, nil], [lop.schedule, lop.klass, lop.tz]
    assert_equal({"retry" => 2, "queue" => "low", "args" => [1]}, lop.options)
    assert_instance_of Time, lop.next_run
    assert_equal 0, lop.next_run.min
    assert_in_delta Time.now.to_f + 1_800, lop.next_run.to_f, 1_801
    assert_equal [jid], lop.history.map(&:first)
    assert_instance_of Time, lop.history.first.last
    assert_in_delta Time.now.to_f, lop.history.first.last.to_f, 2
    assert_nil loops.fetch(never).next_run
    assert_equal "Asia/Tokyo", loops.fetch(never).tz
    assert_equal({"queue" => "default", "args" => []}, loops.fetch(never).options)
    assert_empty loops.fetch(never).history
    assert_equal 2, Sidekiq::Periodic::LoopSet.new.size
    assert_instance_of Enumerator, Sidekiq::Periodic::LoopSet.new.each
  end

  def test_a_loops_history_shows_its_latest_25_runs
    manager.register("0 * * * *", Hourly.name)
    redis("HSET", config.key("periodic"), *registry.entries.flat_map { |id, e| [id, JSON.generate(e)] })
    id, = registry.entries.keys
    runs = Array.new(26) { |i| JSON.generate({"at" => i * 1000, "jid" => "j#{i}"}) }
    redis("LPUSH", config.key("periodic", id, "h"), *runs)

    assert_equal Array.new(25) { |i| "j#{25 - i}" }, Sidekiq::Periodic::LoopSet.new.first.history.map(&:first)
  end

  def test_time
    assert_equal Time.at(1, 500, :millisecond), Sidekiq::Periodic.time(1_500)
  end

  def test_config_tester
    assert_same(true, Sidekiq::Periodic::ConfigTester.new.verify { |mgr| mgr.register("* * * * *", Hourly.name, retry: 1) })
    error = assert_raises(Robin::Periodic::Invalid) do
      Sidekiq::Periodic::ConfigTester.new.verify do |mgr|
        assert_instance_of Sidekiq::Periodic::Manager, mgr
        mgr.register("*/4 * 10 * *", "Missing", retry: 2)
      end
    end
    assert_includes error.message, "Missing: no such class"
  end

  def test_cron_parser
    after = Time.utc(2026, 1, 1, 10, 7)

    assert_equal Time.utc(2026, 1, 1, 10, 15), Sidekiq::CronParser.new("*/15 * * * *").next(after)
    assert_equal Time.utc(2026, 1, 1, 23), Sidekiq::CronParser.new("0 8 * * *", tz: "Asia/Tokyo").next(after)
    assert_equal Time.utc(2026, 1, 1, 23), Sidekiq::CronParser.new("0 8 * * *", tz: ASZone.new(TZInfo::Timezone.get("Asia/Tokyo"))).next(after)
    assert_operator Sidekiq::CronParser.new("* * * * *").next, :>, Time.now
    assert_operator Sidekiq::CronParser.new("* * * * *").next, :<=, Time.now + 60
  end
end
