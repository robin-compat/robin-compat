# frozen_string_literal: true

require_relative "test_helper"

# How many objects the paths every job takes through Sidekiq's interface
# may allocate, once warm (each runs first to fill caches): a change that
# allocates more fails here. The budgets are the most CRuby 3.4 and 4.0
# took, with Robin's C extension and without (ROBIN_NO_EXT=1).
# Re-measure with ROBIN_MEASURE_ALLOCATIONS=1, which prints each count
# instead.
class AllocationsTest < RobinCompatTest
  NATIVE = !Robin::Redis::PARSER.equal?(Robin::Redis::RESP)

  class PlainJob
    include Sidekiq::Job

    def perform(*) = nil
  end

  # A manager that keeps nothing.
  class QuietManager
    def work_started(*) = nil

    def work_finished(*) = nil
  end

  def assert_budget(native:, ruby:, &block)
    3.times(&block)
    # Robin reads random bytes for a batch of job ids at a time, per
    # thread: start the batch over, so the jobs measured never refill it
    # (when they would depends on how many ids earlier tests took).
    Thread.current[:robin_jids]&.offset = 0
    limit = NATIVE ? native : ruby
    return assert_allocations(count: 0..limit, &block) unless ENV["ROBIN_MEASURE_ALLOCATIONS"]

    counted = Minitest::Memory::AllocationCounter.count(&block).total.count
    puts "#{name} (#{NATIVE ? "native" : "ruby"}): #{counted} of #{limit}"
  end

  def test_perform_async
    assert_budget(native: 42, ruby: 62) { PlainJob.perform_async(1) }
  end

  def test_client_push
    assert_budget(native: 33, ruby: 53) { Sidekiq::Client.push("class" => PlainJob, "args" => [1]) }
  end

  def test_running_a_sidekiq_job
    fetcher = Robin::Fetcher.new(config.default_capsule, "me")
    processor = Robin::Processor.new(QuietManager.new, config.default_capsule, fetcher, 0)
    units = Array.new(4) do
      raw = JSON.generate({"class" => PlainJob.name, "args" => [1], "jid" => Robin.generate_jid, "queue" => "default"})
      key = "#{fetcher.inflight_key}:#{Robin::Fetcher.mint}"
      Robin.redis do |c|
        c.pipelined do |p|
          p.call("SADD", fetcher.inflight_key, key)
          p.call("RPUSH", key, raw)
        end
      end
      Robin::Fetcher::UnitOfWork.new("default", raw, fetcher, key)
    end
    assert_budget(native: 18, ruby: 18) { processor.send(:process, units.shift) }
  end
end
