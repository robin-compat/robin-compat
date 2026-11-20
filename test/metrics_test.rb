# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq-pro"
require "sidekiq/middleware/server/statsd"

class SidekiqMetricsTest < RobinCompatTest
  cover "Sidekiq::Pro::Metrics*"
  cover "Sidekiq::Middleware::Server::Statsd*"

  # Records calls as dogstatsd-ruby's Datadog::Statsd takes them.
  class FakeStatsd
    attr_reader :calls

    def initialize = @calls = []

    def increment(stat, opts = {}) = @calls << [:increment, stat, opts]

    def gauge(stat, value, opts = {}) = @calls << [:gauge, stat, value, opts]

    def timing(stat, ms, opts = {}) = @calls << [:timing, stat, ms, opts]

    def distribution(stat, value, opts = {}) = @calls << [:distribution, stat, value, opts]
  end

  class Broken
    def initialize(error) = @error = error

    def increment(*) = raise(@error)

    def gauge(*) = raise(@error)
  end

  def setup
    super
    @statsd = FakeStatsd.new
    @made = 0
    Sidekiq::Config.new(config).dogstatsd = -> { (@made += 1) && @statsd }
  end

  def teardown
    Sidekiq::Middleware::Server::Statsd.options = nil
    Sidekiq::Pro::Metrics.install(Robin::Config.new, nil)
    super
  end

  def events = config.statsd

  def run_job(payload = {"class" => "HardJob", "jid" => "j"}, queue = "default", &block)
    Sidekiq::Middleware::Server::Statsd.new.call(Object.new, payload, queue, &block || -> { :done })
  end

  def test_client_is_made_once_on_first_use
    assert_equal 0, @made
    assert_same @statsd, Sidekiq::Pro::Metrics.client
    assert_same @statsd, Sidekiq::Pro::Metrics.client
    assert_equal 1, @made
  end

  def test_install_replaces_the_client
    other = FakeStatsd.new
    Sidekiq::Pro::Metrics.client
    Sidekiq::Pro::Metrics.install(config, -> { other })

    assert_same other, Sidekiq::Pro::Metrics.client
  end

  def test_events_forward_robins_metrics_under_sidekiq_pros_names
    assert_predicate events, :dogstatsd?
    assert_nil events.increment("batch.created")
    events.increment("jobs.recovered", 3)
    events.increment("batch.success", tags: {tag: %w[a b]})
    events.gauge("queue.size", 4, tags: {queue: "q"})
    events.timing("batch.duration", 12.5)
    events.distribution("batch.duration_dist", 12.5, tags: {tag: []})
    events.distribution("jobs.perform_dist", 1, tags: {class: "X"})
    events.distribution("custom", 2)

    assert_equal [
      [:increment, "sidekiq.batch.created", {by: 1, tags: []}],
      [:increment, "sidekiq.jobs.recovered.fetch", {by: 3, tags: []}],
      [:increment, "sidekiq.batch.success", {by: 1, tags: ["tag:a", "tag:b"]}],
      [:gauge, "sidekiq.queue.size", 4, {tags: ["queue:q"]}],
      [:timing, "sidekiq.batches.duration", 12.5, {tags: []}],
      [:distribution, "sidekiq.batches.duration_dist", 12.5, {tags: []}],
      [:distribution, "sidekiq.custom", 2, {tags: []}]
    ], @statsd.calls
    assert_nil events.job_metrics({"X" => {}})
    assert_nil events.queue_metrics({"q" => {}})
    assert_nil events.close
  end

  def test_robin_batches_report_through_the_events
    batch = Robin::Batch.new
    batch.tags = ["imports"]
    batch.jobs { nil }

    assert_equal ["sidekiq.batch.created", "sidekiq.batch.complete", "sidekiq.batch.success",
      "sidekiq.batches.duration", "sidekiq.batches.duration_dist"], @statsd.calls.map { |c| c[1] }
    assert_equal({by: 1, tags: ["tag:imports"]}, @statsd.calls[1].last)
  end

  def test_datadog_extensions_can_be_turned_off
    assert_true events.distributions?
    Sidekiq.default_configuration[:use_datadog_extensions] = false

    refute_predicate events, :distributions?
    run_job

    assert_equal %w[sidekiq.jobs.count sidekiq.jobs.success sidekiq.jobs.perform], @statsd.calls.map { |c| c[1] }
    Sidekiq.default_configuration[:use_datadog_extensions] = true

    assert_predicate events, :distributions?
  end

  def test_without_a_client_nothing_is_sent
    Sidekiq::Pro::Metrics.install(config, nil)

    assert_nil Sidekiq::Pro::Metrics.client
    assert_nil events.increment("batch.created")
    assert_equal :done, run_job
  end

  def test_client_errors_never_raise
    [IOError.new("closed"), Errno::ECONNREFUSED.new, SocketError.new("dns")].each do |error|
      Sidekiq::Pro::Metrics.install(config, -> { Broken.new(error) })

      assert_nil events.increment("batch.created")
      assert_nil events.gauge("queue.size", 1)
      assert_equal :done, run_job
    end
  end

  def test_other_client_errors_raise
    Sidekiq::Pro::Metrics.install(config, -> { Broken.new(ArgumentError.new("bug")) })

    assert_raises(ArgumentError) { events.increment("batch.created") }
  end

  def test_middleware_counts_and_times_successes
    before = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    assert_equal :done, run_job({"class" => "HardJob", "jid" => "j"}, "critical") { :done }
    elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - before) * 1000
    tags = {tags: ["worker:HardJob", "queue:critical"]}
    count, success, timing, dist = @statsd.calls

    assert_equal [[:increment, "sidekiq.jobs.count", tags], [:increment, "sidekiq.jobs.success", tags]], [count, success]
    assert_equal [:timing, "sidekiq.jobs.perform", tags], timing.values_at(0, 1, 3)
    assert_equal [:distribution, "sidekiq.jobs.perform_dist", tags], dist.values_at(0, 1, 3)
    assert_operator timing[2], :>=, 0
    assert_operator timing[2], :<=, elapsed
    assert_in_delta timing[2], dist[2], 0.001
    assert_equal 4, @statsd.calls.size
  end

  # Its clock reads 10.0 and then 10.25 seconds.
  class TimedStatsd < Sidekiq::Middleware::Server::Statsd
    def initialize = @times = [10.0, 10.25]

    def monotonic = @times.shift
  end

  def test_middleware_times_in_milliseconds
    TimedStatsd.new.call(Object.new, {"class" => "HardJob", "jid" => "j"}, "default") { nil }

    assert_equal [250.0, 250.0], @statsd.calls.drop(2).map { |c| c[2] }
    assert_in_delta ::Process.clock_gettime(::Process::CLOCK_MONOTONIC), Sidekiq::Middleware::Server::Statsd.new.send(:monotonic), 1
  end

  def test_middleware_counts_failures
    error = assert_raises(ArgumentError) { run_job { raise ArgumentError, "bad" } }
    assert_equal "bad", error.message
    assert_equal %w[sidekiq.jobs.count sidekiq.jobs.failure sidekiq.jobs.perform sidekiq.jobs.perform_dist],
      @statsd.calls.map { |c| c[1] }
  end

  def test_middleware_tags_the_wrapped_class
    run_job("class" => "Sidekiq::ActiveJob::Wrapper", "wrapped" => "MailJob", "jid" => "j")

    assert_equal({tags: ["worker:MailJob", "queue:default"]}, @statsd.calls.first.last)
  end

  def test_middleware_options
    seen = []
    Sidekiq::Middleware::Server::Statsd.options = lambda do |klass, job, queue|
      seen << [klass, job.fetch("jid"), queue]
      {tags: ["w:#{klass}"], sample_rate: job["dd_rate"]} if job["dd_rate"]
    end
    run_job("class" => "HardJob", "jid" => "j", "dd_rate" => 0.1)
    run_job("class" => "HardJob", "jid" => "k")

    assert_equal [["HardJob", "j", "default"], ["HardJob", "k", "default"]], seen
    assert_equal({tags: ["w:HardJob"], sample_rate: 0.1}, @statsd.calls.first.last)
    assert_equal({tags: ["worker:HardJob", "queue:default"]}, @statsd.calls.last.last)
  end

  def test_failing_options_are_reported_not_raised
    reported = []
    config.error_handlers << ->(e, context, _config) { reported << [e.message, context] }
    Sidekiq::Middleware::Server::Statsd.options = ->(*) { raise "bad options" }
    payload = {"class" => "HardJob", "jid" => "j"}

    assert_equal :done, run_job(payload)
    assert_equal [["bad options", {context: "statsd options", job: payload}]], reported
    assert_empty @statsd.calls
  end

  def test_middleware_is_stateless_and_chainable
    assert_operator Sidekiq::Middleware::Server::Statsd, :<, Robin::Middleware::Stateless
    Sidekiq::Config.new(config).server_middleware { |chain| chain.add Sidekiq::Middleware::Server::Statsd }
    job = {"class" => "HardJob", "jid" => "j", "queue" => "default"}

    assert_equal :ran, config.server_middleware.invoke(Object.new, job, "default") { :ran }
    assert_equal "sidekiq.jobs.count", @statsd.calls.first[1]
  end
end
