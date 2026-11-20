# frozen_string_literal: true

require_relative "test_helper"

class ClientTestJob
  include Sidekiq::Job

  def perform(*) = nil
end

class ClientTest < RobinCompatTest
  cover "Sidekiq::Client*"

  def queued(queue = "default") = redis("LRANGE", key("queue:#{queue}"), 0, -1).map { |raw| JSON.parse(raw) }

  def scheduled = redis("ZRANGE", key("schedule"), 0, -1, "WITHSCORES").each_slice(2).map { |raw, s| [JSON.parse(raw), Float(s)] }

  def other_config
    other = Robin::Config.new
    other.redis = {url: REDIS_URL}
    other.key_prefix = "#{key_prefix_for_test}other:"
    other
  end

  def test_normalize_item_fills_in_a_job_without_pushing_it
    item = Sidekiq::Client.new.normalize_item({class: ClientTestJob, args: [1], queue: "low"})

    assert_equal({"class" => "ClientTestJob", "args" => [1], "queue" => "low", "retry" => true}, item.slice("class", "args", "queue", "retry"))
    assert_match(/\A\h{24}\z/, item.fetch("jid"))
    assert_in_delta Robin.now_ms, item.fetch("created_at"), 5_000
    assert_empty queued("low")
    assert_raises(ArgumentError) { Sidekiq::Client.new.normalize_item({"class" => ClientTestJob}) }
  end

  def test_normalize_item_follows_the_clients_config
    strict = other_config
    strict.strict_args = :raise
    config.strict_args = false
    item = {"class" => ClientTestJob, "args" => [:sym]}

    assert_equal [:sym], Sidekiq::Client.new.normalize_item(item).fetch("args")
    assert_raises(ArgumentError) { Sidekiq::Client.new(config: strict).normalize_item(item) }
  ensure
    strict&.redis_pool&.shutdown
  end

  def test_push_and_push_bulk
    jid = assert_roundtrips(1) { Sidekiq::Client.push("class" => ClientTestJob, "args" => [1]) }

    assert_equal [[jid, "ClientTestJob", [1]]], queued.map { |j| j.values_at("jid", "class", "args") }
    jids = assert_roundtrips(1) { Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[2], [3]], "queue" => "q") }

    assert_equal jids, queued("q").reverse.map { |j| j["jid"] }
  end

  # Sidekiq 7.0 leaves out the jobs middleware stopped; from 7.1 they're
  # nil.
  def test_push_bulk_returns_only_pushed_jids_for_sidekiq_7_0
    config.client_middleware { |chain| chain.add(Class.new { def call(_, job, *) = (yield unless job["args"] == [2]) }) }
    full = Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[1], [2], [3]])
    Sidekiq.compatible_with = "7.1"

    assert_equal 3, Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[1], [2], [3]]).size
    Sidekiq.compatible_with = "7.0"
    pushed = Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[1], [2], [3]])

    assert_nil full[1]
    assert_equal [full[0].class, String], [String, pushed.last.class]
    assert_equal 2, pushed.size
  end

  def test_push_bulk_slices_ats_and_jids
    jids = Array.new(3) { |i| "jid#{i}" }
    later = (Time.now.to_f + 600).floor
    ats = [later, later + 1, later + 2]
    pushed = Sidekiq::Client.new.push_bulk("class" => "ClientTestJob", "args" => [[1], [2], [3]], "at" => ats,
      "jids" => jids, "batch_size" => 2)

    assert_equal jids, pushed
    assert_equal jids, scheduled.map { |job, _| job["jid"] }
    scheduled.zip(ats).each { |(_, score), at| assert_in_delta at, score, 0.001 }
    refute(scheduled.any? { |job, _| job.key?("batch_size") || job.key?("jids") })
    Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[4]], :batch_size => 1)

    assert_equal 1, queued.size
    redis("DEL", key("schedule"))
    Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[5], [6]], "at" => later, "batch_size" => 1)
    # Any Array gives one time per job.
    Sidekiq::Client.push_bulk("class" => "ClientTestJob", "args" => [[7], [8]], "at" => Class.new(Array).new(ats.first(2)), "batch_size" => 1)

    assert_equal [[[5], later], [[6], later], [[7], later], [[8], later + 1]],
      scheduled.map { |job, score| [job["args"], score] }.sort_by(&:first)
  end

  def test_enqueue_helpers
    Sidekiq::Client.enqueue(ClientTestJob, 1)
    Sidekiq::Client.enqueue_to("low", ClientTestJob, 2)
    Sidekiq::Client.enqueue_in(60, ClientTestJob, 3)
    Sidekiq::Client.enqueue_to_in("low", 120, ClientTestJob, 4)

    assert_equal [[1]], queued.map { |j| j["args"] }
    assert_equal [[2]], queued("low").map { |j| j["args"] }
    assert_equal [[[3], "default"], [[4], "low"]], scheduled.map { |job, _| job.values_at("args", "queue") }
  end

  def test_pools
    other = other_config
    Sidekiq::Client.new(pool: Sidekiq::RedisConnection::Pool.new(other.redis_pool)).push("class" => "ClientTestJob", "args" => [])
    Sidekiq::Client.new(other.redis_pool).push("class" => "ClientTestJob", "args" => [])
    Sidekiq::Client.via(other.redis_pool) { ClientTestJob.perform_async }
    Sidekiq::Client.via(Sidekiq::RedisConnection::Pool.new(other.redis_pool)) { ClientTestJob.perform_async }

    assert_equal 4, redis("LLEN", key("queue:default")), "the job keys don't depend on the pool's config"
    assert_same other.redis_pool, Sidekiq::Client.robin_pool(Sidekiq::RedisConnection::Pool.new(other.redis_pool))
    assert_same other.redis_pool, Sidekiq::Client.robin_pool(other.redis_pool)
    client = Sidekiq::Client.new(pool: other.redis_pool)

    assert_same other.redis_pool, client.redis_pool
  ensure
    other&.redis_pool&.shutdown
  end

  def test_pushes_go_to_the_clients_pool
    other = other_config
    pools = record_push_pools
    wrapped = Sidekiq::RedisConnection::Pool.new(other.redis_pool)
    Sidekiq::Client.new(other.redis_pool).push("class" => "ClientTestJob", "args" => [])
    Sidekiq::Client.new(pool: wrapped).push("class" => "ClientTestJob", "args" => [])
    Sidekiq::Client.new(wrapped).push_bulk("class" => "ClientTestJob", "args" => [[1]])
    Sidekiq::Client.via(wrapped) { ClientTestJob.perform_async }
    Sidekiq::Client.new.push("class" => "ClientTestJob", "args" => [])

    assert_equal [other.redis_pool] * 4 + [config.redis_pool], pools
    assert_same wrapped, Sidekiq::Client.new(wrapped).redis_pool
    assert_nil Sidekiq::Client.new.redis_pool
  ensure
    other&.redis_pool&.shutdown
  end

  def test_a_sidekiq_config_is_unwrapped
    other = other_config
    client = Sidekiq::Client.new(config: Sidekiq::Config.new(other))

    assert_instance_of Robin::Middleware::Chain, client.middleware.__getobj__
    assert_same other.client_middleware.entries.size, client.middleware.entries.size
    chain = Robin::Middleware::Chain.new(config)

    assert_instance_of Robin::Middleware::Chain, Sidekiq::Client.new(chain: Sidekiq::Middleware::Chain.new(chain)).middleware.__getobj__
    assert_same chain, Sidekiq::Client.new(chain: chain).middleware.__getobj__
    assert_same chain, Sidekiq::Client.new(chain: Sidekiq::Middleware::Chain.new(chain)).middleware.__getobj__
  ensure
    other&.redis_pool&.shutdown
  end

  def test_push_bulk_sends_a_thousand_jobs_per_round_trip
    sizes = []
    client = Sidekiq::Client.new
    robin = client.send(:robin)
    robin.define_singleton_method(:push_bulk) { |items| (sizes << items.fetch("args").size) && super(items) }
    client.define_singleton_method(:robin) { robin }
    assert_roundtrips(3) { client.push_bulk(class: "ClientTestJob", args: Array.new(2_001) { [it] }) }

    assert_equal [1_000, 1_000, 1], sizes
  end

  def test_config_and_chains
    stop = Class.new { def call(*) = nil }
    other = other_config
    Sidekiq::Client.new(config: other).push("class" => "ClientTestJob", "args" => [])
    Sidekiq::Client.new(config: Sidekiq::Config.new(other)).push("class" => "ClientTestJob", "args" => [])

    assert_equal 2, redis("LLEN", "#{key_prefix_for_test}other:queue:default")
    chain = Robin::Middleware::Chain.new(config).add(stop)

    assert_nil Sidekiq::Client.new(chain: chain).push("class" => "ClientTestJob", "args" => [])
    assert_nil Sidekiq::Client.new(chain: Sidekiq::Middleware::Chain.new(chain)).push("class" => "ClientTestJob", "args" => [])
    client = Sidekiq::Client.new
    seen = client.middleware { |c| c.add stop }

    assert_same seen, client.middleware
    assert_nil client.push("class" => "ClientTestJob", "args" => [])
    assert_empty config.client_middleware.entries.select { |e| e.klass == stop }, "the configured chain is untouched"
    assert Sidekiq::Client.new.push("class" => "ClientTestJob", "args" => [])
    assert_equal 1, queued.size
  ensure
    other&.redis_pool&.shutdown
  end

  def test_cancel
    Sidekiq::Client.new.cancel!("jid1")

    assert Robin::Cancellation.cancelled?("jid1", config: config)
  end
end
