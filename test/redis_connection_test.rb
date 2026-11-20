# frozen_string_literal: true

require_relative "test_helper"

class RedisConnectionTest < RobinCompatTest
  cover "Sidekiq::RedisConnection*"

  def conn(&) = Sidekiq.redis(&)

  # Stands in for a Robin connection (or pool): records what it's sent
  # and answers +reply+.
  class Fake
    attr_reader :sent

    def initialize(reply = nil)
      @reply = reply
      @sent = []
    end

    def call(*args) = (@sent << args) && @reply

    def blocking_call(timeout, *args) = (@sent << [timeout, *args]) && @reply

    def close = :closed

    def shutdown = :shut_down
  end

  def test_commands_take_flags_and_flatten_arguments
    command = Sidekiq::RedisConnection.method(:command)

    assert_equal ["SET", "k", "v", "EX", 60, "NX"], command.call(["SET", "k", "v"], {ex: 60, nx: true, xx: false, px: nil})
    assert_equal ["HSET", "h", "a", 1, "b", 2], command.call(["HSET", "h", {"a" => 1, "b" => [2]}], {})
    # Any Hash (HashWithIndifferentAccess, say) is splatted.
    assert_equal ["HSET", "h", "a", 1], command.call(["HSET", "h", Class.new(Hash).new.merge!("a" => 1)], {})
    assert_equal ["DEL", "a", "b"], command.call(["DEL", %w[a b]], {})
  end

  def test_what_is_sent
    fake = Fake.new(1)
    c = Sidekiq::RedisConnection::Connection.new(fake)
    c.get("k")
    c.exists?("a", "b")
    c.evalsha("sha")
    c.evalsha("sha", ["k"])
    c.blocking_call(2, "BLPOP", "q", 2)
    c.blocking_call(nil, "LPOP", "q")
    hash = Sidekiq::RedisConnection::Connection.new(Fake.new(%w[a 1])).blocking_call(1, "HGETALL", "h")
    collector = Sidekiq::RedisConnection::Collector.new
    collector.set("k", ["v"], nx: true)

    assert_equal [%w[get k], %w[EXISTS a b], ["EVALSHA", "sha", 0], ["EVALSHA", "sha", 1, "k"],
      [2, "BLPOP", "q", 2], [0, "LPOP", "q"]], fake.sent
    assert_equal [%w[set k v NX]], collector.commands
    assert_equal({"a" => "1"}, hash)
    assert_nil collector.call("GET", "k")
    assert_respond_to collector, :anything
    assert_same :closed, c.close
    assert_same :shut_down, Sidekiq::RedisConnection::Pool.new(fake).shutdown { nil }
  end

  def test_info_parses_lines
    c = Sidekiq::RedisConnection::Connection.new(Fake.new("# Server\r\nredis_version:8.0.1\r\nexecutable:C:\\redis\r\n\r\nempty:\r\n"))

    assert_equal({"redis_version" => "8.0.1", "executable" => "C:\\redis", "empty" => ""}, c.info)
  end

  def test_commands_as_methods_and_keyword_flags
    k = key("k")
    conn do |c|
      assert_equal "OK", c.set(k, "v", ex: 60, nx: true)
      assert_nil c.set(k, "w", nx: true), "NX: not set again"
      assert_nil c.call("SET", k, "w", xx: false, nx: true)
      assert_equal "v", c.get(k)
      assert_operator c.ttl(k), :>, 50
      assert_equal "OK", c.call("SET", k, "w", px: 5_000, keepttl: nil)
      assert_true c.exists?(k)
      assert_false c.exists?(key("missing"))
      assert_equal 2, c.call("RPUSH", key("l"), %w[a b])
      assert_equal 1, c.hset(key("h"), {"f" => "1"})
    end
  end

  def test_replies_take_resp3_shapes
    conn do |c|
      c.hset(key("h"), "a", "1", "b", "2")
      c.zadd(key("z"), 1.5, "m", 2, "n")

      assert_equal({"a" => "1", "b" => "2"}, c.hgetall(key("h")))
      assert_equal({"a" => "1", "b" => "2"}, c.call(:hgetall, key("h")))
      assert_equal({}, c.hgetall(key("none")))
      assert_in_delta(1.5, c.zscore(key("z"), "m"))
      assert_nil c.zscore(key("z"), "x")
      assert_equal [1.5, nil], c.zmscore(key("z"), "m", "x")
      assert_in_delta(3.5, c.zincrby(key("z"), 2, "m"))
      assert_equal [["n", 2.0], ["m", 3.5]], c.zrange(key("z"), 0, -1, withscores: true)
      assert_equal [["m", 3.5], ["n", 2.0]], c.call("ZREVRANGE", key("z"), 0, -1, "WITHSCORES")
      assert_equal [["n", 2.0]], c.zrangebyscore(key("z"), 0, 3, "withscores")
      assert_equal [["m", 3.5]], c.zrevrangebyscore(key("z"), 5, 3, withscores: true)
      assert_equal %w[n m], c.zrange(key("z"), 0, -1)
    end
  end

  def test_every_sorted_set_reply_with_scores_takes_pairs
    conn do |c|
      c.zadd(key("z"), 1.5, "m", 2, "n")

      assert_equal [["m", 1.5]], c.call(:zrange, key("z"), 0, 0, :withscores)
      assert_equal [["m", 1.5], ["n", 2.0]], c.call("ZUNION", 1, key("z"), "WITHSCORES")
      assert_equal [["m", 1.5], ["n", 2.0]], c.call("ZINTER", 1, key("z"), "WITHSCORES")
      assert_equal [["m", 1.5], ["n", 2.0]], c.call("ZDIFF", 1, key("z"), "WITHSCORES")
      assert_includes [[["m", 1.5]], [["n", 2.0]]], c.call("ZRANDMEMBER", key("z"), 1, "WITHSCORES")
    end
  end

  def test_pops_floats_and_config_take_resp3_shapes
    conn do |c|
      c.zadd(key("z"), 2, "n", 3.5, "m")

      assert_equal [["n", 2.0]], c.zpopmin(key("z"))
      assert_equal [["m", 3.5]], c.zpopmax(key("z"))
      assert_in_delta(1.5, c.incrbyfloat(key("f"), 1.5))
      assert_in_delta(0.5, c.hincrbyfloat(key("h"), "c", 0.5))
      assert_equal ["maxmemory"], c.config("get", "maxmemory").keys
      assert_equal ["maxmemory"], c.config(:get, "maxmemory").keys
      assert_equal %w[maxclients maxmemory], c.config("GET", "maxmemory", "maxclients").keys.sort
      assert_equal "OK", c.config("RESETSTAT")
      assert_kind_of Array, c.config("HELP")
    end
  end

  def test_pipelines_and_transactions
    replies = conn do |c|
      c.pipelined do |p|
        p.set(key("a"), "1")
        p.call("HSET", key("h"), "f", "v")
        p.hgetall(key("h"))
      end
    end

    assert_equal ["OK", 1, {"f" => "v"}], replies
    replies = conn do |c|
      c.multi do |tx|
        tx.incr(key("n"))
        tx.zadd(key("z"), 1, "m")
        tx.zscore(key("z"), "m")
      end
    end

    assert_equal [1, 1, 1.0], replies
    assert_respond_to Sidekiq::RedisConnection::Collector.new, :anything
  end

  def test_scans_iterate_every_element
    conn do |c|
      5.times { |i| c.set(key("str#{i}"), i) }
      c.sadd(key("set"), %w[a b])
      c.hset(key("h"), "f", "v", "g", "w")
      c.zadd(key("z"), 2, "m", 3, "n")

      assert_equal 5, c.scan(match: key("str*")).count
      assert_equal 5, c.scan("MATCH", key("str*"), count: 1).to_a.size
      assert_equal %w[a b], c.sscan(key("set")).sort
      assert_equal [%w[f v], %w[g w]], c.hscan(key("h")).to_a.sort
      assert_equal [["m", 2.0], ["n", 3.0]], c.zscan(key("z")).to_a.sort
      found = []
      c.scan(match: key("str1")) { |k| found << k }
      c.sscan(key("set"), match: "a") { |m| found << m }
      c.hscan(key("h"), match: "f") { |pair| found << pair }
      c.zscan(key("z"), match: "m") { |pair| found << pair }
      c.sscan(key("set"), "MATCH", "b") { |m| found << m }
      c.hscan(key("h"), "MATCH", "g") { |pair| found << pair }
      c.zscan(key("z"), "MATCH", "n") { |pair| found << pair }

      assert_equal [key("str1"), "a", %w[f v], ["m", 2.0], "b", %w[g w], ["n", 3.0]], found
    end
  end

  def test_info_blocking_calls_and_scripts
    conn do |c|
      info = c.info

      assert_match(/\A\d+\.\d+\.\d+\z/, info.fetch("redis_version"))
      refute info.key?("# Server")
      assert c.info("memory").key?("used_memory")
      refute c.info("memory").key?("redis_version")
      c.rpush(key("q"), "job")

      assert_equal [key("q"), "job"], c.blocking_call(1, "BRPOP", key("q"), 1)
      assert_nil c.blocking_call(nil, "LPOP", key("q"))
      sha = c.script("load", "return ARGV[1] .. KEYS[1]")

      assert_equal "x#{key("k")}", c.evalsha(sha, [key("k")], ["x"])
      assert_equal "y", c.evalsha(c.script("load", "return ARGV[1]"), [], ["y"])
      assert_true c.connected?
      assert_respond_to c, :any_command_at_all
      assert_instance_of Robin::Redis::Connection, c.conn
    end
  end

  def test_close
    conn do |c|
      c.close

      assert_equal "PONG", c.ping, "reconnects"
    end
  end

  def test_pool
    pool = Sidekiq.redis_pool

    assert_same config.redis_pool, pool.pool
    assert_equal "PONG", pool.with(&:ping)
    assert_equal "PONG", pool.then(&:ping)
    assert_equal config.redis_pool.size, pool.size
    pool.shutdown

    assert_equal "PONG", pool.with(&:ping)
  end
end
