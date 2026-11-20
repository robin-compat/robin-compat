# frozen_string_literal: true

module Sidekiq
  # What Sidekiq.redis yields: a Robin connection that speaks the
  # redis-client interface apps use with Sidekiq.
  #
  #   Sidekiq.redis do |conn|
  #     conn.set("lock", "1", nx: true, ex: 60)  # keyword arguments become flags
  #     conn.call("HGETALL", "h")                # => {"field" => "value"}
  #     conn.pipelined { |p| p.incr("a"); p.incr("b") }
  #   end
  #
  # Replies come back in the shapes RESP3 gives them, as redis-client
  # returns them: hashes for HGETALL and CONFIG GET, [member, score]
  # pairs for sorted-set ranges WITHSCORES, Floats for scores.
  module RedisConnection
    # Turns call arguments into a command: keyword arguments become flags
    # (+true+ adds just the name, +false+/+nil+ leave it out, anything else
    # adds the name and the value), Arrays are flattened, Hashes splatted.
    def self.command(args, kwargs)
      command = args.flat_map { |arg| arg.is_a?(Hash) ? arg.to_a.flatten : arg }
      kwargs.each do |name, value|
        next unless value

        command << name.to_s.upcase
        command << value unless value.equal?(true)
      end
      command
    end

    # The RESP3-shaped reply to +command+ (see the module docs).
    def self.shape(command, reply)
      name, subcommand = command
      case name.to_s.upcase
      when "HGETALL" then reply.each_slice(2).to_h
      when "CONFIG" then subcommand.to_s.casecmp?("GET") ? reply.each_slice(2).to_h : reply
      when "ZSCORE", "ZINCRBY", "INCRBYFLOAT", "HINCRBYFLOAT"
        reply && Float(reply)
      when "ZMSCORE"
        reply.map { |score| score && Float(score) }
      when "ZPOPMIN", "ZPOPMAX"
        pairs(reply)
      when "ZRANGE", "ZRANGEBYSCORE", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZRANDMEMBER", "ZUNION", "ZINTER", "ZDIFF"
        (command.any? { |arg| arg.to_s.casecmp?("WITHSCORES") }) ? pairs(reply) : reply
      else reply
      end
    end

    def self.pairs(reply) = reply.each_slice(2).map { |member, score| [member, Float(score)] }

    # Commands collected for a pipeline or transaction.
    class Collector
      attr_reader :commands

      def initialize
        @commands = []
      end

      def call(*args, **kwargs)
        @commands << RedisConnection.command(args, kwargs)
        nil
      end

      def method_missing(name, *args, **kwargs) = call(name.to_s, *args, **kwargs)

      def respond_to_missing?(*) = true
    end

    class Connection
      def initialize(conn)
        @conn = conn
      end

      # The Robin::Redis::Connection underneath.
      attr_reader :conn

      def call(*args, **kwargs)
        command = RedisConnection.command(args, kwargs)
        RedisConnection.shape(command, @conn.call(*command))
      end

      def blocking_call(timeout, *args, **kwargs)
        command = RedisConnection.command(args, kwargs)
        RedisConnection.shape(command, @conn.blocking_call(timeout || 0, *command))
      end

      def pipelined
        collector = Collector.new
        yield collector
        replies = @conn.pipelined { |p| collector.commands.each { |command| p.call(*command) } }
        collector.commands.zip(replies).map { |command, reply| RedisConnection.shape(command, reply) }
      end

      def multi
        collector = Collector.new
        yield collector
        replies = @conn.multi { |tx| collector.commands.each { |command| tx.call(*command) } }
        collector.commands.zip(replies).map { |command, reply| RedisConnection.shape(command, reply) }
      end

      # Iterates with SCAN (or SSCAN, HSCAN, ZSCAN on +key+), yielding each
      # element (HSCAN and ZSCAN: each [field, value] or [member, score]).
      def scan(*args, **kwargs, &) = cursor_scan("SCAN", nil, args, kwargs, &)

      def sscan(key, *args, **kwargs, &) = cursor_scan("SSCAN", key, args, kwargs, &)

      def hscan(key, *args, **kwargs, &) = cursor_scan("HSCAN", key, args, kwargs, &)

      def zscan(key, *args, **kwargs, &) = cursor_scan("ZSCAN", key, args, kwargs, &)

      # INFO as a Hash (section headers and blank lines left out).
      def info(*sections)
        call("INFO", sections).each_line.with_object({}) do |line, info|
          key, value = line.chomp.split(":", 2)
          info[key] = value if value
        end
      end

      def exists?(*keys) = call("EXISTS", keys).positive?

      def evalsha(sha, keys = [], argv = []) = call("EVALSHA", sha, keys.size, keys, argv)

      def close = @conn.close

      def connected? = true

      def method_missing(name, *args, **kwargs) = call(name.to_s, *args, **kwargs)

      def respond_to_missing?(*) = true

      private

      def cursor_scan(command, key, args, kwargs, &block)
        return to_enum(:cursor_scan, command, key, args, kwargs) unless block

        pairs = %w[HSCAN ZSCAN].include?(command)
        cursor = "0"
        loop do
          cursor, items = call(command, *key, cursor, args, **kwargs)
          items = items.each_slice(2).map { |a, b| [a, (command == "ZSCAN") ? Float(b) : b] } if pairs
          items.each(&block)
          break if cursor == "0"
        end
      end
    end

    # What Sidekiq.redis_pool returns: Robin's pool, yielding Connections.
    class Pool
      def initialize(pool)
        @pool = pool
      end

      # The Robin::Redis::Pool underneath.
      attr_reader :pool

      def with
        @pool.with { |conn| yield Connection.new(conn) }
      end
      alias_method :then, :with

      def size = @pool.size

      def shutdown(&) = @pool.shutdown
    end
  end
end
