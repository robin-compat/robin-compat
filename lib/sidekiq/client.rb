# frozen_string_literal: true

module Sidekiq
  # Pushes jobs, as Sidekiq::Client does, through Robin's client.
  #
  #   Sidekiq::Client.push("class" => HardJob, "args" => [1, 2])
  #   Sidekiq::Client.new(pool: shard).push_bulk("class" => HardJob, "args" => [[1], [2]])
  class Client
    class << self
      def push(item) = new.push(item)

      def push_bulk(items) = new.push_bulk(items)

      # Pushes to another Redis (a Sidekiq::RedisConnection::Pool, or
      # Robin's own pool) for the duration of the block.
      def via(pool, &) = Robin::Client.via(robin_pool(pool), &)

      def enqueue(klass, *args) = klass.client_push({"class" => klass, "args" => args})

      def enqueue_to(queue, klass, *args) = klass.client_push({"queue" => queue, "class" => klass, "args" => args})

      def enqueue_in(interval, klass, *args) = klass.perform_in(interval, *args)

      def enqueue_to_in(queue, interval, klass, *args)
        klass.client_push({"queue" => queue, "class" => klass, "args" => args, "at" => interval})
      end

      # A pool as Robin's client takes it.
      def robin_pool(pool) = pool.respond_to?(:pool) ? pool.pool : pool
    end

    attr_reader :redis_pool

    # +pool+ pushes to another Redis; a +chain+ replaces the client
    # middleware for this client's pushes.
    def initialize(*args, pool: nil, config: nil, chain: nil)
      pool ||= args.first
      @config = config.respond_to?(:robin) ? config.robin : config || Robin.config
      @pool = Client.robin_pool(pool)
      @redis_pool = pool
      @chain = chain && Middleware::Chain.new(chain.respond_to?(:__getobj__) ? chain.__getobj__ : chain)
    end

    # This client's own middleware chain: a copy of the configured one,
    # made the first time it's asked for.
    def middleware
      @chain ||= Middleware::Chain.new(@config.client_middleware.dup)
      yield @chain if block_given?
      @chain
    end

    def push(item) = robin.push(item)

    # Pushes +batch_size+ (default 1,000) jobs per round trip; returns the
    # jids.
    def push_bulk(items)
      items = items.transform_keys(&:to_s)
      batch_size = items.delete("batch_size") || 1_000
      args = items.fetch("args")
      ats = items["at"]
      jids = items.delete("jids")
      args.each_slice(batch_size).with_index.flat_map do |slice, n|
        bulk = items.merge({"args" => slice})
        bulk["at"] = ats[n * batch_size, slice.size] if ats.is_a?(Array)
        bulk["jids"] = jids[n * batch_size, slice.size] if jids
        robin.push_bulk(bulk)
      end
    end

    # Iterable jobs check for this at their next iteration.
    def cancel!(jid) = Robin.cancel!(jid)

    # The job hash as it would be pushed: the class's options filled in,
    # with a jid and created_at, and validated (see Robin::Client#normalize).
    def normalize_item(item) = robin.normalize(item)

    private

    def robin = Robin::Client.new(config: @config, pool: @pool, chain: @chain)
  end
end
