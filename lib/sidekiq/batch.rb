# frozen_string_literal: true

require "sidekiq"

module Sidekiq
  # Sidekiq Pro's batches, as Robin::Batch:
  #
  #   batch = Sidekiq::Batch.new
  #   batch.description = "Import #{import.id}"
  #   batch.on(:success, ImportCallbacks, "import_id" => import.id)
  #   batch.jobs { rows.each { |row| ImportRowJob.perform_async(row) } }
  #
  # Callbacks get (status, options), as with Sidekiq.
  class Batch < Robin::Batch
    NoSuchBatch = Robin::Batch::NoSuchBatch

    # The server middleware that tracks batch jobs. Robin's servers always
    # run it; tests add it to Sidekiq::Testing.server_middleware.
    Server = Robin::Batch::ServerMiddleware

    # As with Sidekiq Pro, the block's jobs are pushed together when it
    # finishes, and none if it raises.
    def jobs = super(atomic: true)

    # Callback options are meant to be a Hash, but Sidekiq Pro takes any
    # other value with a warning (from Pro 8.0.3) and hands it to the
    # callback as given.
    def on(event, target, options = {})
      return super if Hash === options

      config.logger.warn { "Sidekiq::Batch#on: callback options should be a Hash, not #{options.inspect}" }
      super(event, target, Verbatim.new(options))
    end

    # A value Robin::Batch#on stores as is (it stringifies a Hash's keys).
    Verbatim = Struct.new(:value) do
      def transform_keys = value
    end
    private_constant :Verbatim

    # Stops the batch's jobs that haven't run yet (Robin skips them, and
    # running ones see valid_within_batch? turn false).
    def invalidate_all = cancel!

    # Stops these jobs of the batch the same way (in one round trip,
    # however many). Returns the jids.
    def invalidate_jids(*jids)
      jids = jids.flatten
      Robin::Cancellation.cancel!(jids, ttl: TTL, config: config)
      jids
    end

    def invalidated? = cancelled?

    def status = Status.new(bid, config: config)

    def parent = parent_bid && self.class.new(parent_bid, config: config)

    # A batch's state, as Sidekiq Pro's Batch::Status reports it.
    class Status < Robin::Batch::Status
      # Why a batch's job failed.
      FailureInfo = Struct.new(:jid, :error_class, :error_message, :backtrace)

      def initialize(bid, config: Robin.config)
        super
        raise NoSuchBatch, "no batch #{bid} (expired or deleted?)" unless exists?
      end

      def created_at
        ms = super
        ms && Time.at(Rational(ms, 1000))
      end

      def invalidated? = cancelled?

      def failed_jids = failed_jobs.map { |job| job.fetch("jid") }

      # Robin keeps each failure's error class and message, not its
      # backtrace.
      def failure_info
        failed_jobs.map { |job| FailureInfo.new(job.fetch("jid"), job["error_class"], job["error_message"]) }
      end

      # The failed jobs that are now in the dead set.
      def dead_jids
        dead = Robin::DeadSet.new(config: @config)
        failed_jids.select { |jid| dead.find_job(jid) }
      end

      def delete = delete!

      # Waits until the batch is complete, checking every +interval+
      # seconds.
      def join(interval = 0.5)
        until complete?
          sleep interval
          reload
        end
        self
      end

      # The batch as a JSON-friendly Hash.
      def data
        {"bid" => bid, "total" => total, "pending" => pending, "failures" => failures,
         "description" => description, "parent_bid" => parent_bid, "complete" => complete?,
         "created_at" => created_at&.to_f, "fail_info" => failed_jobs, "tags" => tags}
      end

      private

      def reload = initialize(bid, config: @config)
    end

    # Batch summaries (Robin::Batch.list), every one of them (a limit of
    # 0 is no limit): those tagged +tag+, if given.
    def self.summaries(tag = nil) = list(limit: 0, tag: tag)

    # The batches that haven't succeeded yet, newest first.
    class BatchSet
      include Enumerable

      def each
        return to_enum unless block_given?

        Batch.summaries.each { |summary| yield Status.new(summary.fetch("bid")) unless summary.fetch("success_at") }
      end

      def size = count

      # The bids of the batches tagged +tag+ (Sidekiq Pro 8.1's tags),
      # yielded or returned.
      def scan_tags(tag, &block)
        bids = Batch.summaries(tag).map { |summary| summary.fetch("bid") }
        block ? bids.each(&block) : bids
      end
    end

    # The batches that had a job die.
    class DeadSet
      include Enumerable

      def each
        return to_enum unless block_given?

        Batch.summaries.each { |summary| yield Status.new(summary.fetch("bid")) if summary.fetch("death_at") }
      end

      def size = count
    end

    private

    # A batch is a child only when created inside another's +jobs+ block
    # (not merely inside a job that belongs to a batch).
    def parent_for_new = Batch.current&.bid
  end

  BatchSet = Batch::BatchSet

  module Job
    # Batch helpers on running jobs.
    module InstanceMethods
      # The job's batch (adding jobs to it is atomic, as with any
      # Sidekiq::Batch), or nil.
      def batch = bid && Batch.new(bid)

      # False once the job's batch was invalidated (cancelled): the job
      # should stop.
      def valid_within_batch? = !cancelled?
    end
  end
end

# Batch callbacks get Sidekiq's status object.
Sidekiq.defaults!(Robin.config)
