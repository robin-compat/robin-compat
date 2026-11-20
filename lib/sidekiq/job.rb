# frozen_string_literal: true

module Sidekiq
  # Include in a class to make it a job, as with Sidekiq. It's a Robin job
  # (Robin::Job) underneath, so Robin's own options work too.
  #
  #   class HardJob
  #     include Sidekiq::Job
  #     sidekiq_options queue: "critical", retry: 5
  #     sidekiq_retry_in { |count, exception, job| 10 * (count + 1) }
  #
  #     def perform(name, count)
  #     end
  #   end
  module Job
    @default_options = {"retry" => true, "queue" => "default"}.freeze

    # Sidekiq's retry backoff, as Robin's config.retry_backoff takes it.
    RETRY_BACKOFF = lambda do |failures, _exception, _job, custom|
      count = failures - 1
      (custom || ((count**4) + 15)) + rand(10 * (count + 1))
    end

    class << self
      # Options every job class starts from (Sidekiq.default_job_options).
      attr_reader :default_options

      def default_options=(options)
        @default_options = options.to_h { |k, v| [k.to_s, v] }.freeze
      end

      def included(base)
        base.include(Robin::Job)
        base.include(InstanceMethods)
        base.extend(ClassMethods)
      end

      # What a sidekiq_retry_in block's answer means: a positive number of
      # seconds (Strings too), :discard or :kill; anything else is the
      # default backoff (nil).
      def retry_delay(answer)
        return answer if answer == :discard || answer == :kill

        seconds = answer.to_i if answer.respond_to?(:to_i)
        seconds if seconds&.positive?
      end

      # Sidekiq stringifies option keys, and the keys of Hash values.
      def stringify(options)
        options.map { |k, v| [k.to_s, (Hash === v) ? v.transform_keys(&:to_s) : v] }.to_h
      end
    end

    # Methods a job instance has that take precedence over Robin::Job's
    # (sidekiq/batch adds +batch+ and +valid_within_batch?+).
    module InstanceMethods; end

    module ClassMethods
      def sidekiq_options(opts = {})
        opts = Job.stringify(opts.to_h) # Sidekiq takes nil as no options
        # A pool (a shard's Redis) stays with the class rather than the job.
        @sidekiq_pool = opts.delete("pool") if opts.key?("pool")
        # Sidekiq retries until retry_for runs out, whatever "retry" says.
        opts["retry"] = true if opts.key?("retry_for")
        # Sidekiq encrypts the last argument only.
        opts["encrypt"] = "last" if opts["encrypt"]
        robin_options(opts)
        get_sidekiq_options
      end

      def get_sidekiq_options
        pool = sidekiq_pool
        pool ? robin_options.merge({"pool" => pool}) : robin_options
      end

      # A class without options of its own (or inherited) has
      # Sidekiq.default_job_options rather than Robin's defaults.
      def robin_options(opts = nil)
        options = super
        options.equal?(Robin::Job::DEFAULT_OPTIONS) ? Job.default_options : options
      end

      def sidekiq_pool = @sidekiq_pool || (superclass.sidekiq_pool if superclass.respond_to?(:sidekiq_pool))

      # The block gets (retry count, exception, job): the count is 0 the
      # first time the job fails.
      def sidekiq_retry_in(&block)
        @sidekiq_retry_in_block = block
        robin_retry_in { |failures, exception, job| Job.retry_delay(block.call(failures - 1, exception, job)) }
      end

      def sidekiq_retry_in_block
        @sidekiq_retry_in_block || (superclass.sidekiq_retry_in_block if superclass.respond_to?(:sidekiq_retry_in_block))
      end

      # The block gets (job, exception); returning :discard skips the dead
      # set.
      def sidekiq_retries_exhausted(&) = robin_retries_exhausted(&)

      def sidekiq_retries_exhausted_block = robin_retries_exhausted_block

      def queue_as(queue) = sidekiq_options({"queue" => queue})

      def set(options) = Setter.new(self, options)

      # perform_async, perform_in and perform_at are Robin::Job's (pushing
      # through client_push below).

      def perform_inline(*) = Setter.new(self, {}).perform_inline(*)
      alias_method :perform_sync, :perform_inline

      def perform_bulk(args, **) = Setter.new(self, {}).perform_bulk(args, **)

      def delay(*) = raise(ArgumentError, "Do not call .delay on a Sidekiq::Job class, call .perform_async")

      def delay_for(*) = raise(ArgumentError, "Do not call .delay_for on a Sidekiq::Job class, call .perform_in")

      def delay_until(*) = raise(ArgumentError, "Do not call .delay_until on a Sidekiq::Job class, call .perform_at")

      def client_push(item)
        item = item.transform_keys(&:to_s)
        pool = item.delete("pool") || sidekiq_pool
        client = Client.new(pool: pool)
        client.push(item)
      end

      def build_client = Client.new(pool: sidekiq_pool)

      # A job's uniqueness: the digest covers its class, queue and what
      # sidekiq_unique_context returns (the class may define it, given the
      # job), else its arguments.
      def robin_unique_args(args)
        return args unless respond_to?(:sidekiq_unique_context)

        sidekiq_unique_context({"class" => name, "queue" => get_sidekiq_options["queue"], "args" => args})
      end
    end

    # Per-push options: HardJob.set(queue: "low", wait: 30).perform_async(1)
    class Setter
      def initialize(klass, options)
        @klass = klass
        @options = {}
        set(options)
      end

      # +wait+ is resolved now (so a reused setter keeps its time);
      # +sync: true+ runs the job inline.
      def set(options)
        options = Job.stringify(options)
        if (wait = options.delete("wait"))
          options["at"] = Robin::Client.timestamp_ms(wait)
        end
        if (wait_until = options.delete("wait_until"))
          options["at"] = wait_until
        end
        @options.merge!(options)
        self
      end

      def perform_async(*args)
        return perform_inline(*args) if @options["sync"]

        @klass.client_push(@options.merge({"class" => @klass, "args" => args}))
      end

      def perform_in(interval, *args)
        @klass.client_push(@options.merge({"class" => @klass, "args" => args, "at" => interval}))
      end
      alias_method :perform_at, :perform_in

      # Runs the job now, in this thread, through the client and server
      # middleware and a JSON round trip, as Sidekiq does. Returns true, or
      # nil when a middleware stopped it.
      def perform_inline(*args)
        client = Robin::Client.new
        # normalize leaves out "sync".
        item = client.normalize(@options.except("pool").merge({"class" => @klass, "args" => args}))
        config = Robin.config
        pushed = config.client_middleware.invoke(item.fetch("class"), item, item.fetch("queue"), config.redis_pool) { item }
        return unless pushed

        job = Robin.load_json(Robin.dump_json(item))
        instance = Object.const_get(job.fetch("class")).new
        instance.jid = job.fetch("jid")
        instance.payload = job
        ran = config.server_middleware.invoke(instance, job, job.fetch("queue")) do
          instance.perform(*job.fetch("args"))
          true
        end
        true if ran
      end
      alias_method :perform_sync, :perform_inline

      # +options+ as Client#push_bulk takes them (batch_size, at,
      # spread_interval), over the setter's.
      def perform_bulk(args, **options)
        Client.new(pool: @options["pool"] || @klass.sidekiq_pool)
          .push_bulk(@options.except("pool").merge(options, {"class" => @klass, "args" => args}))
      end
    end
  end

  # Sidekiq's older name for Sidekiq::Job.
  Worker = Job
end
