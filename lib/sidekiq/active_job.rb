# frozen_string_literal: true

require "sidekiq"
require "robin/active_job"

module Sidekiq
  module ActiveJob
    # Active Job jobs Sidekiq pushed name this class: they run on Robin's
    # wrapper.
    Wrapper = Robin::ActiveJob::Wrapper
  end

  module Job
    # sidekiq_options on Active Job classes.
    module Options
      def self.included(base)
        base.include(Robin::ActiveJob::Options)
        base.extend(ClassMethods)
      end

      # sidekiq_options, and the retry hooks (Robin runs an Active Job
      # job's from the class it wraps).
      module ClassMethods
        def sidekiq_options(opts = {}) = robin_options(Job.stringify(opts))

        def get_sidekiq_options = Job.default_options.merge(robin_options)

        def sidekiq_options_hash = get_sidekiq_options

        # The block gets (retry count, exception, job): the count is 0 the
        # first time the job fails.
        def sidekiq_retry_in(&block)
          @sidekiq_retry_in_block = block
          @robin_retry_in_block = ->(failures, exception, job) { Job.retry_delay(block.call(failures - 1, exception, job)) }
        end

        # The block gets (job, exception); returning :discard skips the
        # dead set.
        def sidekiq_retries_exhausted(&block)
          @robin_retries_exhausted_block = block
        end

        def sidekiq_retry_in_block = inherited_hook(:@sidekiq_retry_in_block)

        def robin_retry_in_block = inherited_hook(:@robin_retry_in_block)

        def robin_retries_exhausted_block = inherited_hook(:@robin_retries_exhausted_block)

        def sidekiq_retries_exhausted_block = robin_retries_exhausted_block

        private

        # The hook this class or the closest superclass set.
        def inherited_hook(name) = ancestors.filter_map { |klass| klass.instance_variable_get(name) }.first
      end
    end
  end
end

module ActiveJob
  module QueueAdapters
    # The adapter Rails ships under this name (registered for autoloading,
    # or already loaded; Rails 8.2 has none) gives way to Robin's, without
    # being loaded.
    (constants(false) & [:SidekiqAdapter]).each { |name| remove_const(name) }

    # The :sidekiq adapter, as Active Job names it, is Robin's adapter.
    class SidekiqAdapter < RobinAdapter
      # The wrapper class name Sidekiq used before it named its own.
      JobWrapper = Robin::ActiveJob::Wrapper

      # Jobs pushed inside a transaction are sent after it commits.
      def enqueue_after_transaction_commit? = true

      private

      # Jobs name Sidekiq's wrapper class, as Sidekiq's adapter pushes
      # them, so Sidekiq processes can run them too (Robin's run them on
      # its own wrapper, an alias): Sidekiq 8's, or for apps written for
      # Sidekiq 7, the one Sidekiq 7 knows (which Sidekiq 8 knows too).
      def payload(job, timestamp) = super.merge({"class" => Sidekiq.compatible_before?("8.0") ? JOB_WRAPPER_7 : JOB_WRAPPER})

      JOB_WRAPPER = "Sidekiq::ActiveJob::Wrapper"
      JOB_WRAPPER_7 = "ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper"
    end
  end
end

ActiveSupport.on_load(:active_job) { include Sidekiq::Job::Options }
