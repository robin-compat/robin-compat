# frozen_string_literal: true

require "sidekiq"

module Sidekiq
  # Sidekiq's testing modes. Requiring "sidekiq/testing" switches to fake
  # mode: pushed jobs are kept in memory (Sidekiq::Queues) instead of
  # going to Redis.
  #
  #   HardJob.perform_async(1)
  #   assert_equal 1, HardJob.jobs.size
  #   HardJob.drain
  #
  #   Sidekiq::Testing.inline! { HardJob.perform_async(2) } # runs it now
  #
  # Jobs run in tests go through Sidekiq::Testing.server_middleware, which
  # is empty unless a test adds to it, not the configured server chain.
  class Testing
    # Sidekiq 7.2+ raises it for a nested test mode, which robin-compat
    # allows (see .switch). Kept for apps that rescue it.
    class TestModeAlreadySetError < RuntimeError; end

    MODE_KEY = :sidekiq_test_mode

    class << self
      attr_accessor :global_mode

      def fake!(&) = switch(:fake, &)

      def inline!(&) = switch(:inline, &)

      def disable!(&) = switch(:disable, &)

      def mode = Thread.current[MODE_KEY] || global_mode

      def enabled? = !disabled?

      def disabled? = mode == :disable

      def fake? = mode == :fake

      def inline? = mode == :inline

      # Sidekiq's internal names, which some test helpers call.
      alias_method :__test_mode, :mode

      # The middleware jobs run through in tests (empty by default).
      def server_middleware
        @server_chain ||= Middleware::Chain.new(Robin::Middleware::Chain.new(Robin.config))
        yield @server_chain if block_given?
        @server_chain
      end

      # Called by the client in place of a Redis push: returns the
      # payloads it took, or nil to let them go to Redis.
      def intercept(payloads)
        if fake?
          now = Robin.now_ms
          payloads.each do |job|
            job = Robin.load_json(Robin.dump_json(job))
            # As in Sidekiq, only a job due later keeps its "at"; the rest
            # count as pushed now.
            unless job.key?("at") && job.fetch("at") > Robin.epoch_seconds(now)
              job.delete("at")
              job["enqueued_at"] = Robin.config.job_timestamp(now)
            end
            Queues.push(job.fetch("queue"), job.fetch("class"), job)
          end
        elsif inline?
          payloads.each do |job|
            job = Robin.load_json(Robin.dump_json(job))
            job_class(job.fetch("class")).process_job(job)
          end
        end
      end

      def __set_test_mode(mode, &) = switch(mode, &)

      # The job class named +name+.
      # mutant:disable -- `const_get` on Testing instead of Object differs
      # only for constants nested in Testing, none of which are job classes.
      def job_class(name) = Object.const_get(name)

      private

      # A block's mode lasts for the block, on its thread. Blocks may nest,
      # as in Sidekiq 7.0 and 7.1 (from 7.2, Sidekiq raises
      # TestModeAlreadySetError instead).
      def switch(mode)
        return self.global_mode = mode unless block_given?

        previous = Thread.current[MODE_KEY]
        Thread.current[MODE_KEY] = mode
        begin
          yield
        ensure
          Thread.current[MODE_KEY] = previous
        end
      end
    end
  end

  class EmptyQueueError < RuntimeError; end

  # The jobs pushed in fake mode, by queue and by class. The arrays are
  # live: tests may change them.
  module Queues
    class << self
      def [](queue) = jobs_by_queue[queue.to_s]

      def push(queue, klass, job)
        self[queue] << job
        jobs_by_class[klass] << job
      end

      def jobs_by_queue = @jobs_by_queue ||= Hash.new { |h, k| h[k] = [] }

      def jobs_by_class = @jobs_by_class ||= Hash.new { |h, k| h[k] = [] }
      alias_method :jobs_by_worker, :jobs_by_class

      def delete_for(jid, queue, klass)
        self[queue].delete_if { |job| job["jid"] == jid }
        jobs_by_class[klass].delete_if { |job| job["jid"] == jid }
      end

      def clear_for(queue, klass)
        self[queue].clear
        jobs_by_class[klass].clear
      end

      def clear_all
        jobs_by_queue.clear
        jobs_by_class.clear
      end
    end
  end

  module Job
    # Fake-mode helpers on job classes.
    module TestingMethods
      def queue = get_sidekiq_options.fetch("queue")

      # The class name its jobs are pushed with, which fake mode keeps
      # them by.
      def jobs_key = to_s

      def jobs = Queues.jobs_by_class[jobs_key]

      def clear = Queues.clear_for(queue, jobs_key)

      # Runs the jobs of this class (including any they push), in the
      # order they were pushed.
      def drain
        while (job = jobs.first)
          Queues.delete_for(job["jid"], job["queue"], jobs_key)
          process_job(job)
        end
      end

      def perform_one
        job = jobs.first or raise(EmptyQueueError, "perform_one called with empty job queue")
        Queues.delete_for(job["jid"], job["queue"], jobs_key)
        process_job(job)
      end

      def process_job(job)
        instance = new
        instance.jid = job["jid"]
        instance.payload = job
        Testing.server_middleware.invoke(instance, job, job["queue"]) { execute_job(instance, job["args"]) }
      end

      def execute_job(job, args) = job.perform(*args)
    end
    ClassMethods.include(TestingMethods)

    class << self
      def jobs = Queues.jobs_by_queue.values.flatten

      def clear_all = Queues.clear_all

      def drain_all
        while jobs.any?
          jobs.map { |job| job.fetch("class") }.uniq.each { |name| Testing.job_class(name).drain }
        end
      end
    end
  end

  Testing.global_mode = :fake
  Robin::Client.interceptor = ->(payloads) { Testing.intercept(payloads) }
end
