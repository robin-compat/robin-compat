# frozen_string_literal: true

require "sidekiq/cron"

module Sidekiq
  module Cron
    # Enqueues the cron jobs due, every +interval+ seconds, while this
    # process is Robin's leader. (sidekiq-cron polls on every process: the
    # claim in Job#should_enqueue? decides who enqueues a tick, so leaders
    # and sidekiq-cron processes never enqueue one twice.)
    class Poller
      def initialize(config, interval:)
        @config = config
        @interval = interval
        @wake = Thread::Queue.new
      end

      def start
        @thread = Thread.new do
          Thread.current.name = "sidekiq.cron"
          until @wake.closed?
            enqueue if Robin::Leader.leader?(@config)
            @wake.deq(timeout: @interval)
          end
        end
        self
      end

      # Stops polling (after the poll under way, if any).
      def terminate
        @wake.close
        @thread&.join
      end

      # Enqueues every job due at +time+.
      def enqueue(time = Time.now)
        Job.all("*").each do |job|
          job.test_and_enqueue_for_time!(time) if job.valid?
        rescue => e
          @config.handle_exception(e, {context: "cron job #{job.name}"})
        end
      rescue => e
        @config.handle_exception(e, {context: "cron jobs"})
      end
    end
  end
end
