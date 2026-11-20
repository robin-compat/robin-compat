# frozen_string_literal: true

module SidekiqScheduler
  # Runs a Scheduler on every server: reloads its dynamic schedule every
  # dynamic_every, and (while the process is Robin's leader) pushes the
  # jobs due, waking for each.
  class Poller
    def initialize(scheduler, config)
      @scheduler = scheduler
      @config = config
      @wake = Queue.new
    end

    def start
      @thread = Thread.new do
        update_at = Time.now
        until @wake.closed?
          update_at = poll(Time.now, update_at)
          @wake.deq(timeout: @scheduler.seconds_until_next)
        end
      end
      @thread.name = "sidekiq-scheduler"
      self
    end

    # Stops (after the poll under way, if any).
    def terminate
      @wake.close
      @thread&.join unless Thread.current.equal?(@thread)
    end

    # One poll at +now+. Returns when the dynamic schedule is next to be
    # reloaded.
    def poll(now, update_at)
      if @scheduler.dynamic && now >= update_at
        @scheduler.update_schedule
        update_at = now + Utils.duration(@scheduler.dynamic_every || "5s")
      end
      @scheduler.enqueue_due(now) if Robin::Leader.leader?(@config)
      update_at
    rescue => e
      @config.handle_exception(e, {context: "sidekiq-scheduler"})
      update_at
    end
  end
end
