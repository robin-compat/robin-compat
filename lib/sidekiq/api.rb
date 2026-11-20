# frozen_string_literal: true

require "sidekiq"

module Sidekiq
  # Sidekiq's data API on Robin's (see Robin::Stats, Robin::Queue...):
  # Sidekiq's names, orderings and return values.
  class Stats < Robin::Stats
    QueueSummary = Data.define(:name, :size, :latency, :paused) do
      alias_method :paused?, :paused
    end

    alias_method :fetch_stats!, :fetch
    alias_method :fetch_stats_fast!, :fetch
    alias_method :fetch_stats_slow!, :fetch

    # Each queue's name, size, latency and paused flag, largest first.
    def queue_summaries
      c = @config
      names = c.redis { |r| r.call("SMEMBERS", c.key("queues")) }
      rows = c.redis do |r|
        r.pipelined do |p|
          names.each do |name|
            p.call("LLEN", c.key("queue", name))
            p.call("LINDEX", c.key("queue", name), -1)
            p.call("SISMEMBER", c.key("paused"), name)
          end
        end
      end
      names.zip(rows.each_slice(3)).map do |name, (size, oldest, paused)|
        QueueSummary.new(name: name, size: size, latency: Queue.latency_of(oldest), paused: paused == 1)
      end.sort_by { |s| [-s.size, s.name] }
    end

    # Only the processed and failed counts can be reset (both by default).
    def reset(*stats)
      names = %w[processed failed]
      names &= stats.flatten.map(&:to_s) unless stats.empty?
      super(*names) unless names.empty?
    end

    # Daily processed and failed counts, newest first, for the
    # +days_previous+ days up to +start_date+ (a Date; today by default).
    class History
      def initialize(days_previous, start_date = nil, config: Robin.config)
        # Sidekiq keeps five years of daily counts.
        raise ArgumentError, "days_previous must be between 1 and 1825" unless days_previous.between?(1, 5 * 365)

        now = start_date ? Time.utc(start_date.year, start_date.month, start_date.day) : Time.now
        @history = Stats.new(config: config).history(days: days_previous, now: now)
      end

      def processed = @history.fetch("processed")

      def failed = @history.fetch("failed")
    end
  end

  # What Sidekiq's job records add to Robin's.
  module JobRecordMethods
    # Iteration state saved by an iterable job (Sidekiq::IterableJob).
    IterableState = Struct.new(:jid, :raw) do
      def cursor = Robin.load_json(raw.fetch("cursor")).fetch("c")

      def iterations = raw.fetch("iterations").to_i
    end

    # The job's iteration state, or nil if it has none.
    def iterable_state
      raw = @config.redis { |c| c.call("HGETALL", @config.key("iter", jid)) }
      IterableState.new(jid, raw.each_slice(2).to_h) unless raw.empty?
    end

    # When the job was created (or else pushed); the epoch for neither.
    def created_at = super || enqueued_at || Time.at(0)
  end

  class JobRecord < Robin::JobRecord
    include JobRecordMethods
  end

  class Queue < Robin::Queue
    alias_method :id, :name

    # Sidekiq starts with the most recently pushed job.
    def each(&block)
      # (Robin's each without a block would enumerate with this one.)
      jobs = []
      super { |job| jobs << job }
      jobs.reverse_each(&block)
    end

    private

    def record(raw) = JobRecord.new(raw, name, config: @config)
  end

  # A job in a sorted set, with Sidekiq's method names.
  class SortedEntry < Robin::SortedEntry
    include JobRecordMethods

    def at = super.utc

    # Pushes the job back onto its queue; as in Sidekiq, a retry made
    # here doesn't count as one of the job's retries.
    def retry
      remove_and { Client.new(config: @config).push(queued_item) }
    end

    # As in Sidekiq, a retry made here doesn't count as one of the job's
    # retries.
    def queued_item
      super.tap { |item| item["retry_count"] = Integer(item.fetch("retry_count")) - 1 if item.key?("retry_count") }
    end

    # Moves the job to the dead set, running the death handlers unless
    # +notify_failure+ is false.
    def kill(notify_failure: true, ex: nil)
      remove_and do |item|
        DeadSet.new(config: @config).kill(Robin.dump_json(item), notify_failure: notify_failure, ex: ex)
      end
    end

    def error? = !!self["error_class"]
  end

  class JobSet < Robin::JobSet
    # Entries each ZSCAN call looks at (by default).
    SCAN_COUNT = 100

    def initialize(name = nil, config: Robin.config)
      super(name || self.class::NAME, config: config)
    end

    # Sidekiq starts with the latest score.
    def each(&block)
      return to_enum unless block

      super(reverse: true)
    end

    # Entries whose JSON matches +match+ (a glob: "*" is added on both
    # sides unless it has one), yielded or returned.
    def scan(match, count = SCAN_COUNT, &block)
      pattern = match.include?("*") ? match : "*#{match}*"
      found = []
      cursor = "0"
      @config.redis do |c|
        loop do
          cursor, pairs = c.call("ZSCAN", key, cursor, "MATCH", pattern, "COUNT", count)
          pairs.each_slice(2) { |value, score| found << entry(score, value) }
          break if cursor == "0"
        end
      end
      block ? found.each(&block) : found
    end

    # Entries with exactly this score (a Time, epoch seconds or a Range of
    # either, which may be open-ended), optionally only the one with +jid+.
    def fetch(score, jid = nil)
      low, high = score.instance_of?(Range) ? [score.begin || "-inf", score.end || "+inf"] : [score, score]
      raw = @config.redis { |c| c.call("ZRANGE", key, seconds(low), seconds(high), "BYSCORE", "WITHSCORES") }
      entries = raw.each_slice(2).map { |value, s| entry(s, value) }
      jid ? entries.select { |e| e.jid == jid } : entries
    end

    def delete_by_value(_name, value) = @config.redis { |c| c.call("ZREM", key, value) } == 1

    alias_method :delete, :delete_by_jid

    # Pushes every entry back onto its queue, a page at a time; returns how
    # many.
    def retry_all = drain_pages { |page| retry_entries(page) }

    # Moves every entry to the dead set, a page at a time (running the
    # death handlers for each, with +notify_failure+); returns how many.
    def kill_all(notify_failure: false, ex: nil)
      return drain_pages { |page| kill_entries(page) } unless notify_failure

      drain { |e| e.kill(ex: ex) }
    end

    private

    def entry(score, value) = SortedEntry.new(self, score, value)

    def seconds(time) = time.is_a?(Time) ? time.to_f : time
  end

  SortedSet = JobSet

  class ScheduledSet < JobSet
    NAME = "schedule"
  end

  class RetrySet < JobSet
    NAME = "retry"
  end

  class DeadSet < JobSet
    NAME = "dead"

    # Adds a job (its JSON) to the dead set. Unless +notify_failure+ is
    # false, runs the death handlers with +ex+ (by default an error saying
    # the API killed it).
    def kill(message, opts = {})
      Robin::DeadSet.new(config: @config).kill(message)
      unless opts[:notify_failure] == false
        ex = opts[:ex] || RuntimeError.new("Job killed by API").tap { |e| e.set_backtrace(caller) }
        job = Robin.load_json(message)
        @config.death_handlers.each { |h| h.call(job, ex) }
      end
      true
    end
  end

  # A running process, with Sidekiq's readers.
  class Process < Robin::ProcessRecord
    def [](key)
      case key
      when "busy" then busy
      when "beat" then Robin.epoch_seconds(beat)
      when "quiet" then quiet?.to_s
      when "rss" then rss
      when "rtt_us" then rtt_us
      else info[key]
      end
    end

    alias_method :id, :identity

    def tag = self["tag"]

    def labels = self["labels"].to_a

    # The queues the process's capsules take jobs from.
    def queues = capsules.empty? ? self["queues"].to_a : capsules.values.flat_map { |c| c.fetch("weights").keys }.uniq

    # Each queue's weight (merged across capsules).
    def weights = capsules.empty? ? self["weights"].to_a : capsules.values.map { |c| c.fetch("weights") }.reduce(:merge)

    def capsules = self["capsules"].to_h

    def version = self["version"]

    def embedded? = !!self["embedded"]

    def stopping? = quiet?

    # An embedded process belongs to its host app, which stops it.
    def quiet!
      raise "Can't quiet an embedded process" if embedded?

      super
    end

    def stop!
      raise "Can't stop an embedded process" if embedded?

      super
    end
  end

  class ProcessSet < Robin::ProcessSet
    def self.[](identity, config: Robin.config) = new(false, config: config).find { |p| p.identity == identity }

    # Like Sidekiq, removes departed processes from the set first unless
    # +clean_plz+ is false.
    def initialize(clean_plz = true, config: Robin.config)
      super(config: config)
      cleanup if clean_plz
    end

    # Removes processes whose records have expired from the set (at most
    # once a minute across the cluster; Robin's leader does it anyway).
    # Returns how many.
    def cleanup
      c = @config
      return 0 unless c.redis { |r| r.call("SET", c.key("process_cleanup"), "1", "NX", "EX", 60) }

      identities = c.redis { |r| r.call("SMEMBERS", c.key("processes")) }
      alive = c.redis { |r| r.pipelined { |p| identities.each { |i| p.call("EXISTS", c.process_key(i)) } } }
      gone = identities.zip(alive).filter_map { |identity, exists| identity if exists.zero? }
      c.redis { |r| r.call("SREM", c.key("processes"), *gone) } unless gone.empty?
      gone.size
    end

    def total_rss_in_kb = total_rss_kb
    alias_method :total_rss, :total_rss_in_kb

    # The leader's identity ("" when there is none).
    def leader = super.to_s

    private

    def record(identity, data) = Process.new(identity, data, config: @config)
  end

  # A job running right now.
  class Work
    attr_reader :process_id, :thread_id

    def initialize(process_id, thread_id, hash)
      @process_id = process_id
      @thread_id = thread_id
      @hash = hash
    end

    def queue = @hash.fetch("queue")

    def run_at = Time.at(Rational(@hash.fetch("run_at"), 1000))

    def payload = @hash.fetch("payload")

    def job = JobRecord.new(payload, queue)
  end

  class WorkSet < Robin::WorkSet
    # Yields (process identity, thread id, Work), longest-running first.
    def each(&block)
      return to_enum unless block

      works = []
      super { |identity, tid, hash| works << [identity, tid, Work.new(identity, tid, hash)] }
      works.sort_by { |*, work| work.run_at }.each { |args| block.call(*args) }
    end

    # How many jobs are running, as the processes last reported.
    def size
      c = @config
      identities = c.redis { |r| r.call("SMEMBERS", c.key("processes")) }
      c.redis { |r| r.pipelined { |p| identities.each { |i| p.call("HGET", c.process_key(i), "busy") } } }.sum(&:to_i)
    end

    def find_work(jid) = find { |*, work| work.job.jid == jid }&.last
    alias_method :find_work_by_jid, :find_work
  end

  Workers = WorkSet
end
