# frozen_string_literal: true

require "json"
require "securerandom"
require "time"
require "sidekiq/cron"

module Sidekiq
  module Cron
    # A cron job, kept in Redis as sidekiq-cron keeps it:
    #
    #   cron_jobs:<namespace>                    set   the jobs' keys
    #   cron_job:<namespace>:<name>              hash  the job (#to_hash)
    #   cron_job:<namespace>:<name>:enqueued     zset  the ticks enqueued (24 hours' worth)
    #   cron_job:<namespace>:<name>:jid_history  list  the latest enqueues, newest first
    #
    # (each key with Sidekiq's key prefix, none by default). A namespace
    # left out (or blank) is the default one.
    class Job
      # How long the ticks enqueued are remembered.
      REMEMBER_THRESHOLD = 24 * 60 * 60

      # How last_enqueue_time is stored.
      LAST_ENQUEUE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"

      # The key of an argument stored as a GlobalID.
      GLOBALID_KEY = "_sc_globalid"

      # A flag's true values ("1", "yes"...), as sidekiq-cron reads them.
      TRUE_FLAG = /\A(true|t|yes|y|1)\z/i
      private_constant :TRUE_FLAG

      attr_accessor :name, :namespace, :cron, :description, :klass, :message
      attr_reader :last_enqueue_time, :fetch_missing_args, :source, :args, :status

      # Takes the job's attributes (String or Symbol keys): name, cron and
      # class (or klass) are needed; namespace, source ("schedule" or
      # "dynamic"), queue, retry, args, date_as_argument, active_job,
      # queue_name_prefix, queue_name_delimiter, symbolize_args,
      # description and status ("enabled" or "disabled") are optional.
      # The status and last enqueue time not given are read from Redis.
      def initialize(input_args = nil)
        attrs = input_args.to_h { |key, value| [key.to_s, value] }
        @fetch_missing_args = !attrs.delete("fetch_missing_args").equal?(false)
        @name = attrs["name"]
        @cron = attrs["cron"]
        @description = attrs["description"]
        @source = (attrs["source"] == "schedule") ? "schedule" : "dynamic"
        @namespace = namespace_for(attrs["namespace"])
        @klass = attrs["klass"] || attrs["class"]
        @status = attrs["status"] || status_from_redis
        last = attrs["last_enqueue_time"]
        @last_enqueue_time = last.to_s.empty? ? last_enqueue_time_from_redis : parse_enqueue_time(last)
        @symbolize_args = flag?(attrs["symbolize_args"])
        @args = parse_args(attrs["args"])
        @date_as_argument = flag?(attrs["date_as_argument"])
        @active_job = flag?(attrs["active_job"])
        @active_job_queue_name_prefix = attrs["queue_name_prefix"]
        @active_job_queue_name_delimiter = attrs["queue_name_delimiter"]
        if @symbolize_args && !@active_job
          Sidekiq.logger.warn { "Cron Jobs - 'symbolize_args' is gonna be ignored, as it is only used when 'active_job' is true" }
        end
        build_message(attrs)
        @queue_name_with_prefix = queue_name_with_prefix
      end

      class << self
        # Saves the jobs of +hash+ (name => attributes); returns the
        # invalid ones' errors (name => errors).
        def load_from_hash(hash, options = {})
          load_from_array(hash.map { |name, job| job.merge({"name" => name}) }, options)
        end

        # As load_from_hash, and destroys the jobs from a schedule
        # (source "schedule") +hash+ no longer has.
        def load_from_hash!(hash, options = {})
          destroy_removed_jobs(hash) # its names, the keys
          load_from_hash(hash, options)
        end

        # Saves the jobs of +array+ (attributes each); returns the invalid
        # ones' errors (name => errors).
        def load_from_array(array, options = {})
          array.each_with_object({}) do |attrs, errors|
            job = new(attrs.merge(options))
            errors[job.name] = job.errors unless job.save
          end
        end

        # As load_from_array, and destroys the jobs from a schedule
        # (source "schedule") +array+ no longer has.
        def load_from_array!(array, options = {})
          destroy_removed_jobs(array.map { |job| job["name"] || job[:name] })
          load_from_array(array, options)
        end

        # The jobs of +namespace+ ("*" for every namespace).
        def all(namespace = nil)
          keys = job_keys_from_namespace(namespace)
          hashes = Cron.redis { |c| c.pipelined { |p| keys.each { |key| p.call("HGETALL", "#{Cron.prefix}#{key}") } } }
          hashes.reject(&:empty?).map { |fields| new(fields.each_slice(2).to_h.merge({"fetch_missing_args" => false})) }
        end

        def count(namespace = nil)
          return Namespace.all_with_count.sum { |namespace_count| namespace_count.fetch(:count) } if namespace == "*"

          Cron.redis { |c| c.call("SCARD", "#{Cron.prefix}#{jobs_key(namespace)}") }
        end

        # The valid job named +name+ (or a Hash's "name"), or nil.
        def find(name, namespace = nil)
          name = name[:name] || name["name"] if name.instance_of?(Hash)
          job = new(Cron.redis { |c| c.call("HGETALL", "#{Cron.prefix}#{redis_key(name, namespace)}") }.each_slice(2))
          job if job.valid?
        end

        # Saves a new job; true if it's valid.
        def create(hash) = new(hash).save

        # Destroys the job named +name+ (or a Hash's "name"): true, or
        # false if there's none.
        def destroy(name, namespace = nil)
          job = find(name, namespace)
          job ? job.destroy : false
        end

        # Destroys the default namespace's jobs.
        def destroy_all!
          all.each(&:destroy)
          Sidekiq.logger.info { "Cron Jobs - deleted all jobs" }
        end

        # Destroys the jobs from a schedule not named in +names+; returns
        # their names.
        def destroy_removed_jobs(names)
          stale = all("*").select { |job| job.source == "schedule" && !names.include?(job.name) }
          stale.each { |job| destroy(job.name, job.namespace) }
          stale.map(&:name)
        end

        def exists?(name, namespace = nil)
          Cron.redis { |c| c.call("EXISTS", "#{Cron.prefix}#{redis_key(name, namespace)}") } == 1
        end

        # Moves the jobs of sidekiq-cron before 2.0 (no namespaces) to the
        # default namespace.
        def migrate_old_jobs_if_needed!
          old = "#{Cron.prefix}cron_jobs"
          Cron.redis { |c| c.call("SMEMBERS", old) }.each do |key|
            fields = Cron.redis { |c| c.call("HGETALL", "#{Cron.prefix}#{key}") }
            create(fields.each_slice(2).to_h.merge({"namespace" => nil})) # the default
            Cron.redis { |c| c.call("SREM", old, key) }
          end
        end

        def jobs_key(namespace = nil) = "cron_jobs:#{default_if_blank(namespace)}"

        def redis_key(name, namespace = nil) = "cron_job:#{default_if_blank(namespace)}:#{name}"

        def job_enqueued_key(name, namespace = nil) = "#{redis_key(name, namespace)}:enqueued"

        def jid_history_key(name, namespace = nil) = "#{redis_key(name, namespace)}:jid_history"

        private

        def default_if_blank(namespace) = namespace.to_s.empty? ? Cron.configuration.default_namespace : namespace

        def job_keys_from_namespace(namespace)
          names = (namespace == "*") ? Namespace.all : [namespace]
          Cron.redis { |c| names.flat_map { |name| c.call("SMEMBERS", "#{Cron.prefix}#{jobs_key(name)}") } }
        end
      end

      # -- scheduling -------------------------------------------------------

      # Claims the tick before +time+ and returns true if this job should
      # be enqueued for it: it's enabled, the tick is within the
      # reschedule grace period and isn't enqueued yet (by Robin or
      # sidekiq-cron: the claim is theirs too).
      def should_enqueue?(time)
        return false unless enabled?
        return false if past_scheduled_time?(time)
        return false if enqueued_after?(time)

        redis { |c| c.call("ZADD", key(job_enqueued_key), formatted_enqueue_time(time), formatted_last_time(time)) } == 1
      end

      # Forgets the ticks enqueued more than REMEMBER_THRESHOLD before +time+.
      def remove_previous_enqueues(time)
        redis { |c| c.call("ZREMRANGEBYSCORE", key(job_enqueued_key), "-inf", "(#{time.to_f - REMEMBER_THRESHOLD}") }
      end

      # Enqueues the job if it's due at +time+ (what a poll does).
      def test_and_enqueue_for_time!(time)
        return unless should_enqueue?(time)

        enqueue!
        remove_previous_enqueues(time)
      end

      # Enqueues the job now, outside its schedule.
      def enqueue!(time = Time.now.utc)
        @last_enqueue_time = time
        klass = constant
        jid =
          if klass && is_active_job?
            job = klass.set(queue: @queue).perform_later(*enqueue_args)
            job.provider_job_id if job # rubocop:disable Style/SafeNavigation -- false (not nil) when not enqueued
          elsif klass
            options = {queue: queue_name_with_prefix}
            options[:retry] = retry_option(@retry) unless @retry.nil?
            klass.set(options).perform_async(*enqueue_args)
          else
            Client.push(@active_job ? active_job_message : sidekiq_worker_message)
          end
        save_last_enqueue_time
        add_jid_history(jid)
        Sidekiq.logger.debug { "enqueued #{@name}: #{@message}" }
      end

      def is_active_job?(klass = nil) # rubocop:disable Naming/PredicatePrefix -- sidekiq-cron's name
        return true if @active_job

        base = active_job_base
        klass ||= constant
        (!base.nil? && klass.instance_of?(Class) && klass < base) || false
      end

      def date_as_argument? = @date_as_argument

      # The arguments for the job enqueued now: its args (with the time,
      # with date_as_argument), GlobalIDs found.
      def enqueue_args
        args = date_as_argument? ? @args + [Time.now.to_f] : @args
        deserialize_argument(args)
      end

      # What's pushed for a job of a class this process can't load.
      def sidekiq_worker_message
        message = @message.instance_of?(String) ? Sidekiq.load_json(@message) : @message.dup
        message["retry"] = retry_option(message.fetch("retry")) if message.key?("retry")
        message.merge({"args" => enqueue_args})
      end

      # An Active Job's queue: its queue_name_prefix (and delimiter), or
      # ActiveJob::Base's, then the queue.
      def queue_name_with_prefix
        return @queue unless is_active_job?

        delimiter = @active_job_queue_name_delimiter.to_s
        delimiter = active_job_base&.queue_name_delimiter.to_s if delimiter.empty?
        delimiter = "_" if delimiter.empty?
        prefix = @active_job_queue_name_prefix.to_s
        prefix = active_job_base&.queue_name_prefix.to_s if prefix.empty?
        prefix.empty? ? @queue : "#{prefix}#{delimiter}#{@queue}"
      end

      # What's pushed for an Active Job of a class this process can't load.
      def active_job_message
        {"class" => "ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper", "wrapped" => @klass,
         "queue" => @queue_name_with_prefix, "description" => @description,
         "args" => [{"job_class" => @klass, "job_id" => SecureRandom.uuid, "queue_name" => @queue_name_with_prefix,
                     "arguments" => enqueue_args}]}
      end

      # The latest tick before +now+.
      def last_time(now = Time.now) = parsed_cron.previous_time(now)

      def formatted_enqueue_time(now = Time.now) = last_time(now).to_f.to_s

      def formatted_last_time(now = Time.now) = last_time(now).iso8601

      # -- state --------------------------------------------------------------

      def disable!
        @status = "disabled"
        save
      end

      def enable!
        @status = "enabled"
        save
      end

      def enabled? = @status == "enabled"

      def disabled? = !enabled?

      def pretty_message
        JSON.pretty_generate(Sidekiq.load_json(message))
      rescue JSON::ParserError, TypeError
        message
      end

      # The schedule in words, e.g. "every 5 minutes".
      def human_cron
        parsed_cron.description
      rescue ArgumentError
        cron
      end

      def status_from_redis
        (fetch_missing_args && redis { |c| c.call("HGET", key(redis_key), "status") }) || "enabled"
      end

      def last_enqueue_time_from_redis
        stored = redis { |c| c.call("HGET", key(redis_key), "last_enqueue_time") } if fetch_missing_args
        parse_enqueue_time(stored) unless stored.to_s.empty?
      rescue ArgumentError
        nil
      end

      # The latest enqueues ({"jid", "enqueued"} each), newest first.
      def jid_history_from_redis
        redis { |c| c.call("LRANGE", key(jid_history_key), 0, -1) }.map { |raw| Sidekiq.load_json(raw) }
      end

      # The job as stored in Redis.
      def to_hash
        {name: @name, namespace: @namespace, klass: @klass.to_s, cron: @cron, description: @description,
         source: @source, args: @args.instance_of?(String) ? @args : Sidekiq.dump_json(@args),
         date_as_argument: date_as_argument? ? "1" : "0",
         message: @message.instance_of?(String) ? @message : Sidekiq.dump_json(@message || {}),
         status: @status, active_job: @active_job ? "1" : "0", queue_name_prefix: @active_job_queue_name_prefix,
         queue_name_delimiter: @active_job_queue_name_delimiter,
         retry: (@retry.nil? || @retry.is_a?(Numeric)) ? @retry : @retry.to_s,
         last_enqueue_time: serialized_last_enqueue_time, symbolize_args: @symbolize_args ? "1" : "0"}
      end

      def errors = @errors ||= []

      def valid?
        @errors = validation_errors
        @errors.empty?
      end

      def klass_valid
        case @klass
        when Class then true
        when String then !@klass.empty?
        else false
        end
      end

      # Saves the job if it's valid; returns whether it was. A new job's
      # first tick is the one after now.
      def save
        return false unless valid?

        new_job = redis do |c|
          c.call("SADD", key(self.class.jobs_key(@namespace)), redis_key)
          c.call("HSET", key(redis_key), *to_hash.flatten)
          c.call("EXISTS", key(job_enqueued_key)).zero? &&
            c.call("ZADD", key(job_enqueued_key), Time.now.to_f, formatted_last_time)
        end
        Sidekiq.logger.info { "Cron Jobs - added job with name #{@name} in the namespace #{@namespace}" } if new_job
        true
      end

      def save_last_enqueue_time
        redis { |c| c.call("HSET", key(redis_key), "last_enqueue_time", serialized_last_enqueue_time) }
      end

      # Records an enqueue, keeping cron_history_size of them.
      def add_jid_history(jid)
        entry = Sidekiq.dump_json({"jid" => jid, "enqueued" => @last_enqueue_time})
        redis do |c|
          c.call("LPUSH", key(jid_history_key), entry)
          c.call("LTRIM", key(jid_history_key), 0, Integer(Cron.configuration.cron_history_size) - 1)
        end
      end

      def destroy
        redis do |c|
          c.call("SREM", key(self.class.jobs_key(@namespace)), redis_key)
          c.call("DEL", key(job_enqueued_key), key(jid_history_key), key(redis_key))
        end
        Sidekiq.logger.info { "Cron Jobs - deleted job with name #{@name} from namespace #{@namespace}" }
      end

      def exists? = self.class.exists?(@name, @namespace)

      # Enabled jobs first, then by name.
      def sort_name = "#{enabled? ? 0 : 1}_#{name}".downcase

      def args=(args)
        @args = parse_args(args)
      end

      def cron_expression_string = parsed_cron.to_s

      def redis_key = self.class.redis_key(@name, @namespace)

      def job_enqueued_key = self.class.job_enqueued_key(@name, @namespace)

      def jid_history_key = self.class.jid_history_key(@name, @namespace)

      private

      def redis(&) = Cron.redis(&)

      def key(name) = "#{Cron.prefix}#{name}"

      def parsed_cron = @parsed_cron ||= Cron.parse(@cron)

      def flag?(value) = TRUE_FLAG.match?(value.to_s)

      def namespace_for(namespace)
        default = Cron.configuration.default_namespace
        namespace ||= default
        return namespace unless Namespace.available_namespaces_provided? && !Namespace.all.include?(namespace)

        Sidekiq.logger.warn { "Cron Jobs - unexpected namespace #{namespace} encountered. Assigning to default namespace." }
        default
      end

      def validation_errors
        problems = []
        problems << "'name' must be set" if @name.to_s.empty?
        problems << "'namespace' must be set" if @namespace.to_s.empty?
        problems << "'namespace' cannot be '*'" if @namespace == "*"
        if @cron.to_s.empty?
          problems << "'cron' must be set"
        else
          begin
            @parsed_cron = Cron.parse(@cron)
          rescue ArgumentError => e
            problems << "'cron' -> #{@cron.inspect} -> #{e.class}: #{e}"
          end
        end
        problems << "'klass' (or class) must be set" unless klass_valid
        problems
      end

      # The job's message (what's pushed) and queue: a stored message's, or
      # the class's options with this job's queue, retry and args.
      def build_message(attrs)
        message = attrs["message"]
        if message
          @message = message
          data = (@message.instance_of?(String) ? Sidekiq.load_json(@message) : @message) || {}
          @queue = data["queue"] || "default"
          @retry = data["retry"]
        elsif @klass
          data = job_options.merge({"class" => @klass.to_s, "args" => @args})
          queue = attrs["queue"]
          data["queue"] = queue if queue
          @queue = data["queue"] || "default"
          retry_value = attrs["retry"]
          data["retry"] = retry_value unless retry_value.nil?
          @retry = data["retry"]
          @message = data
        end
      end

      # The class's queue and other options (Sidekiq's), or the default
      # queue for a class this process can't load.
      def job_options
        klass = constant
        if klass.nil?
          {"queue" => "default"}
        elsif is_active_job?
          {"queue" => klass.new(*@args).queue_name}
        else
          klass.respond_to?(:get_sidekiq_options) ? klass.get_sidekiq_options : {}
        end
      end

      def constant = Support.safe_constantize(@klass.to_s)

      # "retry" as stored ("true", "5"...) as a job takes it.
      def retry_option(value)
        case value
        when "true" then true
        when "false" then false
        when /\A\d+\z/ then Integer(value)
        else value
        end
      end

      def past_scheduled_time?(now) = Integer(now) - Integer(last_time(now)) > Cron.configuration.reschedule_grace_period

      def enqueued_after?(time) = !@last_enqueue_time.nil? && Integer(@last_enqueue_time) >= Integer(last_time(time))

      def serialized_last_enqueue_time = @last_enqueue_time&.strftime(LAST_ENQUEUE_TIME_FORMAT)

      def parse_enqueue_time(timestamp) = Time.parse(timestamp).utc

      # Arguments as given (Redis's JSON, an Array, a Hash, or one value)
      # as an Array, GlobalIDs stored as such.
      def parse_args(args)
        case args
        when String then parse_json_args(args)
        when Hash then symbolize_args([serialize_argument(args)])
        when Array then symbolize_args(serialize_argument(args))
        else global_id?(args) ? [serialize_argument(args)] : Array(args)
        end
      end

      def parse_json_args(args)
        symbolize_args(Sidekiq.load_json(args))
      rescue JSON::ParserError
        [args]
      end

      # Hash keys as Symbols, one level deep, with symbolize_args.
      def symbolize_args(args)
        return args unless @symbolize_args
        return args.transform_keys(&:to_sym) if args.instance_of?(Hash)
        return args unless args.instance_of?(Array)

        args.map { |arg| arg.instance_of?(Hash) ? arg.transform_keys(&:to_sym) : arg }
      end

      def global_id?(value)
        identification = global_id_identification
        !identification.nil? && value.is_a?(identification)
      end

      # ActiveJob::Base and GlobalID::Identification, or nil when they
      # aren't loaded.
      # mutant:disable -- loaded for every test at once under mutant (the
      # tests stub these to cover both)
      def active_job_base = defined?(::ActiveJob::Base) && ::ActiveJob::Base

      # mutant:disable -- see active_job_base
      def global_id_identification = defined?(::GlobalID::Identification) && ::GlobalID::Identification

      def serialize_argument(argument)
        case argument
        when Array then argument.map { |arg| serialize_argument(arg) }
        when Hash then argument.transform_values { |value| serialize_argument(value) }
        else global_id?(argument) ? {GLOBALID_KEY => argument.to_global_id.to_s} : argument
        end
      end

      def deserialize_argument(argument)
        case argument
        when Array then argument.map { |arg| deserialize_argument(arg) }
        when Hash
          if argument.size == 1 && argument.key?(GLOBALID_KEY)
            GlobalID::Locator.locate(argument.fetch(GLOBALID_KEY))
          else
            argument.transform_values { |value| deserialize_argument(value) }
          end
        else argument
        end
      end
    end
  end
end
