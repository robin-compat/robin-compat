# frozen_string_literal: true

require_relative "test_helper"

class MiddlewareChainTest < RobinCompatTest
  cover "Sidekiq::Middleware::Chain*"
  cover "Sidekiq::ServerMiddleware*"

  class A
    def call(*) = yield
  end

  class B < A; end

  class Shared
    include Robin::Middleware::Stateless

    def call(*) = yield
  end

  class Helper
    include Sidekiq::ServerMiddleware

    def call(*) = yield
  end

  def chain = @chain ||= Sidekiq::Middleware::Chain.new(Robin::Middleware::Chain.new(config))

  def test_changes_return_the_wrapper
    assert_same chain, chain.add(A)
    assert_same chain, chain.prepend(B)
    assert_same chain, chain.insert_before(A, Shared)
    assert_same chain, chain.insert_after(A, Helper)
    assert_equal [B, Shared, A, Helper], chain.to_a.map(&:klass)
    assert_same chain, chain.remove(B)
    assert_true chain.include?(A)
    assert_false chain.include?(B)
    assert_same chain, chain.copy_for(:capsule)
    assert_same chain, chain.clear
    assert_empty chain.to_a
  end

  def test_retrieve_builds_the_middleware
    chain.add(A).add(Shared)
    first = chain.retrieve
    second = chain.retrieve

    assert_equal [A, Shared], first.map(&:class)
    refute_same first.first, second.first
    assert_same first.last, second.last
  end

  def test_server_middleware_helpers
    helper = Helper.new
    helper.config = config

    assert_same config, helper.config
    assert_same config.logger, helper.logger
    assert_equal "PONG", helper.redis(&:ping)
    assert_instance_of Sidekiq::RedisConnection::Pool, helper.redis_pool
    assert_same Sidekiq::ServerMiddleware, Sidekiq::ClientMiddleware
  end
end

class LoggerAndRetryTest < RobinCompatTest
  cover "Sidekiq::Context*"
  cover "Sidekiq::JobRetry*"

  def test_context
    seen = Sidekiq::Context.with(jid: "j") do
      Sidekiq::Context.add(:extra, 1)
      Sidekiq::Context.current.dup
    end

    assert_equal({jid: "j", extra: 1}, seen)
    assert_empty Sidekiq::Context.current
  ensure
    Thread.current[Robin::Context::KEY] = nil
  end

  def test_formatters_and_retry_exceptions
    assert_same Robin::JobLogger::Formatter, Sidekiq::Logger::Formatters::Pretty
    assert_same Robin::JobLogger::Formatter, Sidekiq::Logger::Formatters::WithoutTimestamp
    assert_same Robin::JobLogger::JSONFormatter, Sidekiq::Logger::Formatters::JSON
    assert_same Robin::Discard, Sidekiq::JobRetry::Skip
    assert_operator Sidekiq::JobRetry::Handled, :<, RuntimeError
  end
end

module IterableTestJobs
  class Rows
    include Sidekiq::IterableJob

    sidekiq_options queue: "rows"

    def build_enumerator(rows, cursor:) = array_enumerator(rows, cursor: cursor)

    def each_iteration(row, _rows) = (@seen ||= []) << row

    attr_reader :seen
  end
end

class IterableJobTest < RobinCompatTest
  cover "Sidekiq::Job::Iterable*"

  def test_sidekiq_iterable_jobs_are_robin_iterable_jobs
    assert_same Sidekiq::Job::Iterable, Sidekiq::IterableJob
    assert_includes IterableTestJobs::Rows.ancestors, Robin::IterableJob
    assert_equal "rows", IterableTestJobs::Rows.get_sidekiq_options["queue"]
    assert_operator Sidekiq::Job::Interrupted, :<, RuntimeError
    klass = Class.new { include Sidekiq::IterableJob }
    ancestors = klass.ancestors

    assert_operator ancestors.index(Sidekiq::Job::InstanceMethods), :<, ancestors.index(Robin::IterableJob)
    assert_operator ancestors.index(Robin::IterableJob), :<, ancestors.index(Sidekiq::Job::Iterable)
    assert_includes ancestors, Robin::Job
    assert_includes klass.singleton_class.ancestors, Sidekiq::Job::ClassMethods
  end

  def test_helpers
    job = IterableTestJobs::Rows.new

    assert_nil job.arguments
    job.payload = {"args" => [[1, 2]]}

    assert_equal [[1, 2]], job.arguments
    assert_equal :ok, job.around_iteration { :ok }
    job.jid = "it1"
    job.cancel!

    assert Robin::Cancellation.cancelled?("it1", config: config)
  end

  def test_csv_batches_enumerator
    job = IterableTestJobs::Rows.new
    rows = (1..5).zip

    assert_equal [[[[1], [2]], 0], [[[3], [4]], 1], [[[5]], 2]], job.csv_batches_enumerator(rows, cursor: nil, batch_size: 2).to_a
    assert_equal [[[[5]], 2]], job.csv_batches_enumerator(rows, cursor: 1, batch_size: 2).to_a
    assert_equal [100, 1], job.csv_batches_enumerator((1..101).zip, cursor: nil).map { |batch, _| batch.size }
  end

  def test_active_record_relations_enumerator
    relation = FakeRelation.new((1..5).to_a)
    job = IterableTestJobs::Rows.new
    batches = job.active_record_relations_enumerator(relation, cursor: nil, batch_size: 2).to_a

    assert_equal [[[1, 2], 2], [[3, 4], 4], [[5], 5]], batches.map { |rel, last| [rel.ids, last] }
    assert_equal [[[5], 5]], job.active_record_relations_enumerator(relation, cursor: 4, batch_size: 2).to_a.map { |rel, last| [rel.ids, last] }
    many = FakeRelation.new((1..101).to_a)

    assert_equal [[100, 100], [1, 101]], job.active_record_relations_enumerator(many, cursor: nil).map { |rel, last| [rel.ids.size, last] }
  end

  # Just enough of an ActiveRecord relation for the batch enumerators:
  # conditions and limits apply when it's loaded, as in SQL.
  class FakeRelation
    Record = Struct.new(:id)

    def initialize(ids, after: nil, limit: nil)
      @ids = ids
      @after = after
      @limit = limit
    end

    def ids = to_a.map(&:id)

    def primary_key = "id"

    def where(conditions)
      return FakeRelation.new(@ids & conditions.fetch("id"), after: @after, limit: @limit) if conditions.is_a?(Hash)

      FakeRelation.new(@ids, after: conditions.last, limit: @limit)
    end

    def reorder(*) = self

    def limit(count) = FakeRelation.new(@ids, after: @after, limit: count)

    def to_a
      ids = @after ? @ids.select { |id| id > @after } : @ids
      (@limit ? ids.first(@limit) : ids).map { |id| Record.new(id) }
    end

    def arel_table = self

    def [](_column) = Column.new

    # Builds the "id > cursor" condition.
    class Column
      def gt(value) = [:gt, value]
    end
  end
end
