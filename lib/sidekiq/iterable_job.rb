# frozen_string_literal: true

module Sidekiq
  module Job
    # Raised when an iterable job is interrupted.
    class Interrupted < ::RuntimeError; end

    # Sidekiq's iterable jobs: build_enumerator, each_iteration and the
    # on_* hooks, run by Robin::IterableJob.
    #
    #   class ImportJob
    #     include Sidekiq::IterableJob
    #
    #     def build_enumerator(import_id, cursor:)
    #       active_record_records_enumerator(Import.find(import_id).rows, cursor: cursor)
    #     end
    #
    #     def each_iteration(row, import_id)
    #     end
    #   end
    module Iterable
      def self.included(base)
        base.include(Robin::IterableJob)
        base.include(InstanceMethods)
        base.extend(ClassMethods)
      end

      # The job's arguments.
      def arguments = payload&.fetch("args")

      # Stops the job at its next iteration (see Robin.cancel!).
      def cancel! = Robin.cancel!(jid)

      # Iterates ActiveRecord relations of +batch_size+ records, in
      # primary-key order; the cursor is the last finished batch's last id.
      def active_record_relations_enumerator(relation, cursor:, batch_size: 100)
        Enumerator.new do |y|
          active_record_batches_enumerator(relation, cursor: cursor, batch_size: batch_size).each do |records, last_id|
            y.yield relation.where({relation.primary_key => records.map(&:id)}), last_id
          end
        end
      end

      # Iterates a CSV in batches of +batch_size+ rows; the cursor is the
      # index of the last finished batch.
      def csv_batches_enumerator(csv, cursor:, batch_size: 100)
        Enumerator.new do |y|
          csv.each_slice(batch_size).with_index { |rows, i| y.yield rows, i if cursor.nil? || i > cursor }
        end
      end
    end
  end

  IterableJob = Job::Iterable
end
