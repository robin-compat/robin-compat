# frozen_string_literal: true

require "sidekiq/cron"

module Sidekiq
  module Cron
    # Loads config.cron_schedule_file (config/schedule.yml, or .yaml):
    # YAML, after ERB, of name => job attributes, or of a list of job
    # attributes. Its jobs are the schedule's: loading it again destroys
    # the ones it no longer has.
    class ScheduleLoader
      # Saves the schedule's jobs; returns the invalid ones' errors.
      def load_schedule
        case schedule
        when Hash then Job.load_from_hash!(schedule, source: "schedule")
        when Array then Job.load_from_array!(schedule, source: "schedule")
        else raise "Not supported schedule format. Confirm your #{schedule_file_name}"
        end
      end

      def has_schedule_file? = File.exist?(schedule_file_name) # rubocop:disable Naming/PredicatePrefix -- sidekiq-cron's name

      private

      def schedule
        @schedule ||= begin
          require "erb"
          require "yaml"
          YAML.safe_load(ERB.new(File.read(schedule_file_name)).result, permitted_classes: [Symbol], aliases: true)
        end
      end

      def schedule_file_name
        @schedule_file_name ||= begin
          name = Cron.configuration.cron_schedule_file
          File.exist?(name) ? name : name.sub(/\.yml\z/, ".yaml")
        end
      end
    end
  end
end
