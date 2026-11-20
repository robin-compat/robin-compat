# frozen_string_literal: true

require "robin"

module Robin
  module Compat
    # Cron lines as sidekiq-cron and sidekiq-scheduler take them (through
    # fugit): five fields, or six with seconds first, and an optional time
    # zone at the end ("0 22 * * 1-5 America/Chicago"). Without one, a line
    # is in Rails' Time.zone, else the system's.
    module CronLine
      # A time zone at the end of a cron line: an IANA name
      # ("America/Chicago", "Etc/GMT+5"), UTC, GMT, Z or an offset ("+05:30").
      ZONE = %r{\A([A-Za-z][\w+-]*(/[\w+-]+)+|UTC|GMT|Z|[+-]\d\d:\d\d)\z}
      private_constant :ZONE

      class << self
        # The line as a Robin::Cron.
        def parse(line)
          *fields, last = line.split
          return Cron.new(fields.join(" "), tz: last) if ZONE.match?(last)

          Cron.new(line, tz: default_zone)
        end

        # Rails' Time.zone's name, or nil (the system's zone).
        def default_zone = rails_time_zone&.tzinfo&.name

        # Fugit, or nil when the app doesn't have the gem.
        def fugit
          require "fugit"
          Fugit
        rescue LoadError
          nil
        end

        private

        # Rails' Time.zone, or nil without Active Support's.
        # mutant:disable -- loaded for every test at once under mutant (the
        # tests stub this to cover both)
        def rails_time_zone = (::Time.respond_to?(:zone) && ::Time.zone) || nil
      end
    end
  end
end
