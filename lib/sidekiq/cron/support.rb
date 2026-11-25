# frozen_string_literal: true

require "sidekiq/cron"

module Sidekiq
  module Cron
    # sidekiq-cron's helpers, which apps call (Chatwoot parses its
    # schedule with load_yaml) and stub in tests (a job class this process
    # can't load, say). The cron jobs and the schedule loader go through
    # them, as sidekiq-cron's do.
    module Support
      # The constant named +name+, or nil when there's none.
      # mutant:disable -- `const_get` on Support instead of Object finds the
      # same constants, as Support has none of its own.
      def self.safe_constantize(name)
        Object.const_get(name)
      rescue NameError
        nil
      end

      # YAML as sidekiq-cron reads schedules: Symbols and aliases allowed.
      def self.load_yaml(source)
        require "yaml"
        YAML.safe_load(source, permitted_classes: [Symbol], aliases: true)
      end
    end
  end
end
