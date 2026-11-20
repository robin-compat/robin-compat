# frozen_string_literal: true

module Sidekiq
  # Sidekiq's job logging context: Context.with(key: value) { ... } adds
  # to every line logged inside the block; Context.add, to the rest of the
  # running job's.
  module Context
    def self.with(hash, &) = Robin::Context.with(hash, &)

    def self.current = Robin::Context.current

    def self.add(key, value)
      current[key] = value
    end
  end

  # Log formatters under Sidekiq's names.
  module Logger
    module Formatters
      Pretty = Robin::JobLogger::Formatter
      JSON = Robin::JobLogger::JSONFormatter
      WithoutTimestamp = Robin::JobLogger::Formatter
    end
  end
end
