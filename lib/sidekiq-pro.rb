# frozen_string_literal: true

# Sidekiq Pro's interface: batches, reliable push and fetch, expiring jobs,
# queue pausing and metrics. Robin has all of these built in.
require "sidekiq"
require "sidekiq/batch"

module Sidekiq
  class Client
    # Buffers pushes in memory while Redis is unreachable and sends them
    # once it's back (Robin's reliable push, 1,000 jobs at most).
    def self.reliable_push!(config = Robin.config)
      config.reliable_push = true
    end
  end
end
