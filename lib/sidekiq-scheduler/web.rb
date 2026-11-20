# frozen_string_literal: true

require "sidekiq-scheduler"

# sidekiq-scheduler's Web UI tab: Robin's dashboard has no page for these
# jobs (Sidekiq.get_schedule lists them).
Robin.logger.warn("sidekiq-scheduler/web: Robin's dashboard doesn't show sidekiq-scheduler jobs; Sidekiq.get_schedule lists them")
