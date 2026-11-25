# frozen_string_literal: true

require "sidekiq-scheduler"
# As sidekiq-scheduler's does, so an app that requires only this file
# can mount Sidekiq::Web.
require "sidekiq/web"

# sidekiq-scheduler's Web UI tab: Robin's dashboard has no page for these
# jobs (Sidekiq.get_schedule lists them).
Robin.logger.warn("sidekiq-scheduler/web: Robin's dashboard doesn't show sidekiq-scheduler jobs; Sidekiq.get_schedule lists them")
