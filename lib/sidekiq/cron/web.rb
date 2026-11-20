# frozen_string_literal: true

require "sidekiq/cron"

# sidekiq-cron's Web UI tab: Robin's dashboard has no page for these jobs
# (Sidekiq::Cron::Job.all lists them).
Robin.logger.warn("sidekiq/cron/web: Robin's dashboard doesn't show sidekiq-cron jobs; Sidekiq::Cron::Job.all lists them")
