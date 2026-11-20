# frozen_string_literal: true

require "sidekiq"
require "sidekiq-scheduler/version"
require "sidekiq-scheduler/redis_manager"
require "sidekiq-scheduler/utils"
require "sidekiq-scheduler/config"
require "sidekiq-scheduler/schedule"
require "sidekiq-scheduler/scheduled_job"
require "sidekiq-scheduler/scheduler"
require "sidekiq-scheduler/poller"
require "sidekiq-scheduler/manager"

# sidekiq-scheduler's interface, on Robin: the :scheduler: section of
# config/sidekiq.yml, Sidekiq.schedule= and friends, and
# Sidekiq::Scheduler.
#
#   # config/sidekiq.yml
#   :scheduler:
#     :schedule:
#       hello_world:
#         cron: "0 * * * * *"
#         class: HelloWorld
#
# The schedule is kept in Redis as sidekiq-scheduler keeps it. Only Robin's
# leader pushes the jobs, and it claims each cron tick in Redis the way
# sidekiq-scheduler does, so the two can run side by side while you switch.
module SidekiqScheduler
  class << self
    # What a server does as it starts: schedules the jobs and pushes them
    # as they come due, until it quiets or stops. +sidekiq_config+ is the
    # server's Sidekiq::Config.
    def boot(sidekiq_config)
      RedisManager.clean_schedules_changed
      manager = Manager.new(Config.new(sidekiq_config: sidekiq_config))
      sidekiq_config[:schedule_manager] = manager
      manager.start
      sidekiq_config.on(:quiet) { manager.stop }
      sidekiq_config.on(:shutdown) { manager.stop }
    end
  end
end

module Sidekiq
  Scheduler = SidekiqScheduler::Scheduler
end

Sidekiq.extend SidekiqScheduler::Schedule
