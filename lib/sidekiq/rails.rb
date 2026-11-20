# frozen_string_literal: true

# Robin's Rails integration does the rest of the Rails wiring; loaded now,
# so its Railtie joins the app's boot even if robin was loaded first.
require "robin/rails"

module Sidekiq
  # Sets up Active Job's :sidekiq adapter name and sidekiq_options on
  # Active Job classes.
  class Rails < ::Rails::Railtie
    # Before Active Job looks up the configured adapter. Loaded right away
    # (without loading ActiveJob::Base), so a server can run jobs naming
    # Sidekiq's wrapper classes before anything loads Active Job.
    initializer "sidekiq.active_job", before: "active_job.set_configs" do
      require "sidekiq/active_job" if defined?(::ActiveJob::Railtie)
    end
  end
end
