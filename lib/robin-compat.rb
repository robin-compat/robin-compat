# frozen_string_literal: true

# What Bundler.require loads for `gem "robin-compat"`: Sidekiq's interface
# with Pro's and Enterprise's, as Bundler loaded sidekiq-pro and sidekiq-ent
# when they were in the Gemfile. sidekiq-cron's and sidekiq-scheduler's
# load when they're used (see sidekiq.rb).
require "sidekiq-ent"
