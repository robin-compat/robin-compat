# frozen_string_literal: true

# Loaded by mutant (see mutant.yml) so every subject is defined before it
# starts matching.
require "sidekiq"
require "sidekiq/api"
require "sidekiq/web"
require "sidekiq/cli"
require "sidekiq/deploy"
require "sidekiq-ent"
require "sidekiq/pro/metrics"
require "rails"
require "active_job/railtie"
require "sidekiq/rails"
require "sidekiq/active_job"
require "sidekiq/middleware/current_attributes"
require "sidekiq/testing"
require "sidekiq/testing/inline"
# Loading the testing API turned fake mode on; tests switch it on themselves.
Sidekiq::Testing.disable!
Robin::Client.interceptor = nil
