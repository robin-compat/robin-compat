# frozen_string_literal: true

require "sidekiq"
require "robin/rails/current_attributes"

module Sidekiq
  # Carries ActiveSupport::CurrentAttributes into jobs:
  #
  #   Sidekiq::CurrentAttributes.persist("Current")
  #   Sidekiq::CurrentAttributes.persist(["Current", "Tenant::Current"])
  #
  # They're kept in the job as Sidekiq keeps them ("cattr", "cattr_1"...),
  # so jobs pushed by either run with them.
  module CurrentAttributes
    def self.persist(klass_or_array, config = Sidekiq.default_configuration)
      Robin::Rails::CurrentAttributes.install(config, klass_or_array)
    end
  end
end
