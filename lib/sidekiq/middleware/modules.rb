# frozen_string_literal: true

module Sidekiq
  # Helpers for middleware classes: +config+, +logger+, +redis+ and
  # +redis_pool+ (the configuration is set when the middleware is built).
  module ServerMiddleware
    include Robin::Middleware::ConfigAware

    def redis(&) = Sidekiq.redis(&)

    def redis_pool = Sidekiq.redis_pool
  end

  ClientMiddleware = ServerMiddleware
end
