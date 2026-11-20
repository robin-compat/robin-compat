# frozen_string_literal: true

require "delegate"

module Sidekiq
  module Middleware
    # A Robin middleware chain with the methods Sidekiq's chains add.
    class Chain < SimpleDelegator
      # Robin's chain is replaced on every change, so its methods return
      # it; these return this wrapper instead, for chaining calls.
      %i[add prepend insert_before insert_after remove clear].each do |name|
        define_method(name) do |*args|
          __getobj__.public_send(name, *args)
          self
        end
      end

      def include?(klass) = exists?(klass)

      # The middleware instances, built (or reused, for stateless ones) as
      # a job would get them.
      def retrieve = map(&:build)

      def copy_for(_capsule) = self
    end
  end
end
