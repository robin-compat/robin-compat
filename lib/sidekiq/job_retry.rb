# frozen_string_literal: true

module Sidekiq
  # The exceptions apps rescue or raise around retries.
  class JobRetry
    # Raised (by Sidekiq) once a failure has been recorded for retry.
    class Handled < ::RuntimeError; end

    # Raise to stop a job without retrying it, as Robin::Discard does.
    Skip = Robin::Discard

    # With "retry" true, a job retries at most config.max_retries times,
    # even with retry_for: it stops at whichever limit comes first. That's
    # Sidekiq 8.0's rule. Sidekiq 8.1, like Robin, lets retry_for alone set
    # the limit, which differs only when the window outlasts the retries.
    module CountLimit
      def max_retries(payload)
        (payload["retry"] == true) ? config.max_retries : super
      end
    end
    Robin::Retrier.prepend(CountLimit)
  end
end
