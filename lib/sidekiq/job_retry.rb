# frozen_string_literal: true

module Sidekiq
  # The exceptions apps rescue or raise around retries.
  class JobRetry
    # Raised (by Sidekiq) once a failure has been recorded for retry.
    class Handled < ::RuntimeError; end

    # Raise to stop a job without retrying it, as Robin::Discard does.
    Skip = Robin::Discard
  end
end
