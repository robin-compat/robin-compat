# frozen_string_literal: true

module Robin
  module Compat
    VERSION = "0.1.2"

    # The robin versions robin-compat works with, released on their own:
    # from the lowest one CI tests (the lowest-robin job), up to robin's
    # next minor release, which may change the extension API robin-compat
    # builds on (robin's EXTENDING.md).
    ROBIN_REQUIREMENT = [">= 0.1.2", "< 0.2"].freeze
  end
end
