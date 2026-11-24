# frozen_string_literal: true

source "https://rubygems.org"

gemspec

# The newest released robin robin-compat allows, unless ROBIN_PATH names a
# checkout (for development) or ROBIN_VERSION a release (CI's lowest-robin
# job sets it).
if ENV["ROBIN_PATH"]
  gem "robin", path: ENV["ROBIN_PATH"]
elsif ENV["ROBIN_VERSION"]
  gem "robin", ENV["ROBIN_VERSION"]
end

gem "rake"
gem "minitest"
gem "minitest-strict"
gem "minitest-memory", ">= 1.1.1"
gem "rack"
gem "rack-test"
gem "rack-session"
gem "tzinfo"
gem "fugit"
gem "activejob", ">= 7.2"
gem "railties", ">= 7.2"
gem "standard", ">= 1.57"
gem "standard-performance"
gem "rubocop"
gem "rubocop-performance"
gem "rubocop-minitest"
gem "simplecov", ">= 1.3"

platforms :mri do
  gem "mutant-minitest"
end
