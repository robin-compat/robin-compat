# frozen_string_literal: true

source "https://rubygems.org"

gemspec

# Robin, from a checkout next to this one until it's released (ROBIN_PATH
# for another).
gem "robin", path: ENV.fetch("ROBIN_PATH", "../robin")

gem "rake"
gem "minitest"
gem "minitest-strict"
gem "minitest-memory", ">= 1.1.1"
gem "rack"
gem "rack-test"
gem "rack-session"
gem "tzinfo"
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
