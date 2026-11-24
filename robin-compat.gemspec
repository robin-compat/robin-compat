# frozen_string_literal: true

require_relative "lib/robin/compat/version"

Gem::Specification.new do |spec|
  spec.name = "robin-compat"
  spec.version = Robin::Compat::VERSION
  spec.authors = ["Robin Compat"]
  spec.email = ["robin-compat@proton.me"]
  spec.summary = "A Sidekiq-compatible interface for Robin"
  spec.description = "Sidekiq-compatible: runs apps written for Sidekiq on Robin, unchanged. " \
    "It provides Sidekiq's job, client, configuration, testing, data, web and " \
    "command-line interfaces, and the Pro and Enterprise ones, on top of Robin, which " \
    "works on Sidekiq's Redis data in place, so switching needs no draining. Not " \
    "affiliated with or endorsed by Contributed Systems LLC; Sidekiq is their trademark."
  spec.homepage = "https://github.com/robin-compat/robin-compat"
  spec.license = "LGPL-3.0-only"
  spec.required_ruby_version = ">= 3.4"

  spec.metadata = {
    "homepage_uri" => spec.homepage,
    "source_code_uri" => spec.homepage,
    "changelog_uri" => "#{spec.homepage}/blob/main/CHANGELOG.md",
    "rubygems_mfa_required" => "true"
  }

  spec.files = Dir["lib/**/*", "exe/*", "LICENSE", "COPYING", "README.md", "CHANGELOG.md"].select { |f| File.file?(f) }
  spec.bindir = "exe"
  spec.executables = %w[sidekiq sidekiqswarm]
  spec.require_paths = ["lib"]

  spec.add_dependency "robin", *Robin::Compat::ROBIN_REQUIREMENT
end
