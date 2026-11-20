# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

# `rake release` builds the gem, tags the release and pushes the tag. The
# push workflow (.github/workflows/push.yml) then signs the gem and pushes
# it to RubyGems.org, with an attestation, from CI, as the robin-compat
# account (trusted publishing: no API key here).
ENV["gem_push"] = "no"

# Who robin-compat is released as: its tags, its commits and the repo the
# tags go to name this identity and no other.
RELEASE_EMAIL = "robin-compat@proton.me"
RELEASE_REPO = "robin-compat/robin-compat"
RELEASE_REMOTE = %r{[/:]#{Regexp.escape(RELEASE_REPO)}(\.git)?\z}

desc "Check that the release tag and every commit name #{RELEASE_EMAIL} only, unsigned, and go to #{RELEASE_REPO}"
task "release:guard_identity" do
  git = ->(*args) { IO.popen(["git", *args], err: File::NULL, &:read).strip }
  problems = []

  email = git.call("config", "user.email")
  problems << "git's user.email is #{email.inspect}, not #{RELEASE_EMAIL}" unless email == RELEASE_EMAIL
  %w[commit.gpgsign tag.gpgsign].each do |key|
    problems << "#{key} is on: a signature names its key" if git.call("config", "--bool", key) == "true"
  end
  origin = git.call("remote", "get-url", "origin")
  problems << "origin is #{origin.inspect}, not #{RELEASE_REPO}" unless origin.match?(RELEASE_REMOTE)
  others = git.call("log", "--format=%ae%n%ce").lines(chomp: true).uniq - [RELEASE_EMAIL]
  problems << "commits name #{others.join(", ")}" unless others.empty?

  abort "Not releasing:\n#{problems.map { |problem| "  - #{problem}" }.join("\n")}" unless problems.empty?
end
task "release:guard_clean" => "release:guard_identity"

Rake::TestTask.new(:test) do |t|
  t.libs << "test" << "lib"
  t.test_files = FileList["test/*_test.rb"]
  t.warning = false
end

task default: :test
