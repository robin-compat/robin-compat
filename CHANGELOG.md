# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Job classes, their jobs and Active Job classes have
  `sidekiq_options_hash`, the class's options, which Sidekiq keeps in
  that class attribute. Apps' tests that checked an option with it
  failed with "undefined method".
- `Sidekiq::Config.new` takes no arguments, or Sidekiq's options, as
  Sidekiq's does, for a configuration of its own with Sidekiq's
  defaults. It required a Robin::Config, so apps' tests that built one
  failed with "wrong number of arguments".
- `require "sidekiq-scheduler/web"` loads `Sidekiq::Web`, as
  sidekiq-scheduler's does. Routes that required only it (Mastodon's do)
  failed to mount the dashboard with "uninitialized constant
  Sidekiq::Web".
- `bundle exec sidekiq` boots a Rails app with Active Job's `:sidekiq`
  adapter. It loaded Rails's own adapter instead, which failed with
  "sidekiq is not part of the bundle".
- `gem "robin-compat"` loads Pro's and Enterprise's interfaces, as Bundler
  loaded sidekiq-pro and sidekiq-ent when they were in the Gemfile.
  `Sidekiq::Client.reliable_push!` and `Sidekiq::Batch` were undefined
  in an initializer unless it required `sidekiq-pro`.

### Changed

- robin-compat is versioned on its own, no longer in step with robin. It
  depends on robin from 0.1.2 up to (not including) 0.2, so a release of
  either no longer needs one of the other.
- The README warns about gems that depend on the sidekiq gem, which bring
  the real Sidekiq back into the bundle.

## [0.1.2] - 2026-11-22

The first release, alongside Robin 0.1.2. See README.md.

### Added

- A Sidekiq-compatible interface (Sidekiq's OSS, Pro and Enterprise APIs)
  on Robin, working on Sidekiq's Redis data in place.
- Support for apps written for Sidekiq 8.0 (and Pro and Enterprise 8.0) as
  well as 8.1, following 8.0's rules where the two differ.
- sidekiq-cron's interface (`Sidekiq::Cron::Job`, `config/schedule.yml`),
  working on sidekiq-cron's jobs in Redis in place.
- sidekiq-scheduler's interface (the `:scheduler:` section of
  `config/sidekiq.yml`, `Sidekiq.schedule=`, `Sidekiq::Scheduler`),
  working on sidekiq-scheduler's schedule in Redis in place.
- Support for apps written for Sidekiq 7.0 to 7.3:
  `Sidekiq.compatible_with` names the version, and the forms Sidekiq 8
  dropped (error handlers with two parameters, `Sidekiq::Work` read as a
  Hash, nested testing modes) work whatever it is.
