# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `bundle exec sidekiq` boots a Rails app with Active Job's `:sidekiq`
  adapter. It loaded Rails's own adapter instead, which failed with
  "sidekiq is not part of the bundle".

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
