# robin-compat

A Sidekiq-compatible interface for [Robin](https://github.com/sferik/robin):
run your Sidekiq app on Robin without changing its code. robin-compat
provides Sidekiq's interface on top of Robin: job classes,
`sidekiq_options`, the client, configuration, `config/sidekiq.yml`, the
`sidekiq` command, the testing API, the data API, `Sidekiq::Web`, and
Sidekiq Pro's and Enterprise's APIs. Robin reads and writes Sidekiq's
Redis data where it is, so you switch without draining any queue.

Robin and robin-compat are free software under the LGPL-3.0, including
the features Sidekiq sells as Pro and Enterprise.

It matches Sidekiq 7.0 to 8.1, with Pro and Enterprise, and
`Sidekiq::VERSION` is 8.1.0. Apps written for Sidekiq 7 say so (see
[Coming from Sidekiq 7](#coming-from-sidekiq-7)). Where 8.0 and 8.1
differ, it follows 8.0,
whose rules are looser: a job with `retry_for` also stops at its retry
count, and `perform_inline` takes arguments JSON would change (a Symbol
becomes a String) where 8.1 raises. It also takes the forms of Sidekiq 7
that 8.0 dropped: error handlers with two parameters (`->(error,
context)`), `Sidekiq::Work` read as a Hash (`work["queue"]`), and testing
modes nested in each other's blocks.

> robin-compat is an independent project. It is not affiliated with,
> sponsored by or endorsed by Contributed Systems LLC or Mike Perham.
> Sidekiq, Sidekiq Pro and Sidekiq Enterprise are trademarks of
> Contributed Systems LLC, and are named here only to say what
> robin-compat is compatible with. It contains none of Sidekiq's code.

## Upgrading from Sidekiq

### 1. Change your Gemfile

```diff
-gem "sidekiq"
+gem "robin-compat"
```

If you use Sidekiq Pro or Enterprise, remove them and their gem source
too:

```diff
-source "https://gems.contribsys.com/" do
-  gem "sidekiq-pro"
-  gem "sidekiq-ent"
-end
```

If you use sidekiq-cron or sidekiq-scheduler, remove them too, since they
depend on the real Sidekiq:

```diff
-gem "sidekiq-cron"
-gem "sidekiq-scheduler"
```

robin-compat provides their interfaces, and runs the jobs they saved in
Redis where they are. If your schedules use natural language ("every day
at five", or durations like "2 hours and 30 minutes"), add the fugit gem
to your Gemfile, which both brought in.

Then `bundle install`. That's the only change to your app. Everything else
keeps working as written:

- `require "sidekiq"`, `"sidekiq/web"`, `"sidekiq/api"`,
  `"sidekiq/testing"`, `"sidekiq-pro"`, `"sidekiq-ent"`
- job classes (`include Sidekiq::Job`, `Sidekiq::Worker`), `sidekiq_options`,
  `sidekiq_retry_in`, `sidekiq_retries_exhausted`, `perform_async`,
  `perform_in`, `perform_at`, `perform_bulk`, `set`, `perform_inline`
- `Sidekiq.configure_server` and `configure_client` blocks, server and
  client middleware, error and death handlers, lifecycle events
- `config/sidekiq.yml` (queues and weights, concurrency, capsules,
  environment sections, other settings) and `bundle exec sidekiq`
- Active Job's `:sidekiq` adapter, and `sidekiq_options` on Active Job
  classes
- `Sidekiq::Testing` (fake, inline and disabled modes, `jobs`, `drain`)
- `Sidekiq::Stats`, `Queue`, `ScheduledSet`, `RetrySet`, `DeadSet`,
  `ProcessSet`, `Workers`
- `Sidekiq::Deploy.mark!` (`require "sidekiq/deploy"`): the dashboard's
  metrics charts show each deploy
- `mount Sidekiq::Web => "/sidekiq"`, with `Sidekiq::Web.use` middleware
  (basic auth, say) and Enterprise's `Sidekiq::Web.authorize`
- `Thread.current[:sidekiq_capsule]` in jobs, and
  `Sidekiq::Client#normalize_item`
- Pro: batches and their callbacks (with `invalidate_all` and
  `invalidate_jids`), `reliable_push!`, `super_fetch!`, expiring jobs,
  queue pausing, StatsD metrics (`config.dogstatsd`)
- Enterprise: rate limiters, unique jobs, periodic jobs, encryption,
  leader election, `sidekiqswarm`
- sidekiq-cron: `Sidekiq::Cron::Job` (`create`, `find`, `all`, `destroy`,
  `enable!`, `disable!`, `enqueue!`, `load_from_hash!`...), namespaces,
  `Sidekiq::Cron.configure`, and `config/schedule.yml`, loaded as each
  server starts. Jobs sidekiq-cron and Robin enqueue claim each tick in
  Redis the same way, so the two can run side by side while you switch.
- sidekiq-scheduler: the `:scheduler:` section of `config/sidekiq.yml`
  (`cron`, `every`, `interval`, `at` and `in` schedules, `enabled`,
  `dynamic`, `listened_queues_only`), `Sidekiq.schedule=`,
  `Sidekiq.set_schedule`, `Sidekiq.get_schedule`,
  `Sidekiq::Scheduler.reload_schedule!` and its other methods, and the
  enabled states its web UI toggles. Robin claims each cron tick in Redis
  as sidekiq-scheduler does, so the two can run side by side too.

### Coming from Sidekiq 7

Robin needs Ruby 3.4 or later, so upgrade Ruby first if your app runs an
older one. Then tell robin-compat which Sidekiq your app was written for,
in an initializer every process loads (web servers and consoles push
jobs too, not just the `sidekiq` processes):

```ruby
# config/initializers/sidekiq.rb
Sidekiq.compatible_with = "7.2"
```

It takes any version from 7.0 to 8.1 ("7.2.4" works too) and decides
what depends on it:

- Before 8.0, jobs record their times (`created_at`, `enqueued_at`,
  `failed_at` and `retried_at`) in epoch seconds rather than
  milliseconds, and Active Job jobs name Sidekiq 7's wrapper class
  (`ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper`), so Sidekiq 7
  can run and show Robin's jobs if you go back. Pro's StatsD metrics
  have no `sidekiq.` prefix, as in Pro 7 (which leaves it to the client's
  `namespace`).
- Before 7.3, a Hash of arguments in a sidekiq-scheduler schedule has its
  keys symbolized, as sidekiq-scheduler 5 does with Active Support.
- Before 7.1, `push_bulk` returns only the jids of the jobs it pushed, as
  Sidekiq 7.0 does, not a `nil` for each job middleware stopped.

The forms Sidekiq 8 dropped work whatever the version: error handlers
with two parameters, `Sidekiq::Work` read as a Hash, and nested testing
modes.

### 2. Deploy

1. Stop your Sidekiq processes as you normally do (`TERM`) and let them
   exit: Sidekiq pushes the jobs they were running back to their queues.
2. Deploy.
3. Start Robin with the same command (`bundle exec sidekiq -C
   config/sidekiq.yml`, or `sidekiqswarm` with Enterprise). Your Procfile,
   systemd unit or Kubernetes manifest doesn't change.

Robin picks up where Sidekiq left off: enqueued jobs, scheduled jobs,
retries and dead jobs, paused queues, and the processed and failed
totals and their history. Run Sidekiq and Robin one after the other,
not side by side.

Robin keeps Sidekiq's data formats, so you can also go back: stop Robin,
deploy Sidekiq again, and Sidekiq picks up the same way (the interop tests
check this with the real Sidekiq). Active Job jobs name Sidekiq's wrapper
class for this reason. Features Sidekiq doesn't have, such as Robin's
encrypted or debounced jobs, are of course lost on Sidekiq.

### 3. That's it

Your app runs on Robin. Its dashboard is where Sidekiq's was, and the
processes show up in it (and in Sidekiq's own API, which reads the same
records) as they did before.

## What carries over, and what doesn't

These stay where Sidekiq keeps them, and Robin uses them as they are:
queues, the schedule, retry and dead sets, the paused queues, the job
totals, and process records. Each job keeps its arguments, retry count,
error details and backtrace.

State that Sidekiq Pro and Enterprise keep in their own formats doesn't
carry over:

- **Batches in progress.** Their jobs still run, but their callbacks won't
  fire. Let running batches finish before switching.
- **Unique job locks.** A duplicate of a job pushed just before the switch
  may be accepted once.
- **Rate limiter counts** start from zero.
- **Periodic jobs** are registered again from your configuration; their
  past runs aren't shown.
- **Encrypted jobs** (Enterprise) can't be decrypted by Robin, which
  encrypts differently. Let them finish before switching.
- **Historical metrics** charts, and the deploy marks on them, start
  fresh.
- **Jobs held by a crashed Sidekiq Pro process** (super_fetch) aren't
  recovered by Robin. Stop Sidekiq cleanly, or let Sidekiq Pro recover
  them before switching.

## Differences you might notice

- **Robin's dashboard** replaces Sidekiq's. Extensions written for
  Sidekiq's web UI (`Sidekiq::Web.register`) don't load on it; Robin logs
  a warning and carries on. Robin's own extension API is
  `Robin::Web.register`.
- **Fetching is always reliable.** Jobs a crashed process was running are
  put back on their queues, without Sidekiq Pro's `super_fetch!`.
- **`log_level`** sets the level of a job's start and done lines, not the
  logger's level while the job runs.
- **Deploy marks.** A label marked in the last hour isn't marked again
  (Sidekiq's window is a minute), and marks are kept as long as Robin's
  hourly metrics rather than for 90 days.
- **Environment variables.** `RAILS_ENV` is checked before `APP_ENV`;
  Sidekiq checks `APP_ENV` first.
- **sidekiq-cron jobs** aren't shown on Robin's dashboard, which has no
  page for them (`require "sidekiq/cron/web"` logs a warning). Only Robin's
  leader polls for them, where sidekiq-cron polls on every process, so
  `cron_poll_process_count` is accepted and ignored. Schedules are read by
  Robin's cron parser, which takes fugit's additions to cron syntax as
  well (`MON#2`, `FRI#L`, `-2` and `TUE%2`).
- **sidekiq-scheduler jobs** aren't shown on Robin's dashboard either
  (`require "sidekiq-scheduler/web"` logs a warning). Only Robin's leader
  pushes them, so `every`, `interval` and `in` jobs run once across your
  processes, where sidekiq-scheduler runs them once on each. `every` and
  `interval` jobs run at the multiples of their interval since the Unix
  epoch (`every: "15m"` at :00, :15, :30 and :45), not counting from when
  a process started, so their `first_in` and `first_at` options are
  ignored. `in` counts from when the leader started. `at` times are read
  with `Time.parse` (Chronic's phrases aren't). `listened_queues_only`
  pushes the jobs whose queue some live process takes.
  `rufus_scheduler_options` are accepted and ignored.

Retries follow Sidekiq's rules: up to 25 by default, `count ** 4 + 15`
seconds apart plus jitter, and `sidekiq_retry_in` counts from 0.

## What you get

Everything Robin does, including these built-in features: reliable fetch
with crash recovery, batches, rate limiters, unique, debounced, expiring
and encrypted jobs, periodic jobs, iterable jobs, progress and
cancellation, a live dashboard, metrics, OpenTelemetry tracing, and swarm
mode with rolling restarts. On the same machine, Robin runs about 5
times as many jobs per second as Sidekiq, and about 6 times as many with
a millisecond of network latency; scheduled jobs and retries coming due
reach their queues about 60 times as fast, and a crash loses no jobs
([benchmarks](https://github.com/sferik/robin/blob/main/bench/RESULTS.md)).

Once you're on Robin, you can move to its own names whenever you like
(`include Robin::Job`, `robin_options`, `Robin.configure_server`): both
work side by side.

## Testing robin-compat

```sh
bundle exec rake test
```

The interop tests run the real Sidekiq gem (from `test/interop/Gemfile`)
against the same Redis as Robin: Robin runs what Sidekiq pushed,
scheduled, retried and killed; Sidekiq's API reads what Robin wrote and
acts on it; and Sidekiq runs what Robin pushed. They need a database
they may flush:

```sh
ROBIN_INTEROP_REDIS_URL=redis://127.0.0.1:6379/10 bundle exec ruby -Itest -Ilib test/interop_test.rb
```

They run Sidekiq 8.1, or with `ROBIN_INTEROP_GEMFILE` set to
`test/interop/sidekiq-8.0/Gemfile` (or `sidekiq-7.3`, `sidekiq-7.2`,
`sidekiq-7.1` or `sidekiq-7.0`), that version, with robin-compat set to
follow it. Install those gems first, with `BUNDLE_GEMFILE` set to the
same Gemfile and `bundle install`.
