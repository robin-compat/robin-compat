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
