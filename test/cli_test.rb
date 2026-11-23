# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq/cli"
require "tmpdir"
require "open3"
require "rbconfig"

class CLITest < RobinCompatTest
  cover "Sidekiq::CLI*"

  # Records the Robin::CLI runs a test asks to capture instead of
  # starting a server.
  module CaptureRun
    def run
      runs = Thread.current[:robin_sidekiq_cli_runs]
      runs ? runs << @argv : super
    end
  end
  Robin::CLI.prepend(CaptureRun)

  def setup
    super
    @runs = Thread.current[:robin_sidekiq_cli_runs] = []
  end

  def teardown
    Thread.current[:robin_sidekiq_cli_runs] = nil
    super
  end

  # Parses +argv+ in a directory holding +files+ and applies what the
  # command line and config file say, as the server does after loading
  # the app.
  def configure(files, argv = [])
    Dir.mktmpdir do |dir|
      files.each do |path, content|
        FileUtils.mkdir_p(File.join(dir, File.dirname(path)))
        File.write(File.join(dir, path), content)
      end
      Dir.chdir(dir) do
        cli = Robin::CLI.new(argv)
        cli.parse
        cli.send(:apply_options)
      end
    end
  end

  def test_sidekiq_yml_comes_before_robin_yml
    assert_equal ["config/sidekiq.yml", "config/sidekiq.yml.erb", "config/robin.yml"], Robin::CLI.config_paths.first(3)
    configure("config/sidekiq.yml" => ":concurrency: 7\n", "config/robin.yml" => "concurrency: 3\n")

    assert_equal 7, config.concurrency
  end

  def test_sidekiq_settings_go_into_the_configuration
    configure("config/sidekiq.yml" => <<~YAML)
      :concurrency: 4
      :queues:
        - critical
        - default
      :max_retries: 7
      :dead_max_jobs: 500
      :average_scheduled_poll_interval: 3
      :labels:
        - eu
        - fast
      custom_setting: 42
      :pidfile: tmp/sidekiq.pid
      :dead_timeout_in_seconds:
    YAML

    assert_equal 4, config.concurrency
    assert_equal %w[critical default], config.queues
    assert_equal 7, config.max_retries
    assert_equal 500, config.dead_max_jobs
    assert_equal 3, config.poll_interval
    assert_equal %w[eu fast], config.labels
    assert_equal 42, Sidekiq.default_configuration[:custom_setting]
    refute Sidekiq.default_configuration.key?(:pidfile)
    refute Sidekiq.default_configuration.key?(:queues_extra)
    refute_nil config.dead_timeout
  end

  def test_server_settings_and_sections_are_left_to_the_server
    configure({"config/sidekiq.yml" => <<~YAML}, %w[-e production])
      :max_retries: 3
      :timeout: 9
      :strict: false
      :logfile: log/sidekiq.log
      :production:
        :max_retries: 11
      :staging:
        :max_retries: 1
      :capsules:
        :single:
          :concurrency: 1
          :queues: [single]
    YAML

    assert_equal 11, config.max_retries
    assert_equal 9, config.timeout
    assert_equal ["single"], config.capsules.fetch("single").queues
    sidekiq = Sidekiq.default_configuration

    assert_equal [false, false, false, false], %i[strict logfile production staging].map { |k| sidekiq.instance_variable_get(:@extra).key?(k) }
  end

  def test_erb_with_trim_mode
    configure("config/sidekiq.yml.erb" => <<~YAML)
      <%- if true -%>
      :max_retries: <%= 2 + 3 %>
      <%- end -%>
    YAML

    assert_equal 5, config.max_retries
  end

  def test_verbose_logs_debug
    config.logger = Logger.new(nil, level: :info)
    configure("config/sidekiq.yml" => ":verbose: false\n")

    assert_equal Logger::INFO, config.logger.level
    configure("config/sidekiq.yml" => ":verbose: true\n")

    assert_equal Logger::DEBUG, config.logger.level
  end

  def test_settings_for_another_config_stay_with_it
    other = Robin::Config.new
    Sidekiq::CLI.apply_settings({max_retries: 2, custom: 1}, other)

    assert_equal 2, other.max_retries
    refute_equal 2, config.max_retries
    refute Sidekiq.default_configuration.key?(:custom)
  end

  def test_instance
    assert_instance_of Sidekiq::CLI, Sidekiq::CLI.instance
    assert_same Sidekiq::CLI.instance, Sidekiq::CLI.instance
  end

  def test_run_runs_robins_command
    env = {"RAILS_ENV" => "test"}
    Sidekiq::CLI.new.run(%w[-q critical], env: env, boot_app: true)

    assert_equal [%w[-q critical]], @runs
    assert_equal({"RAILS_ENV" => "test"}, env)
    assert_equal 5, config.concurrency
  end

  def test_run_takes_concurrency_from_rails_max_threads
    Sidekiq::CLI.new.run([], env: {"RAILS_MAX_THREADS" => "12", "APP_ENV" => "x"})

    assert_equal 12, config.concurrency
    assert_raises(ArgumentError) { Sidekiq::CLI.new.run([], env: {"RAILS_MAX_THREADS" => "lots"}) }
  end

  def test_run_defaults_the_environment_to_development
    %w[APP_ENV RAILS_ENV RACK_ENV].each do |name|
      env = {name => "staging"}
      Sidekiq::CLI.new.run([], env: env)

      assert_equal({name => "staging"}, env)
    end
    env = {}
    Sidekiq::CLI.new.run([], env: env)

    assert_equal({"RACK_ENV" => "development"}, env)
  end

  def test_run_uses_parsed_arguments_or_argv
    cli = Sidekiq::CLI.new
    cli.run(env: {"APP_ENV" => "x"})
    cli.parse(%w[-c 3])
    cli.run(env: {"APP_ENV" => "x"})
    cli.parse
    cli.run(env: {"APP_ENV" => "x"})

    assert_equal [ARGV, %w[-c 3], ARGV], @runs
  end

  def test_parse_keeps_the_arguments_as_they_were
    cli = Sidekiq::CLI.new
    argv = %w[-c 3]
    cli.parse(argv)
    argv << "-v"
    cli.run(env: {"APP_ENV" => "x"})
    saved = ARGV.dup
    cli.parse
    ARGV.replace(%w[later])
    cli.run(env: {"APP_ENV" => "x"})

    assert_equal [%w[-c 3], saved], @runs
  ensure
    ARGV.replace(saved)
  end

  # The real environment, restored afterwards.
  def with_env(vars)
    saved = ENV.to_h
    ENV.update(vars)
    yield
  ensure
    ENV.replace(saved)
  end

  def test_run_and_swarm_read_the_real_environment
    with_env("RAILS_MAX_THREADS" => "9", "SIDEKIQ_MAXMEM_MB" => "100", "SIDEKIQ_COUNT" => "2", "APP_ENV" => "test") do
      Sidekiq::CLI.new.run(%w[-v])

      assert_equal 9, config.concurrency
      Sidekiq::CLI.new.swarm(%w[-v])

      assert_equal 100, config.memory_limit_mb
    end

    assert_equal [%w[-v], %w[swarm -n 2 -v]], @runs
  end

  def test_swarm
    env = {"SIDEKIQ_COUNT" => "4", "SIDEKIQ_MAXMEM_MB" => "300", "RAILS_MAX_THREADS" => "2"}
    Sidekiq::CLI.new.swarm(%w[-r ./app.rb], env: env)

    assert_equal [%w[swarm -n 4 -r ./app.rb]], @runs
    assert_equal 300, config.memory_limit_mb
    assert_equal 2, config.concurrency
    assert_equal "development", env["RACK_ENV"]
  end

  def test_swarm_defaults
    env = {}
    Sidekiq::CLI.new.swarm(env: env)

    assert_equal [["swarm", *ARGV]], @runs
    assert_nil config.memory_limit_mb
    assert_equal "development", env["RACK_ENV"]
  end

  def test_rails_is_loaded_before_robin_boots_the_app
    loader = Robin::CLI.app_loaders.first

    assert_equal [Sidekiq::CLI, :load_rails], [loader.receiver, loader.name]
    assert_includes Robin::CLI.app_loaders, Robin::Rails::AppLoader
  end

  def test_load_rails_loads_sidekiq_rails_and_leaves_the_boot_to_robin
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "config"))
      File.write(File.join(dir, "config", "application.rb"), "")
      paths = recording_requires(Sidekiq::CLI) { assert_nil Sidekiq::CLI.load_rails(dir) }

      assert_equal %w[rails sidekiq/rails], paths
    end
  end

  def test_load_rails_skips_other_directories
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "config"))
      File.write(File.join(dir, "config", "sidekiq.yml"), "")
      paths = recording_requires(Sidekiq::CLI) { assert_nil Sidekiq::CLI.load_rails(dir) }

      assert_empty paths
    end
  end

  # The command loads sidekiq before Rails. A Rails app it boots still
  # gets robin-compat's :sidekiq adapter, not Rails's own (whose file
  # needs the sidekiq gem, which isn't in the bundle).
  def test_a_rails_app_it_boots_gets_the_sidekiq_adapter
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "config"))
      File.write(File.join(dir, "config", "application.rb"), <<~RUBY)
        require "rails"
        require "active_job/railtie"
        require "sidekiq"

        class CLITestApp < Rails::Application
          config.root = File.expand_path("..", __dir__)
          config.eager_load = false
          config.logger = Logger.new(nil)
          config.active_job.queue_adapter = :sidekiq
        end
      RUBY
      File.write(File.join(dir, "config", "environment.rb"), <<~RUBY)
        require_relative "application"
        Rails.application.initialize!
      RUBY
      code = <<~RUBY
        require "sidekiq/cli"
        Robin::CLI.new([]).send(:load_directory, #{dir.dump})
        print ActiveJob::Base.queue_adapter.class.ancestors.include?(ActiveJob::QueueAdapters::RobinAdapter)
      RUBY
      out, status = Open3.capture2e(RbConfig.ruby, "-rbundler/setup", "-I", File.expand_path("../lib", __dir__), "-e", code)

      assert_predicate status, :success?, out
      assert_equal "true", out
    end
  end
end
