# frozen_string_literal: true

require_relative "test_helper"
require "rack/test"
require "sidekiq/web"
require "sidekiq-ent/web"
require "sidekiq/pro/web"

class WebTest < RobinCompatTest
  cover "Sidekiq::Web*"

  include Rack::Test::Methods

  # A middleware recording what it was built with and wrapping the
  # response in a header.
  class Tagger
    def initialize(app, tag, suffix: "", &block)
      @app = app
      @tag = tag + suffix + (block ? block.call : "")
    end

    def call(env)
      status, headers, body = @app.call(env)
      [status, headers.merge("x-tags" => [headers["x-tags"], @tag].compact.join(",")), body]
    end
  end

  WEB_STATE = %i[@middlewares @app @app_url @assets_path @redis_pool @tabs @locales @custom_job_info_rows @options].freeze
  ROBIN_WEB_STATE = %i[@clusters @authorizer @app].freeze

  def setup
    super
    @saved = WEB_STATE.to_h { |name| [name, Sidekiq::Web.instance_variable_get(name)] }
    @saved_robin = ROBIN_WEB_STATE.to_h { |name| [name, Robin::Web.instance_variable_get(name)] }
    WEB_STATE.each { |name| Sidekiq::Web.instance_variable_set(name, nil) }
    Sidekiq::Web.instance_variable_set(:@middlewares, [])
    @log = StringIO.new
    config.logger = Logger.new(@log)
  end

  def teardown
    @saved.each { |name, value| Sidekiq::Web.instance_variable_set(name, value) }
    @saved_robin.each { |name, value| Robin::Web.instance_variable_set(name, value) }
    super
  end

  def app
    Rack::Builder.new do
      map("/sidekiq") { run Sidekiq::Web }
    end
  end

  def json = JSON.parse(last_response.body)

  def pause_default_queue
    post "/sidekiq/api/queues/default/pause", "{}",
      {"CONTENT_TYPE" => "application/json", "HTTP_X_ROBIN_REQUEST" => "1", "HTTP_SEC_FETCH_SITE" => "same-origin"}
  end

  def test_serves_robins_dashboard_where_mounted
    Robin::Client.push("class" => "HardJob", "args" => [1], "queue" => "default")
    get "/sidekiq/api/queues"

    assert_equal 200, last_response.status
    assert_equal [["default", 1]], json.fetch("queues").map { |q| [q["name"], q["size"]] }
  end

  def test_an_instance_serves_it_too
    status, = Sidekiq::Web.new.call(Rack::MockRequest.env_for("/api/stats"))

    assert_equal 200, status
  end

  def test_use_wraps_the_dashboard_in_rack_middleware
    Sidekiq::Web.use(Rack::Auth::Basic) { |user, password| [user, password] == %w[admin secret] }
    get "/sidekiq/api/stats"

    assert_equal 401, last_response.status
    basic_authorize "admin", "secret"
    get "/sidekiq/api/stats"

    assert_equal 200, last_response.status
  end

  def test_middleware_get_their_arguments_options_and_block_outermost_first
    Sidekiq::Web.use(Tagger, "a")
    Sidekiq::Web.use(Tagger, "b", suffix: "!") { "?" }
    get "/sidekiq/api/stats"

    assert_equal "b!?,a", last_response.headers["x-tags"]
    assert_equal [[Tagger, ["a"], {}, nil]], Sidekiq::Web.middlewares.first(1)
  end

  def test_use_rebuilds_the_app
    first = Sidekiq::Web.app

    assert_same Robin::Web, first
    assert_same first, Sidekiq::Web.app
    Sidekiq::Web.use(Tagger, "a")

    assert_instance_of Tagger, Sidekiq::Web.app
  end

  def test_configure_yields_or_returns_the_settings
    assert_equal :done, Sidekiq::Web.configure { |c| (c == Sidekiq::Web) ? :done : :wrong }
    assert_same Sidekiq::Web, Sidekiq::Web.configure
    Sidekiq::Web.configure { |c| c.use(Tagger, "c") }

    assert_equal [Tagger, ["c"], {}, nil], Sidekiq::Web.middlewares.last
  end

  def test_settings_robins_dashboard_does_not_use
    assert_nil Sidekiq::Web[:profile_view_url]
    Sidekiq::Web[:profile_store_url] = "https://store.example.com"
    Sidekiq::Web.app_url = "https://example.com"
    Sidekiq::Web.assets_path = "/srv/sidekiq/assets"
    Sidekiq::Web.tabs["Cron"] = "cron"
    Sidekiq::Web.locales << "/srv/locales"
    Sidekiq::Web[:profile_view_url] = "https://profiler.example.com/%s"

    assert_equal "https://example.com", Sidekiq::Web.app_url
    assert_equal "/srv/sidekiq/assets", Sidekiq::Web.assets_path
    assert_nil Robin::Web.assets_path
    assert_equal({"Cron" => "cron"}, Sidekiq::Web.tabs)
    assert_equal ["/srv/locales"], Sidekiq::Web.locales
    assert_equal "https://profiler.example.com/%s", Sidekiq::Web[:profile_view_url]
    assert_equal "https://store.example.com", Sidekiq::Web[:profile_store_url]
    assert_nil Sidekiq::Web[:other]
    assert_nil Sidekiq::Web.set(:sessions, false)
    assert_nil Sidekiq::Web.public_send(:sessions=, false)
  end

  def test_options_can_be_set_first
    Sidekiq::Web[:a] = 1

    assert_equal 1, Sidekiq::Web[:a]
  end

  def test_redis_pool_switches_the_dashboards_redis
    other = Robin::Redis::Pool.new(size: 1) { Robin::Redis::Connection.new(url: REDIS_URL) }
    Sidekiq::Web.redis_pool = Sidekiq::RedisConnection::Pool.new(other)
    shown = Robin::Web.clusters.fetch("default")

    assert_equal ["default"], Robin::Web.clusters.keys
    assert_same other, shown.redis_pool
    refute_same config, shown
    assert_equal config.key_prefix, shown.key_prefix
    refute_same other, config.redis_pool
    assert_same other, Sidekiq::Web.redis_pool.pool
    Sidekiq::Web.redis_pool = other

    assert_same other, Robin::Web.clusters.fetch("default").redis_pool
  ensure
    other.shutdown
  end

  def test_redis_pool_defaults_to_sidekiqs
    Sidekiq::Web.instance_variable_set(:@redis_pool, nil)

    assert_same config.redis_pool, Sidekiq::Web.redis_pool.pool
  end

  def test_authorize_gets_the_env_method_and_path
    calls = []
    Sidekiq::Web.authorize do |env, method, path|
      calls << [env["PATH_INFO"], method, path]
      method == "GET" || env["HTTP_X_ADMIN"] == "yes"
    end
    get "/sidekiq/api/stats"

    assert_equal 200, last_response.status
    assert_equal ["/api/stats", "GET", "/api/stats"], calls.first
    pause_default_queue

    assert_equal 403, last_response.status
    assert_equal ["/api/queues/default/pause", "POST", "/api/queues/default/pause"], calls.last
    header "X-Admin", "yes"
    pause_default_queue

    assert_equal 200, last_response.status
  end

  def test_each_authorize_keeps_its_own_block
    Sidekiq::Web.authorize { |_env, _method, _path| :first }
    first = Robin::Web.authorizer
    Sidekiq::Web.authorize { |_env, _method, _path| :second }

    assert_equal :first, first.call({}, :read, nil)
    assert_equal :second, Robin::Web.authorizer.call({}, :read, nil)
  end

  def test_authorize_can_deny_everything
    Sidekiq::Web.authorize { |_env, _method, _path| nil }
    get "/sidekiq/api/stats"

    assert_equal 403, last_response.status
  end

  def test_authorize_blocks_may_return
    Sidekiq::Web.authorize do |env, _method, _path|
      return false unless env["HTTP_X_ADMIN"]

      true
    end
    get "/sidekiq/api/stats"

    assert_equal 403, last_response.status
    header "X-Admin", "1"
    get "/sidekiq/api/stats"

    assert_equal 200, last_response.status
  end

  def test_authorize_blocks_take_fewer_or_optional_arguments
    seen = []
    Sidekiq::Web.authorize { |env| seen << env.class }
    get "/sidekiq/api/stats"
    Sidekiq::Web.authorize { seen << :none }
    get "/sidekiq/api/stats"
    Sidekiq::Web.authorize { |*args| seen << args.size }
    get "/sidekiq/api/stats"

    assert_equal 200, last_response.status
    assert_equal [Hash, :none, 3], seen
  end

  def test_register_warns
    Sidekiq::Web.register(Tagger, name: "tagger", tab: "Tags", index: "tags")
    Sidekiq::Web.register_extension(:other)

    assert_includes @log.string, "Sidekiq::Web.register(WebTest::Tagger) does nothing on Robin; see Robin::Web.register"
    assert_includes @log.string, "Sidekiq::Web.register(other)"
  end

  def test_custom_job_info_rows_warn
    Sidekiq::Web.instance_variable_set(:@custom_job_info_rows, nil)
    rows = Sidekiq::Web.custom_job_info_rows

    assert_same rows, Sidekiq::Web.custom_job_info_rows
    assert_same rows, rows << :row
    assert_equal [:row], rows
    assert_match(/WARN.*custom_job_info_rows aren't shown by Robin's dashboard; see Robin::Web.job_info/, @log.string)
  end
end
