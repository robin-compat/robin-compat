# frozen_string_literal: true

require "sidekiq"
require "robin/web"

module Sidekiq
  # Robin's dashboard where Sidekiq's was mounted:
  #
  #   mount Sidekiq::Web => "/sidekiq"       # Rails routes
  #   run Sidekiq::Web                       # config.ru
  #
  #   Sidekiq::Web.use Rack::Auth::Basic do |user, password|
  #     ActiveSupport::SecurityUtils.secure_compare(user, ENV["ADMIN"]) && ...
  #   end
  class Web
    # Sidekiq's custom job rows (objects with an add_pair method) don't
    # show on Robin's dashboard: adding one warns. Robin::Web.job_info
    # adds rows there.
    class JobInfoRows < Array
      def <<(row)
        Robin.logger.warn("Sidekiq::Web custom_job_info_rows aren't shown by Robin's dashboard; see Robin::Web.job_info")
        super
      end
    end

    # The Rack middleware Web.use added, outermost first.
    @middlewares = []

    class << self
      attr_reader :middlewares

      def call(env) = app.call(env)

      # Wraps the dashboard in a Rack middleware.
      def use(middleware, *args, **options, &block)
        @app = nil
        @middlewares << [middleware, args, options, block]
      end

      # Yields an object taking Sidekiq::Web's settings (use, app_url=...);
      # without a block, returns it.
      def configure = block_given? ? yield(self) : self

      def app
        @app ||= @middlewares.reverse.reduce(Robin::Web) do |inner, (middleware, args, options, block)|
          middleware.new(inner, *args, **options, &block)
        end
      end

      # Where the dashboard's "back to the app" link goes, and the
      # directory of Sidekiq's own assets: neither is used by Robin's
      # dashboard (Robin::Web.assets_path serves its assets from
      # elsewhere).
      attr_accessor :app_url, :assets_path

      # The Redis the dashboard shows (a Sidekiq::RedisConnection::Pool
      # or Robin's own pool).
      def redis_pool=(pool)
        @redis_pool = pool
        config = Robin.config.dup
        config.redis_pool = Client.robin_pool(pool)
        Robin::Web.clusters = {"default" => config}
      end

      def redis_pool = @redis_pool || Sidekiq.redis_pool

      # Sidekiq Enterprise's authorization: the block gets the Rack env, the
      # HTTP method and the path, and returns whether to allow it. Viewing
      # asks with "GET" and changing anything with "POST", as Sidekiq's own
      # dashboard does. The block may `return` its answer, as it's run as
      # a method.
      def authorize(&block)
        check = Class.new { define_method(:allowed?, &block) }.new
        Robin::Web.authorize do |env, action, _resource|
          args = [env, (action == :read) ? "GET" : "POST", env["PATH_INFO"]]
          check.allowed?(*(block.arity.negative? ? args : args.first(block.arity)))
        end
      end

      # Dashboard extensions made for Sidekiq's web UI don't run on Robin's
      # dashboard (use Robin::Web.register); registering one warns. Their
      # tabs and locales are kept but not shown.
      def register(extension, *, **)
        Robin.logger.warn("Sidekiq::Web.register(#{extension}) does nothing on Robin; see Robin::Web.register")
      end
      alias_method :register_extension, :register

      def tabs = @tabs ||= {}

      def locales = @locales ||= []

      def custom_job_info_rows = @custom_job_info_rows ||= JobInfoRows.new

      # Dashboard options (such as profile_view_url) Robin doesn't use.
      def [](key) = (@options ||= {})[key]

      def []=(key, value)
        (@options ||= {})[key] = value
      end

      # Sessions and CSRF settings: Robin's dashboard needs no session.
      def set(*)
      end

      def sessions=(_value)
      end
    end

    # `run Sidekiq::Web.new` works as `run Sidekiq::Web` does.
    def call(env) = self.class.call(env)
  end
end
