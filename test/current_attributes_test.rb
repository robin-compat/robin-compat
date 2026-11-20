# frozen_string_literal: true

require_relative "test_helper"
require "sidekiq/middleware/current_attributes"
require "active_job/railtie" # ActiveJob::Arguments needs what Rails loads

module CurrentAttributesTestClasses
  class Current < ActiveSupport::CurrentAttributes
    attribute :user
  end

  class Tenant < ActiveSupport::CurrentAttributes
    attribute :slug
  end

  class Job
    include Sidekiq::Job

    def perform = nil
  end
end

class CurrentAttributesTest < RobinCompatTest
  cover "Sidekiq::CurrentAttributes*"

  include CurrentAttributesTestClasses

  def setup
    super
    [Current, Tenant].each(&:reset)
  end

  def teardown
    [Current, Tenant].each(&:reset)
    super
  end

  def pushed = JSON.parse(redis("LINDEX", key("queue:default"), 0))

  # Runs +job+ through the server middleware, returning what Current and
  # Tenant held meanwhile.
  def run_job(job, robin = config)
    instance = Job.new
    robin.server_middleware.invoke(instance, job, "default") { [Current.user, Tenant.slug] }
  end

  def test_persists_one_class_under_cattr
    Sidekiq::CurrentAttributes.persist(Current)
    Current.user = "ann"
    Job.perform_async
    Current.reset
    job = pushed

    assert_equal({"user" => "ann"}, ActiveJob::Arguments.deserialize(job.fetch("cattr")).to_h.transform_keys(&:to_s))
    refute job.key?("cattr_1")
    assert_equal ["ann", nil], run_job(job)
  end

  def test_persists_several_classes_by_name_into_the_given_config
    sidekiq = Sidekiq::Config.new(config)
    Sidekiq::CurrentAttributes.persist([Current.name, Tenant], sidekiq)
    Current.user = "bob"
    Tenant.slug = "acme"
    Job.perform_async
    [Current, Tenant].each(&:reset)
    job = pushed

    assert_equal %w[cattr cattr_1], job.keys.grep(/cattr/).sort
    assert_equal %w[bob acme], run_job(job)
  end

  def test_a_robin_config_works_too
    other = Robin::Config.new
    Sidekiq::CurrentAttributes.persist("CurrentAttributesTestClasses::Current", other)

    assert_equal Robin::Rails::CurrentAttributes::Save, other.client_middleware.entries.last.klass
    assert_equal Robin::Rails::CurrentAttributes::Load, other.server_middleware.entries.first.klass
    refute_includes config.client_middleware.entries.map(&:klass), Robin::Rails::CurrentAttributes::Save
  end
end
