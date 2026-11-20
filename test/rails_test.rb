# frozen_string_literal: true

require_relative "test_helper"
require "rails"
require "active_job/railtie"
require "sidekiq/rails"

class RailsTest < Minitest::Test
  cover "Sidekiq::Rails*"

  def initializer = Sidekiq::Rails.initializers.find { |i| i.name == "sidekiq.active_job" }

  # Runs the initializer, returning the files it required.
  def run_initializer
    required = []
    railtie = Sidekiq::Rails.instance
    railtie.define_singleton_method(:require) { |path| required << path }
    initializer.bind(railtie).run
    required
  ensure
    railtie.singleton_class.remove_method(:require)
  end

  def test_is_a_railtie
    assert_operator Sidekiq::Rails, :<, Rails::Railtie
  end

  def test_loads_the_sidekiq_adapter_before_active_job_picks_one
    assert_equal "active_job.set_configs", initializer.before

    assert_equal ["sidekiq/active_job"], run_initializer
  end

  def test_skips_the_adapter_without_active_job
    railtie = ActiveJob.send(:remove_const, :Railtie)

    assert_empty run_initializer
  ensure
    ActiveJob.const_set(:Railtie, railtie)
  end

  def test_loads_robins_rails_integration
    assert_operator Robin::Rails::Railtie, :<, Rails::Railtie
  end
end
