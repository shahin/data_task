# https://coveralls.io
require 'coveralls'
Coveralls.wear!

require 'minitest/autorun'
require 'minitest/around/unit'

require 'logger'

require 'data_task/sql'
require 'data_task/db'
require 'data_task/filesystem'
require 'data_task/data'
require 'data_task'

require 'data_task/adapters/sqlite'
require 'data_task/adapters/postgres'
require 'data_task/adapters/hdfs'

module TestHelper

  # To extend an adapter with a convenience method to ensure clean tracking state
  module CleanTracking
    def with_tracking &ops
      set_up_tracking
      ops.call
      tear_down_tracking
    end
  end

  # To extend an adapter for an SQL speaker with a common interface for creating tracked test data
  module TestableDbAdapter
    def create_test_data name, options={}
      options = { :data => nil, :columns => '(var1 integer, var2 integer)' }.merge(options)
      create_table name, options[:data], options[:columns]
    end

    attr_reader :test_data_name

    def setup_for_tests; @test_data_name = 'test_data' end
    def teardown_for_tests; remove_instance_variable(:@test_data_name) end
  end

  # To extend an adapter for a filesystem with a common interface for creating tracked test data
  module TestableFilesystemAdapter
    def create_test_data name, options={}
      options = { :data => nil }.merge(options)
      create_file name, options[:data]
    end

    attr_reader :test_data_name

    def setup_for_tests
      @_nontest_base_path = @base_path
      @base_path = "/test"
      mkdir @base_path
      @test_data_name = "test_file"
    end

    def teardown_for_tests
      delete @base_path, :recursive => true
      @base_path = @_nontest_base_path
      remove_instance_variable(:@_nontest_base_path)
      remove_instance_variable(:@test_data_name)
    end
  end

  # To extend an adapter so that it can find its own test helper using its class ancestry
  module TestableAdapter
    def test_helper
      begin
        return ("TestHelper::Testable" + self.to_s.split('::').last + "Adapter").constantize
      rescue
        return self.superclass.test_helper
      end
    end
  end

  # To extend a unit test by instantiating an adapter and an around to keep clean adapter state
  module SingleAdapterTest

    def initialize *args
      super
      @adapter = TestHelper.get_adapter_to_test
    end

    def around(&block)
      @adapter.setup_for_tests if @adapter.respond_to?(:setup_for_tests)
      if @adapter.respond_to? :with_transaction_rollback
        @adapter.with_transaction_rollback do
          yield
        end
      else
        yield
      end
      @adapter.teardown_for_tests if @adapter.respond_to?(:teardown_for_tests)
    end

  end

  # @return [Adapter] configured by the testing environment
  def self.get_adapter_to_test
    # connect an adapter to the configured database for testing
    config = YAML.load_file('test/config/database.yml')[ENV['DATATASK_ENV'] || 'sqlite_test']
    klass = "Rake::DataTask::#{config['adapter'].capitalize}".constantize
    adapter = klass.new(config)

    # extend the adapter instance to enable total tracking setup/teardown within each test
    adapter.extend(CleanTracking)

    # extend the adapter instance with a helper module specifically for its class
    Rake::DataTask::Adapter.extend(TestableAdapter)
    adapter_helper = adapter.class.test_helper
    adapter.extend(adapter_helper) if !adapter_helper.nil?

    adapter
  end

end


module Rake
  module DataTask
    module DataCreation
      OLDDATA = "old_data"
      NEWDATA = "new_data"

      def create_timed_data(adapter, old_data_name, *new_data_names)
        old_data = Data.new(old_data_name, adapter)
        return if (old_data.exists? &&
          new_data_names.all? do |new_data_name|
            new_data = Data.new(new_data_name, adapter)
            new_data.exists? && new_data.mtime > old_data.mtime
          end)
        now = Time.now

        create_data(adapter, old_data_name)
        sleep(1.0)

        new_data_names.each do |new_data_name|
          create_data(adapter, new_data_name)
        end
      end

      def create_data(adapter, name)
        adapter.create_test_data(name) unless adapter.data_exists?(name)
        adapter.data_mtime(name)
      end

      def drop_data(adapter, name)
        adapter.drop_data(name) rescue nil
      end
    end
  end
end
