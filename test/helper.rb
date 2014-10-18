# https://coveralls.io
require 'coveralls'
Coveralls.wear!

require 'minitest/autorun'
require 'minitest/around/unit'

require 'logger'

require 'data_task/sql'
require 'data_task/db'
require 'data_task/data'
require 'data_task'

require 'data_task/adapters/sqlite'
require 'data_task/adapters/postgres'
require 'data_task/adapters/hdfs'

module TestHelper

  module TrackingSetupTeardownHelper
    def with_tracking &ops
      set_up_tracking
      ops.call
      tear_down_tracking
    end
  end

  module RdbmsTestHelper
    def create_test_data name, options={}
      options = { :data => nil, :columns => '(var1 integer, var2 integer)' }.merge(options)
      create_table name, options[:data], options[:columns]
    end
  end

  module FilesystemTestHelper
    def create_test_data name, options={}
      options = { :data => nil }.merge(options)
      create_file name, options[:data]
    end
  end

  def self.get_adapter_to_test_db
    # connect an adapter to the configured database for testing
    config = YAML.load_file('test/config/database.yml')[ENV['DATATASK_ENV'] || 'sqlite_test']
    klass = "Rake::DataTask::#{config['adapter'].capitalize}".split('::').inject(Object) {|memo, name| memo = memo.const_get(name); memo}
    adapter = klass.new(config)

    # extend the adapter instance to enable clean tracking setup/teardown within each test
    adapter.extend(TrackingSetupTeardownHelper)

    adapter.extend(RdbmsTestHelper) if adapter.kind_of? Rake::DataTask::Db
    adapter.extend(FilesystemTestHelper) if adapter.kind_of? Rake::DataTask::Hdfs

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
