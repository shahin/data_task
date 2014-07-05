require_relative './helper.rb'

module Rake
  module TableTask

    describe Table do

      test_table_name = "test"

      def mtime_updated? table, operation
        original_mtime = table.mtime
        sleep(1)
        operation.call
        table.mtime > original_mtime
      end

      around do |test|
        # connect an adapter to the configured database for testing
        config = YAML.load_file('config/database.yml')[ENV['TABLETASK_ENV']]
        klass = "Rake::TableTask::#{config['adapter'].capitalize}".split('::').inject(Object) {|memo, name| memo = memo.const_get(name); memo}
        @adapter = klass.new(config)

        # extend the adapter to enable clean tracking setup/teardown within each test
        @adapter.extend(TrackingSetupTeardownHelper)

        @adapter.with_transaction_rollback do
          test.call
        end
      end

      it "has a modified time after creation" do
        @adapter.with_tracking do
          @adapter.create_table test_table_name, nil, "(var1 integer)"
          t = Table.new(test_table_name, @adapter)
          t.mtime.to_time.must_be :>, Time.new(0)
        end
      end

      it "has an updated modified time after insert" do
        @adapter.with_tracking do
          @adapter.create_table test_table_name, nil, "(var1 integer)"
          t = Table.new(test_table_name, @adapter)
          operation = lambda do
            @adapter.execute "insert into #{test_table_name} values (1)"
          end
          mtime_updated?(t, operation).must_equal true
        end
      end

      it "has an updated modified time after update" do
        @adapter.with_tracking do
          @adapter.create_table test_table_name, nil, "(var1 integer, var2 integer)"
          t = Table.new(test_table_name, @adapter)
          @adapter.execute "insert into #{test_table_name} values (1, 1)"
          operation = lambda do 
            @adapter.execute "update #{test_table_name} set var2 = 2 where var1 = 1"
          end
          mtime_updated?(t, operation).must_equal true
        end
      end

      it "has an updated modified time after delete" do
        @adapter.with_tracking do
          @adapter.create_table test_table_name, nil, "(var1 integer)"
          t = Table.new(test_table_name, @adapter)
          @adapter.execute "insert into #{test_table_name} values (1)"
          operation = lambda do
            @adapter.execute "delete from #{test_table_name}"
          end
          mtime_updated?(t, operation).must_equal true
        end
      end

      it "has an updated modified time after truncate" do
        @adapter.with_tracking do
          @adapter.create_table test_table_name, nil, "(var1 integer)"
          t = Table.new(test_table_name, @adapter)
          @adapter.execute "insert into #{test_table_name} values (1)"
          operation = lambda do
            @adapter.truncate_table test_table_name
          end
          mtime_updated?(t, operation).must_equal true
        end
      end

    end
  end
end
