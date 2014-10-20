require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)

require_relative '../helper.rb'

module Rake
  module DataTask

    class DataTest < Minitest::Test

      include ::TestHelper::SingleAdapterTest

      def mtime_updated? data, operation
        original_mtime = data.mtime
        sleep(1)
        operation.call
        data.mtime > original_mtime
      end

      def test_has_a_modified_time_after_creation
        @adapter.with_tracking do
          @adapter.create_test_data @adapter.test_data_name, columns: "(var1 integer)"
          t = Data.new(@adapter.test_data_name, @adapter)
          assert_operator t.mtime.to_time, :>, Time.new(0)
        end
      end

      def test_has_an_updated_modified_time_after_insert
        skip("Adapter does not support SQL operations.") if !@adapter.kind_of?(Rake::DataTask::Db)
        @adapter.with_tracking do
          @adapter.create_test_data @adapter.test_data_name, columns: "(var1 integer)"
          t = Data.new(@adapter.test_data_name, @adapter)
          operation = lambda do
            @adapter.execute "insert into #{@adapter.test_data_name} values (1)"
          end
          assert mtime_updated?(t, operation)
        end
      end

      def test_has_an_updated_modified_time_after_update
        skip("Adapter does not support SQL operations.") if !@adapter.kind_of?(Rake::DataTask::Db)
        @adapter.with_tracking do
          @adapter.create_test_data @adapter.test_data_name, columns: "(var1 integer, var2 integer)"
          t = Data.new(@adapter.test_data_name, @adapter)
          @adapter.execute "insert into #{@adapter.test_data_name} values (1, 1)"
          operation = lambda do 
            @adapter.execute "update #{@adapter.test_data_name} set var2 = 2 where var1 = 1"
          end
          assert mtime_updated?(t, operation)
        end
      end

      def test_has_an_updated_modified_time_after_delete
        skip("Adapter does not support SQL operations.") if !@adapter.kind_of?(Rake::DataTask::Db)
        @adapter.with_tracking do
          @adapter.create_test_data @adapter.test_data_name, columns: "(var1 integer)"
          t = Data.new(@adapter.test_data_name, @adapter)
          @adapter.execute "insert into #{@adapter.test_data_name} values (1)"
          operation = lambda do
            @adapter.execute "delete from #{@adapter.test_data_name}"
          end
          assert mtime_updated?(t, operation)
        end
      end

      def test_has_an_updated_modified_time_after_truncate
        skip("Adapter does not support SQL operations.") if !@adapter.kind_of?(Rake::DataTask::Db)
        @adapter.with_tracking do
          @adapter.create_test_data @adapter.test_data_name, columns: "(var1 integer)"
          t = Data.new(@adapter.test_data_name, @adapter)
          @adapter.execute "insert into #{@adapter.test_data_name} values (1)"
          operation = lambda do
            @adapter.truncate_data @adapter.test_data_name
          end
          assert mtime_updated?(t, operation)
        end
      end

    end
  end
end
