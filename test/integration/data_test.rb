require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)

require_relative '../helper.rb'

module Rake
  module DataTask

    class DataTest < Rake::TestCase

      def initialize *args
        super
        @test_data_name = "test"
      end

      def mtime_updated? data, operation
        original_mtime = data.mtime
        sleep(1)
        operation.call
        data.mtime > original_mtime
      end

      def around(&block)
        @adapter = TestHelper.get_adapter_to_test_db
        if @adapter.respond_to? :with_transaction_rollback
          @adapter.with_transaction_rollback do
            yield
          end
        else
          yield
        end
      end

      def test_has_a_modified_time_after_creation
        @adapter.with_tracking do
          @adapter.create_test_data @test_data_name, nil, "(var1 integer)"
          t = Data.new(@test_data_name, @adapter)
          assert_operator t.mtime.to_time, :>, Time.new(0)
        end
      end

      def test_has_an_updated_modified_time_after_insert
        @adapter.with_tracking do
          @adapter.create_test_data @test_data_name, nil, "(var1 integer)"
          t = Data.new(@test_data_name, @adapter)
          operation = lambda do
            @adapter.execute "insert into #{@test_data_name} values (1)"
          end
          assert mtime_updated?(t, operation)
        end
      end

      def test_has_an_updated_modified_time_after_update
        @adapter.with_tracking do
          @adapter.create_test_data @test_data_name, nil, "(var1 integer, var2 integer)"
          t = Data.new(@test_data_name, @adapter)
          @adapter.execute "insert into #{@test_data_name} values (1, 1)"
          operation = lambda do 
            @adapter.execute "update #{@test_data_name} set var2 = 2 where var1 = 1"
          end
          assert mtime_updated?(t, operation)
        end
      end

      def test_has_an_updated_modified_time_after_delete
        @adapter.with_tracking do
          @adapter.create_test_data @test_data_name, nil, "(var1 integer)"
          t = Data.new(@test_data_name, @adapter)
          @adapter.execute "insert into #{@test_data_name} values (1)"
          operation = lambda do
            @adapter.execute "delete from #{@test_data_name}"
          end
          assert mtime_updated?(t, operation)
        end
      end

      def test_has_an_updated_modified_time_after_truncate
        @adapter.with_tracking do
          @adapter.create_test_data @test_data_name, nil, "(var1 integer)"
          t = Data.new(@test_data_name, @adapter)
          @adapter.execute "insert into #{@test_data_name} values (1)"
          operation = lambda do
            @adapter.truncate_data @test_data_name
          end
          assert mtime_updated?(t, operation)
        end
      end

    end
  end
end
