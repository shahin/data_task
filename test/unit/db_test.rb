require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)

require_relative '../helper.rb'
require 'data_task/adapters/postgres'


module Rake
  module DataTask

    class DbTest < Minitest::Test

      include ::TestHelper::SingleAdapterTest

      def initialize *args
        super
        @test_table = "test_table"
        @test_view = "test_view"
      end

      def before_setup
        super
        if !@adapter.kind_of?(Rake::DataTask::Db)
          skip("Using adapter #{@adapter}, so skipping #{self.class} tests.")
        end
      end

      def test_finds_a_table_when_it_exists
        if !@adapter.table_exists?(@test_table)
          @adapter.execute <<-EOSQL
            create table #{@test_table} (var1 text)
          EOSQL
        end
        assert_equal @adapter.table_exists?(@test_table), true
      end

      def test_does_not_find_a_table_when_it_does_not_exist
        if @adapter.table_exists?(@test_table)
          @adapter.execute <<-EOSQL
            drop table #{@test_table}
          EOSQL
        end
        assert_equal @adapter.table_exists?(@test_table), false
      end

      def test_creates_a_table_when_called_to
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          assert_equal @adapter.table_exists?(@test_table), true

        end
      end

      def test_drops_a_table_when_called_to
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.drop_table @test_table
          assert_equal @adapter.table_exists?(@test_table), false

        end
      end

      def test_creates_a_view_when_called_to
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.create_view @test_view, "select * from #{@test_table}"
          assert_equal @adapter.view_exists?(@test_view), true

        end
      end

      def test_drops_a_view_when_called_to
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.create_view @test_view, "select * from #{@test_table}"
          @adapter.drop_view @test_view
          assert_equal @adapter.view_exists?(@test_view), false

        end
      end

      def test_drops_a_view_when_the_underlying_table_is_dropped
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.create_view @test_view, "select * from #{@test_table}"
          @adapter.drop_table @test_table
          assert_equal @adapter.view_exists?(@test_view), false

        end
      end

      def test_updates_the_tracking_table_when_it_creates_a_table
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
           tracked_create = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{@test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:create]}'
            EOSQL
          )
          assert_equal tracked_create, 1

        end
      end

      def test_updates_the_tracking_table_when_it_drops_a_table
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.drop_table @test_table
          still_tracking_table = Sql.get_single_int(
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{@test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}'
            EOSQL
          )
          assert_nil still_tracking_table

        end
      end

      def test_updates_the_tracking_table_on_insert_to_a_tracked_table
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.execute <<-EOSQL
            insert into #{@test_table} values ('a')
          EOSQL
          tracked_insert = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{@test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:insert]}'
            EOSQL
          )
          assert_equal tracked_insert, 1

        end
      end

      def test_updates_the_tracking_table_on_update_on_a_tracked_table
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text, var2 text)'
          @adapter.execute <<-EOSQL
            insert into #{@test_table} values ('a', 'a')
          EOSQL
          @adapter.execute <<-EOSQL
            update #{@test_table} set var2 = 'b' where var1 = 'a'
          EOSQL

          tracked_insert = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{@test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:update]}'
            EOSQL
          )
          assert_equal tracked_insert, 1

        end
      end

      def test_updates_the_tracking_table_on_truncate_of_a_tracked_table
        @adapter.with_tracking do

          @adapter.create_test_data @test_table, columns: '(var1 text)'
          @adapter.truncate_table @test_table
          tracked_truncate = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{@test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:truncate]}'
            EOSQL
          )
          assert_equal tracked_truncate, 1

        end
      end

      def test_says_it_is_tracking_operations_after_tracking_is_set_up
        @adapter.tear_down_tracking
        @adapter.set_up_tracking
        assert_equal @adapter.tracking_operations?, true
      end

      def test_says_it_is_not_tracking_operations_after_tracking_is_torn_down
        @adapter.tear_down_tracking
        assert_equal (@adapter.tracking_operations?), false
      end

    end

  end
end
