require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)

require_relative '../helper.rb'
require 'data_task/adapters/postgres'

module Rake
  module DataTask

    class PostgresTest < Rake::TestCase
      include Rake

      def initialize *args
        super

        @right_schema = "test_schema_1"
        @wrong_schema = "test_schema_2"
        @test_table = "test_table"
        @test_view = "test_view"
        @test_role = "test_role"

        @test_table_right_schema = "#{@right_schema}.#{@test_table}"
        @test_table_wrong_schema = "#{@wrong_schema}.#{@test_table}"

        @test_view_right_schema = "#{@right_schema}.#{@test_view}"
        @test_view_wrong_schema = "#{@wrong_schema}.#{@test_view}"
      end

      def setup
        super

        if !@adapter.kind_of?(Rake::DataTask::Postgres)
          skip("Using adapter #{@adapter}, so skipping #{self.class} tests.")
        end

        @adapter.execute <<-EOSQL
          create schema #{@right_schema};
          create schema #{@wrong_schema};
        EOSQL
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

      def test_returns_the_current_user_name_when_called_to
        @adapter.execute "create role #{@test_role}"
        @adapter.with_role(@test_role) do
          assert_equal @adapter.send(:current_user), @test_role
        end
      end

      def test_returns_the_current_search_path_when_called_to
        @adapter.execute "set search_path to #{@right_schema}, public"
        assert_equal @adapter.send(:search_path), [@right_schema, 'public']
      end

      def test_resets_the_search_path_after_exiting_a_with_search_path_block
        @adapter.execute "set search_path to #{@right_schema}, public"
        @adapter.with_search_path([@wrong_schema,'public']) do
          assert_equal @adapter.send(:search_path), [@wrong_schema, 'public']
        end
        assert_equal @adapter.send(:search_path), [@right_schema, 'public']
      end

      def test_returns_the_first_schema_in_the_search_path_that_contains_a_table_when_called_to
        @adapter.execute "create table #{@right_schema}.#{@test_table} (var1 integer)"
        @adapter.execute "set search_path to #{@wrong_schema}, #{@right_schema}, 'public'"
        assert_equal @adapter.send(:first_schema_for, @test_table), @right_schema
      end

      def test_finds_a_table_when_it_exists_in_the_right_schema
        @adapter.execute "create table #{@test_table_right_schema} (var1 integer)"
        assert_equal @adapter.table_exists?(@test_table_right_schema), true
      end

      def test_does_not_find_a_table_when_it_does_not_exist_in_the_right_schema
        assert_equal @adapter.table_exists?(@test_table_right_schema), false
      end

      def test_does_not_find_a_table_when_it_exists_in_the_wrong_schema
        @adapter.execute "create table #{@test_table_wrong_schema} (var1 integer)"
        assert_equal @adapter.table_exists?(@test_table_right_schema), false
      end

      def test_creates_a_table_in_the_right_schema_when_called_to
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do
            @adapter.create_test_data @test_table_right_schema, nil, '(var1 text)'
            assert_equal @adapter.table_exists?(@test_table_right_schema), true
          end
        end
      end

      def test_drops_a_table_in_the_right_schema_when_called_to
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.execute "create table #{@test_table_right_schema} (var1 text)"
            @adapter.execute "create table #{@test_table_wrong_schema} (var1 text)"
            @adapter.drop_table @test_table_right_schema
            assert_equal @adapter.table_exists?(@test_table_right_schema), false
            assert_equal @adapter.table_exists?(@test_table_wrong_schema), true

          end
        end
      end

      def test_creates_a_view_in_the_right_schema_when_called_to
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_view @test_view_right_schema, "select * from information_schema.tables limit 0"
            assert_equal @adapter.view_exists?(@test_view_right_schema), true

          end
        end
      end

      def test_drops_a_view_in_the_right_schema_when_called_to
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_view @test_view_right_schema, "select * from information_schema.tables limit 0"
            @adapter.drop_view @test_view_right_schema
            assert_equal @adapter.view_exists?(@test_view_right_schema), false

          end
        end
      end

      def test_updates_the_tracking_table_in_the_right_schema_when_it_creates_a_table
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_test_data @test_table_right_schema, nil, '(var1 integer)'
            tracked_create = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{@right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{@test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:create]}'
              EOSQL
            )
            assert_equal tracked_create, 1

          end
        end
      end

      def test_updates_the_tracking_table_in_the_right_schema_when_it_drops_a_table
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_test_data @test_table_right_schema, nil, '(var1 text)'
            @adapter.drop_table @test_table_right_schema
            still_tracking_table = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{@right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{@test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}'
              EOSQL
            )
            assert_nil still_tracking_table

          end
        end
      end

      def test_updates_the_tracking_table_in_the_right_schema_on_insert_to_a_tracked_table
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_test_data @test_table_right_schema, nil, '(var1 text)'
            @adapter.execute "insert into #{@test_table_right_schema} values ('a')"
            tracked_insert = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{@right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{@test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:insert]}'
              EOSQL
            )
            assert_equal tracked_insert, 1

          end
        end
      end

      def test_updates_the_tracking_table_in_the_right_schema_on_update_on_a_tracked_table
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_test_data @test_table_right_schema, nil, '(var1 text, var2 text)'
            @adapter.execute "insert into #{@test_table_right_schema} values ('a', 'a')"
            @adapter.execute "update #{@test_table_right_schema} set var2 = 'b' where var1 = 'a'"

            tracked_insert = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{@right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{@test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:update]}'
              EOSQL
            )
            assert_equal tracked_insert, 1

          end
        end
      end

      def test_updates_the_tracking_table_in_the_right_schema_on_truncate_of_a_tracked_table
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_test_data @test_table_right_schema, nil, '(var1 text)'
            @adapter.truncate_table @test_table_right_schema
            tracked_truncate = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{@right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{@test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:truncate]}'
              EOSQL
            )
            assert_equal tracked_truncate, 1

          end
        end
      end

      def test_says_it_is_tracking_operations_after_tracking_is_set_up_in_the_right_schema
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.tear_down_tracking
          @adapter.set_up_tracking
          assert_equal (@adapter.tracking_operations?), true
        end
      end

      def test_says_it_is_not_tracking_operations_after_tracking_is_torn_down_in_the_right_schema
        @adapter.with_search_path([@right_schema,'public']) do
          @adapter.tear_down_tracking
          assert_equal (@adapter.tracking_operations?), false
        end
      end

    end

  end
end
