require_relative './helper.rb'
require 'data_task/adapters/postgres'

module Rake
  module DataTask

    describe Postgres do

      right_schema = "test_schema_1"
      wrong_schema = "test_schema_2"
      test_table = "test_table"
      test_view = "test_view"
      test_role = "test_role"

      test_table_right_schema = "#{right_schema}.#{test_table}"
      test_table_wrong_schema = "#{wrong_schema}.#{test_table}"

      test_view_right_schema = "#{right_schema}.#{test_view}"
      test_view_wrong_schema = "#{wrong_schema}.#{test_view}"

      around do |test|
        @adapter = TestHelper.get_adapter_to_test_db
        if !@adapter.kind_of?(Rake::DataTask::Postgres)
          skip("Using adapter #{@adapter}, so skipping #{self.class} tests.")
        end

        @adapter.with_transaction_rollback do
          @adapter.execute <<-EOSQL
            create schema #{right_schema};
            create schema #{wrong_schema};
          EOSQL
          test.call
          @adapter.execute <<-EOSQL
            drop schema #{right_schema} cascade;
            drop schema #{wrong_schema} cascade;
          EOSQL
        end
      end

      it "returns the current user name when called to" do
        @adapter.execute "create role #{test_role}"
        @adapter.with_role(test_role) do
          @adapter.send(:current_user).must_equal test_role
        end
      end

      it "returns the current search path when called to" do
        @adapter.execute "set search_path to #{right_schema}, public"
        @adapter.send(:search_path).must_equal [right_schema, 'public']
      end

      it "resets the search path after exiting a with_search_path block" do
        @adapter.execute "set search_path to #{right_schema}, public"
        @adapter.with_search_path([wrong_schema,'public']) do
          @adapter.send(:search_path).must_equal [wrong_schema, 'public']
        end
        @adapter.send(:search_path).must_equal [right_schema, 'public']
      end

      it "returns the first schema in the search path that contains a table when called to" do
        @adapter.execute "create table #{right_schema}.#{test_table} (var1 integer)"
        @adapter.execute "set search_path to #{wrong_schema}, #{right_schema}, 'public'"
        @adapter.send(:first_schema_for, test_table).must_equal right_schema
      end

      it "finds a table when it exists in the right schema" do
        @adapter.execute "create table #{test_table_right_schema} (var1 integer)"
        @adapter.table_exists?(test_table_right_schema).must_equal true
      end

      it "does not find a table when it does not exist in the right schema" do
        @adapter.table_exists?(test_table_right_schema).must_equal false
      end

      it "does not find a table when it exists in the wrong schema" do
        @adapter.execute "create table #{test_table_wrong_schema} (var1 integer)"
        @adapter.table_exists?(test_table_right_schema).must_equal false
      end

      it "creates a table in the right schema when called to" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do
            @adapter.create_table test_table_right_schema, nil, '(var1 text)'
            @adapter.table_exists?(test_table_right_schema).must_equal true
          end
        end
      end

      it "drops a table in the right schema when called to" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.execute "create table #{test_table_right_schema} (var1 text)"
            @adapter.execute "create table #{test_table_wrong_schema} (var1 text)"
            @adapter.drop_table test_table_right_schema
            @adapter.table_exists?(test_table_right_schema).must_equal false
            @adapter.table_exists?(test_table_wrong_schema).must_equal true

          end
        end
      end

      it "creates a view in the right schema when called to" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_view test_view_right_schema, "select * from information_schema.tables limit 0"
            @adapter.view_exists?(test_view_right_schema).must_equal true

          end
        end
      end

      it "drops a view in the right schema when called to" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_view test_view_right_schema, "select * from information_schema.tables limit 0"
            @adapter.drop_view test_view_right_schema
            @adapter.view_exists?(test_view_right_schema).must_equal false

          end
        end
      end

      it "updates the tracking table in the right schema when it creates a table" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_table test_table_right_schema, nil, '(var1 integer)'
            tracked_create = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:create]}'
              EOSQL
            )
            tracked_create.must_equal 1

          end
        end
      end

      it "updates the tracking table in the right schema when it drops a table" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_table test_table_right_schema, nil, '(var1 text)'
            @adapter.drop_table test_table_right_schema
            still_tracking_table = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}'
              EOSQL
            )
            still_tracking_table.must_be_nil

          end
        end
      end

      it "updates the tracking table in the right schema on insert to a tracked table" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_table test_table_right_schema, nil, '(var1 text)'
            @adapter.execute "insert into #{test_table_right_schema} values ('a')"
            tracked_insert = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:insert]}'
              EOSQL
            )
            tracked_insert.must_equal 1

          end
        end
      end

      it "updates the tracking table in the right schema on update on a tracked table" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_table test_table_right_schema, nil, '(var1 text, var2 text)'
            @adapter.execute "insert into #{test_table_right_schema} values ('a', 'a')"
            @adapter.execute "update #{test_table_right_schema} set var2 = 'b' where var1 = 'a'"

            tracked_insert = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:update]}'
              EOSQL
            )
            tracked_insert.must_equal 1

          end
        end
      end

      it "updates the tracking table in the right schema on truncate of a tracked table" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.with_tracking do

            @adapter.create_table test_table_right_schema, nil, '(var1 text)'
            @adapter.truncate_table test_table_right_schema
            tracked_truncate = Sql.get_single_int(
              @adapter.execute <<-EOSQL
                select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
                where 
                  relation_name = '#{test_table}' and
                  relation_type = '#{@adapter.relation_type_values[:table]}' and
                  operation = '#{@adapter.operation_values[:truncate]}'
              EOSQL
            )
            tracked_truncate.must_equal 1

          end
        end
      end

      it "says it is tracking tables after tracking is set up in the right schema" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.tear_down_tracking
          @adapter.set_up_tracking
          (@adapter.tracking_tables?).must_equal true
        end
      end

      it "says it is not tracking tables after tracking is torn down in the right schema" do
        @adapter.with_search_path([right_schema,'public']) do
          @adapter.tear_down_tracking
          (@adapter.tracking_tables?).must_equal false
        end
      end

    end

  end
end
