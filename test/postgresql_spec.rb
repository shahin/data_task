require_relative './helper.rb'
require_relative '../adapters/postgresql'

module Rake
  module TableTask

    describe PostgreSQL do

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
        Rake::TableTask::Db.with_transaction_rollback do
          Db.execute "create schema #{right_schema}"
          Db.execute "create schema #{wrong_schema}"
          test.call
          Db.execute "drop schema #{right_schema} cascade"
          Db.execute "drop schema #{wrong_schema} cascade"
        end
      end

      it "returns the current user name when called to" do
        Db.execute "create role #{test_role}"
        PostgreSQL.with_role(test_role) do
          PostgreSQL.current_user.must_equal test_role
        end
      end

      it "returns the current search path when called to" do
        Db.execute "set search_path to #{right_schema}, public"
        PostgreSQL.search_path.must_equal [right_schema, 'public']
      end

      it "resets the search path after exiting a with_search_path block" do
        Db.execute "set search_path to #{right_schema}, public"
        PostgreSQL.with_search_path([wrong_schema,'public']) do
          PostgreSQL.search_path.must_equal [wrong_schema, 'public']
        end
        PostgreSQL.search_path.must_equal [right_schema, 'public']
      end

      it "returns the first schema in the search path that contains a table when called to" do
        Db.execute "create table #{right_schema}.#{test_table} (var1 integer)"
        Db.execute "set search_path to #{wrong_schema}, #{right_schema}, 'public'"
        PostgreSQL.first_schema_for(test_table).must_equal right_schema
      end

      it "finds a table when it exists in the right schema" do
        Db.execute "create table #{test_table_right_schema} (var1 integer)"
        Db.table_exists?(test_table_right_schema).must_equal true
      end

      it "does not find a table when it does not exist in the right schema" do
        Db.table_exists?(test_table_right_schema).must_equal false
      end

      it "does not find a table when it exists in the wrong schema" do
        Db.execute "create table #{test_table_wrong_schema} (var1 integer)"
        Db.table_exists?(test_table_right_schema).must_equal false
      end

      it "creates a table in the right schema when called to" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do
            Db.create_table test_table_right_schema, nil, '(var1 text)'
            Db.table_exists?(test_table_right_schema).must_equal true
          end
        end
      end

      it "drops a table in the right schema when called to" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.execute "create table #{test_table_right_schema} (var1 text)"
            Db.execute "create table #{test_table_wrong_schema} (var1 text)"
            Db.drop_table test_table_right_schema
            Db.table_exists?(test_table_right_schema).must_equal false
            Db.table_exists?(test_table_wrong_schema).must_equal true

          end
        end
      end

      it "creates a view in the right schema when called to" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_view test_view_right_schema, "select * from information_schema.tables limit 0"
            Db.view_exists?(test_view_right_schema).must_equal true

          end
        end
      end

      it "drops a view in the right schema when called to" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_view test_view_right_schema, "select * from information_schema.tables limit 0"
            Db.drop_view test_view_right_schema
            Db.view_exists?(test_view_right_schema).must_equal false

          end
        end
      end

      it "updates the tracking table in the right schema when it creates a table" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_table test_table_right_schema, nil, '(var1 integer)'
             tracked_create = Sql.get_single_int <<-EOSQL
              select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
              where 
                relation_name = '#{test_table}' and
                relation_type = '#{Db.relation_type_values[:table]}' and
                operation = '#{Db.operation_values[:create]}'
            EOSQL
            tracked_create.must_equal 1

          end
        end
      end

      it "updates the tracking table in the right schema when it drops a table" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_table test_table_right_schema, nil, '(var1 text)'
            Db.drop_table test_table_right_schema
            still_tracking_table = Sql.get_single_int <<-EOSQL
              select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
              where 
                relation_name = '#{test_table}' and
                relation_type = '#{Db.relation_type_values[:table]}'
            EOSQL
            still_tracking_table.must_be_nil

          end
        end
      end

      it "updates the tracking table in the right schema on insert to a tracked table" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_table test_table_right_schema, nil, '(var1 text)'
            Db.execute "insert into #{test_table_right_schema} values ('a')"
            tracked_insert = Sql.get_single_int <<-EOSQL
              select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
              where 
                relation_name = '#{test_table}' and
                relation_type = '#{Db.relation_type_values[:table]}' and
                operation = '#{Db.operation_values[:insert]}'
            EOSQL
            tracked_insert.must_equal 1

          end
        end
      end

      it "updates the tracking table in the right schema on update on a tracked table" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_table test_table_right_schema, nil, '(var1 text, var2 text)'
            Db.execute "insert into #{test_table_right_schema} values ('a', 'a')"
            Db.execute "update #{test_table_right_schema} set var2 = 'b' where var1 = 'a'"

            tracked_insert = Sql.get_single_int <<-EOSQL
              select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
              where 
                relation_name = '#{test_table}' and
                relation_type = '#{Db.relation_type_values[:table]}' and
                operation = '#{Db.operation_values[:update]}'
            EOSQL
            tracked_insert.must_equal 1

          end
        end
      end

      it "updates the tracking table in the right schema on truncate of a tracked table" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          with_tracking do

            Db.create_table test_table_right_schema, nil, '(var1 text)'
            Db.truncate_table test_table_right_schema
            tracked_truncate = Sql.get_single_int <<-EOSQL
              select 1 from #{right_schema}.#{Db::TABLE_TRACKER_NAME} 
              where 
                relation_name = '#{test_table}' and
                relation_type = '#{Db.relation_type_values[:table]}' and
                operation = '#{Db.operation_values[:truncate]}'
            EOSQL
            tracked_truncate.must_equal 1

          end
        end
      end

      it "says it is tracking tables after tracking is set up in the right schema" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          Db.tear_down_tracking
          Db.set_up_tracking
          (Db.tracking_tables?).must_equal true
        end
      end

      it "says it is not tracking tables after tracking is torn down in the right schema" do
        PostgreSQL.with_search_path([right_schema,'public']) do
          Db.tear_down_tracking
          (Db.tracking_tables?).must_equal false
        end
      end

    end

  end
end
