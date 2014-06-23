require_relative './helper.rb'

module Rake
  module TableTask

    describe PostgreSQL do

      right_schema = "test_schema_1"
      wrong_schema = "test_schema_2"
      test_table = "#{right_schema}.test_table"
      test_view = "test_view"

      around do |test|
        Rake::TableTask::Db.with_transaction_rollback do
          Db.execute "create schema #{right_schema}"
          Db.execute "create schema #{wrong_schema}"
          test.call
          Db.execute "drop schema #{right_schema}"
          Db.execute "drop schema #{wrong_schema}"
        end
      end

      it "returns the current user name when called to" do
        assert false
      end

      it "returns the current search path when called to" do
        assert false
      end

      it "returns the first schema in the search path that contains a table when called to" do
        assert false
      end

      it "finds a table when it exists in the right schema" do
        if !Db.table_exists?(test_table)
          Db.execute <<-EOSQL
            create table #{test_table} (var1 text)
          EOSQL
        end
        Db.table_exists?(test_table).must_equal true
      end

      it "does not find a table when it does not exist in the right schema" do
        test_table_wrong_schema = "#{wrong_schema}.test_table"
        if Db.table_exists?(test_table_wrong_schema)
          Db.execute <<-EOSQL
            drop table #{test_table_wrong_schema}
          EOSQL
        end
        Db.table_exists?(test_table).must_equal false
      end

      it "creates a table in the right schema when called to" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.table_exists?(test_table).must_equal true

        end
        assert false
      end

      it "drops a table in the right schema when called to" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.drop_table test_table
          Db.table_exists?(test_table).must_equal false

        end
        assert false
      end

      it "creates a view in the right schema when called to" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.create_view test_view, "select * from #{test_table}"
          Db.view_exists?(test_view).must_equal true

        end
        assert false
      end

      it "drops a view in the right schema when called to" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.create_view test_view, "select * from #{test_table}"
          Db.drop_view test_view
          Db.view_exists?(test_view).must_equal false

        end
        assert false
      end

      it "drops a view when the underlying table is dropped" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.create_view test_view, "select * from #{test_table}"
          Db.drop_table test_table
          Db.view_exists?(test_view).must_equal false

        end
        assert false
      end

      it "updates the tracking table in the right schema when it creates a table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
           tracked_create = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{Db.relation_type_values[:table]}' and
              operation = '#{Db.operation_values[:create]}'
          EOSQL
          tracked_create.must_equal 1

        end
      end

      it "updates the tracking table in the right schema when it drops a table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.drop_table test_table
          still_tracking_table = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{Db.relation_type_values[:table]}'
          EOSQL
          still_tracking_table.must_be_nil

        end
        assert false
      end

      it "updates the tracking table in the right schema on insert to a tracked table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.execute <<-EOSQL
            insert into #{test_table} values ('a')
          EOSQL
          tracked_insert = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{Db.relation_type_values[:table]}' and
              operation = '#{Db.operation_values[:insert]}'
          EOSQL
          tracked_insert.must_equal 1

        end
        assert false
      end

      it "updates the tracking table in the right schema on update on a tracked table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text, var2 text)'
          Db.execute <<-EOSQL
            insert into #{test_table} values ('a', 'a')
          EOSQL
          Db.execute <<-EOSQL
            update #{test_table} set var2 = 'b' where var1 = 'a'
          EOSQL

          tracked_insert = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{Db.relation_type_values[:table]}' and
              operation = '#{Db.operation_values[:update]}'
          EOSQL
          tracked_insert.must_equal 1

        end
        assert false
      end

      it "updates the tracking table in the right schema on truncate of a tracked table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.truncate_table test_table
          tracked_truncate = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{Db.relation_type_values[:table]}' and
              operation = '#{Db.operation_values[:truncate]}'
          EOSQL
          tracked_truncate.must_equal 1

        end
        assert false
      end

      it "says it is tracking tables after tracking is set up in the right schema" do
        Db.tear_down_tracking
        Db.set_up_tracking
        Db.tracking_tables?.must_equal true
        assert false
      end

      it "says it is not tracking tables after tracking is torn down in the right schema" do
        Db.tear_down_tracking
        (Db.tracking_tables?).must_equal false
        assert false
      end

    end

  end
end
