require_relative './helper.rb'

module Rake
  module TableTask

    describe Db do

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

      test_table = "test_table"
      test_view = "test_view"

      it "finds a table when it exists" do
        if !@adapter.table_exists?(test_table)
          @adapter.execute <<-EOSQL
            create table #{test_table} (var1 text)
          EOSQL
        end
        @adapter.table_exists?(test_table).must_equal true
      end

      it "does not find a table when it does not exist" do
        if @adapter.table_exists?(test_table)
          @adapter.execute <<-EOSQL
            drop table #{test_table}
          EOSQL
        end
        @adapter.table_exists?(test_table).must_equal false
      end

      it "creates a table when called to" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.table_exists?(test_table).must_equal true

        end
      end

      it "drops a table when called to" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.drop_table test_table
          @adapter.table_exists?(test_table).must_equal false

        end
      end

      it "creates a view when called to" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.create_view test_view, "select * from #{test_table}"
          @adapter.view_exists?(test_view).must_equal true

        end
      end

      it "drops a view when called to" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.create_view test_view, "select * from #{test_table}"
          @adapter.drop_view test_view
          @adapter.view_exists?(test_view).must_equal false

        end
      end

      it "drops a view when the underlying table is dropped" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.create_view test_view, "select * from #{test_table}"
          @adapter.drop_table test_table
          @adapter.view_exists?(test_view).must_equal false

        end
      end

      it "updates the tracking table when it creates a table" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
           tracked_create = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:create]}'
            EOSQL
          )
          tracked_create.must_equal 1

        end
      end

      it "updates the tracking table when it drops a table" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.drop_table test_table
          still_tracking_table = Sql.get_single_int(
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}'
            EOSQL
          )
          still_tracking_table.must_be_nil

        end
      end

      it "updates the tracking table on insert to a tracked table" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.execute <<-EOSQL
            insert into #{test_table} values ('a')
          EOSQL
          tracked_insert = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:insert]}'
            EOSQL
          )
          tracked_insert.must_equal 1

        end
      end

      it "updates the tracking table on update on a tracked table" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text, var2 text)'
          @adapter.execute <<-EOSQL
            insert into #{test_table} values ('a', 'a')
          EOSQL
          @adapter.execute <<-EOSQL
            update #{test_table} set var2 = 'b' where var1 = 'a'
          EOSQL

          tracked_insert = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:update]}'
            EOSQL
          )
          tracked_insert.must_equal 1

        end
      end

      it "updates the tracking table on truncate of a tracked table" do
        @adapter.with_tracking do

          @adapter.create_table test_table, nil, '(var1 text)'
          @adapter.truncate_table test_table
          tracked_truncate = Sql.get_single_int( 
            @adapter.execute <<-EOSQL
            select 1 from #{Db::TABLE_TRACKER_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type = '#{@adapter.relation_type_values[:table]}' and
              operation = '#{@adapter.operation_values[:truncate]}'
            EOSQL
          )
          tracked_truncate.must_equal 1

        end
      end

      it "says it is tracking tables after tracking is set up" do
        @adapter.tear_down_tracking
        @adapter.set_up_tracking
        @adapter.tracking_tables?.must_equal true
      end

      it "says it is not tracking tables after tracking is torn down" do
        @adapter.tear_down_tracking
        (@adapter.tracking_tables?).must_equal false
      end

    end

  end
end
