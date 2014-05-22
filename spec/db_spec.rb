require 'spec_helper'

module Rake
  module TableTask

    describe Db do

      test_table = "test"

      it "finds a table when it exists" do
        if !Db.table_exists?(test_table)
          Db.execute <<-EOSQL
            create table #{test_table} (var1 text)
          EOSQL
        end
        expect(Db.table_exists?(test_table)).to be_true
      end

      it "does not find a table when it does not exist" do
        if Db.table_exists?(test_table)
          Db.execute <<-EOSQL
            drop table #{test_table}
          EOSQL
        end
        expect(Db.table_exists?(test_table)).to be_false
      end

      it "creates a table when called to" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          expect(Db.table_exists?(test_table)).to be_true

        end
      end

      it "drops a table when called to" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.drop_table test_table
          expect(Db.table_exists?(test_table)).to be_false

        end
      end

      it "updates the tracking table when it creates a table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
           tracked_create = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TRACKING_TABLE_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type ilike 'table' and
              operation ilike 'create'
          EOSQL
          expect(tracked_create).to be_true

        end
      end

      it "updates the tracking table when it drops a table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.drop_table test_table
          still_tracking_table = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TRACKING_TABLE_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type ilike 'table'
          EOSQL
          expect(still_tracking_table).to be_false

        end
      end

      it "updates the tracking table on insert to a tracked table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.execute <<-EOSQL
            insert into #{test_table} values ('a')
          EOSQL
          tracked_insert = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TRACKING_TABLE_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type ilike 'table' and
              operation ilike 'insert'
          EOSQL
          expect(tracked_insert).to be_true

        end
      end

      it "updates the tracking table on update on a tracked table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.execute <<-EOSQL
            insert into #{test_table} values ('a')
          EOSQL
          Db.execute <<-EOSQL
            update #{test_table} set var1 = 'b' where var1 = 'a'
          EOSQL

          tracked_insert = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TRACKING_TABLE_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type ilike 'table' and
              operation ilike 'update'
          EOSQL
          expect(tracked_insert).to be_true

        end
      end

      it "updates the tracking table on truncate of a tracked table" do
        with_tracking do

          Db.create_table test_table, nil, '(var1 text)'
          Db.truncate_table test_table
          tracked_truncate = Sql.get_single_int <<-EOSQL
            select 1 from #{Db::TRACKING_TABLE_NAME} 
            where 
              relation_name = '#{test_table}' and
              relation_type ilike 'table' and
              operation = 'truncate'
          EOSQL
          expect(tracked_truncate).to be_true

        end
      end

      it "says it is tracking tables after tracking is set up" do
        Db.tear_down_tracking
        Db.set_up_tracking
        expect(Db.tracking_tables?).to be_true
      end

      it "says it is not tracking tables after tracking is torn down" do
        Db.tear_down_tracking
        expect(Db.tracking_tables?).to be_false
      end

    end

  end
end
