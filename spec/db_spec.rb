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
        Db.create_table test_table, nil, '(var1 text)'
        expect(Db.table_exists?(test_table)).to be_true
      end

      it "drops a table when called to" do
        Db.create_table test_table, nil, '(var1 text)'
        Db.drop_table test_table
        expect(Db.table_exists?(test_table)).to be_false
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
