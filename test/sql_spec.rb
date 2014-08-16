require_relative './helper.rb'

module Rake
  module DataTask

    describe Sql do

      around do |test|
        @adapter = TestHelper.get_adapter_to_test_db
        @adapter.with_transaction_rollback do
          test.call
        end
      end

      context "when asked to parse a single value" do
        it "raises an error if the results array contains more than one column" do
          r = @adapter.execute('select 1,2')
          lambda {Sql.parse_single_value(r)}.must_raise(TypeError)
        end
        it "raises an error if the results array contains more than one row" do
          r = @adapter.execute('select 1 union all select 2')
          lambda {Sql.parse_single_value(r)}.must_raise(TypeError)
        end
        it "returns nil if the results array contains no rows" do
          r = @adapter.execute("select 1 where #{@adapter.falsey_value}")
          Sql.parse_single_value(r).must_be_nil
        end
        it "returns nil if the results array contains a null value" do
          r = @adapter.execute('select NULL')
          Sql.parse_single_value(r).must_be_nil
        end
      end

      context "when asked for a single integer" do
        it "returns a single integer if the query result is a single value convertible to an integer" do
          Sql.get_single_int(@adapter.execute('select 1')).must_be_kind_of Integer
        end
        it "raises an error if the query results in a single non-integer" do
          lambda {Sql.get_single_int(@adapter.execute("select 'a'"))}.must_raise(ArgumentError)
        end
      end

    end

  end
end
