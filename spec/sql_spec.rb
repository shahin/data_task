require 'spec_helper'

module Rake
  module TableTask

    describe Sql do

      context "when asked to parse a single value" do
        it "raises an error if the results array contains more than one column" do
          r = Sql.get_array('select 1,2')
          expect{Sql.parse_single_value(r)}.to raise_error(TypeError)
        end
        it "raises an error if the results array contains more than one row" do
          r = Sql.get_array('select 1 union all select 2')
          expect{Sql.parse_single_value(r)}.to raise_error(TypeError)
        end
        it "returns nil if the results array contains no rows" do
          r = Sql.get_array('select 1 where false')
          Sql.parse_single_value(r).should be_nil
        end
        it "returns nil if the results array contains a null value" do
          r = Sql.get_array('select NULL')
          Sql.parse_single_value(r).should be_nil
        end
      end

      context "when asked for a single integer" do
        it "returns a single integer if the query result is a single value convertible to an integer" do
          expect(Sql.get_single_int('select 1')).to be_a Integer
        end
        it "raises an error if the query results in a single non-integer" do
          expect{Sql.get_single_int("select 'a'")}.to raise_error(ArgumentError)
        end
      end

    end

  end
end
