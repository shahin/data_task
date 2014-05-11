require 'spec_helper'

describe Rake::TableTask::Sql do

  context "when asked for a single integer" do
    it "returns a single integer if the query results in a single integer" do
      expect(Rake::TableTask::Sql.get_single_int('select 1')).to be_a Integer
    end
    it "returns nil if the query results in NULL" do
      Rake::TableTask::Sql.get_single_int('select NULL').should be_nil
    end
    it "raises an error if the query results in a single non-integer" do
      expect{Rake::TableTask::Sql.get_single_int("select 'a'")}.to raise_error
    end
    it "raises an error if the query results in more than one column" do
      expect{Rake::TableTask::Sql.get_single_int('select 1,2')}.to raise_error
    end
    it "raises an error if the query results in more than one row" do
      expect{Rake::TableTask::Sql.get_single_int('select 1 union all select 2')}.to raise_error
    end
  end

end
