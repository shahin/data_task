require 'time'
require_relative './connection.rb'

class Array
  def to_quoted_s
    commad = self.join("','")
    "'#{commad}'"
  end
end

module Sql

  def self.connection
    Connection
  end

  def self.exec sql
    connection.execute sql
  end

  def self.get_single_int sql
    r = exec(sql)
    r.values.first.first.to_i
  end

  def self.get_single_time sql
    r = exec(sql)
    Time.parse(r.values.first.first)
  end

end
