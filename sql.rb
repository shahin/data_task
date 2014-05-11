require 'time'
require_relative './util'
require_relative './db'

module Rake
  module TableTask

    class Sql

      def self.exec sql
        Db.execute sql
      end

      def self.get_single_int sql
        r = exec(sql)
        if r.values.first.first.nil?
          nil
        elsif r.values.length > 1
          raise TypeError, 'Query must result in a single row'
        elsif r.values.first.length > 1
          raise TypeError, 'Query must result in a single column'
        else
          Integer(r.values.first.first)
        end
      end

      def self.get_single_time sql
        r = exec(sql)
        return nil if r.values.first.compact.empty?
        Time.parse(r.values.first.first)
      end

    end

  end
end
