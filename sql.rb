require 'time'
require_relative './util'
require_relative './db'

module Rake
  module TableTask

    class Sql

      def self.get_array sql
        Db.execute(sql)
      end

      def self.parse_single_value r, &type_logic
        if r.nil? || r.empty? || r == [[]] || r == [[nil]]
          return nil
        elsif r.length > 1
          raise TypeError, 'Query must result in a single row'
        elsif r.first.length > 1
          raise TypeError, 'Query must result in a single column'
        end
        yield(r)
      end

      def self.get_single_int sql
        r = get_array(sql)
        parse_single_value r do
          Integer(r.first.first)
        end
      end

      def self.get_single_time sql
        r = get_array(sql)
        parse_single_value r do
          Time.parse(r.first.first)
        end
      end

    end

  end
end
