require 'time'
require_relative './util'

module Rake
  module TableTask

    class Sql

      def self.exec sql
        Db.execute sql
      end

      def self.get_single_int sql
        r = exec(sql)
        r.values.first.first.to_i
      end

      def self.get_single_time sql
        r = exec(sql)
        return nil if r.values.first.compact.empty?
        Time.parse(r.values.first.first)
      end

    end

  end
end
