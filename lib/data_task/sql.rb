require 'time'

module Rake
  module TableTask

    class Sql

      # Parse a single string value into an object using the supplied type logic.
      #
      # @param r [Array] an array (table) of arrays (rows), usually resulting from a database query
      # @param &type_logic [Block] code that takes the first 
      # @raise [TypeError] if r contains more than one row or column
      # @returns [Object] the return value of &type_logic on the value in r
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

      # Get a single integer via SQL.
      #
      # @param r [Array] an array containing a single result from a query
      # @returns [Integer] the single result converted to an Integer
      def self.get_single_int r
        parse_single_value r do
          Integer(r.first.first)
        end
      end

      # Get a single time via SQL.
      #
      # @param r [Array] an array containing a single result from a query
      # @returns [Time] the single result converted to Ruby's local time
      def self.get_single_time r
        parse_single_value r do
          t = Time.parse(r.first.first)
          DateTime.parse(t.to_s)
        end
      end

    end

  end
end
