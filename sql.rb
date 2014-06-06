require 'time'

module Rake
  module TableTask

    class Sql

      def self.get_array sql
        Db.execute(sql)
      end

      def self.truthy_value
        Db.truthy_value
      end

      def self.falsey_value
        Db.falsey_value
      end

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
      # @param sql [String] the SQL to produce the integer
      # @returns [Integer] any valid value for this Ruby type
      def self.get_single_int sql
        r = get_array(sql)
        parse_single_value r do
          Integer(r.first.first)
        end
      end

      # Get a single time via SQL.
      #
      # @param sql [String] the SQL to produce the time string with timezone info
      # @returns [DateTime] the time produced by the SQL, converted to Ruby's local time
      def self.get_single_time sql
        r = get_array(sql)
        parse_single_value r do
          DateTime.parse(r.first.first)
        end
      end

    end

  end
end
