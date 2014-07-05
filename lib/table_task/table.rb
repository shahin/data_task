require_relative './db'

module Rake
  module TableTask

    # Represents a table accessed via a database, roughly analogous to the File class.
    class Table

      attr_accessor :name
      attr_accessor :adapter

      def initialize table_name, adapter
        @name = table_name
        @adapter = adapter
      end

      def exists? options={}
        @adapter.table_exists?(@name, options)
      end

      alias_method :exist?, :exists?

      def mtime
        @adapter.table_mtime(@name)
      end

      def to_s
        @name
      end

    end

  end
end
