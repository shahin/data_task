require_relative './db'

module Rake
  module DataTask

    # Represents a table accessed via a database, roughly analogous to the File class.
    class Data

      attr_accessor :name
      attr_accessor :adapter

      # @param [String] data_name an identifier for a data collection understood by the adapter
      # @param [Object] adapter that knows how to find and manipulate the data
      def initialize data_name, adapter
        @name = data_name
        @adapter = adapter
      end

      def exists? options={}
        @adapter.data_exists?(@name, options)
      end

      alias_method :exist?, :exists?

      def mtime
        @adapter.data_mtime(@name)
      end

      def to_s
        @name
      end

    end

  end
end
