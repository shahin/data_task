require 'yaml'
require 'logger'
require_relative './sql'
require_relative './util'

module Rake
  module DataTask

    class Adapter
      # This is an abstract base class for datastore adapters. It describes the minimum necessary
      # functionality required by DataTask of an adapter to a data storage system (e.g. filesystem, 
      # database, ...). This class should not be instantiated but should be inherited by 
      # adapter implementations.

      LOG = Logger.new(STDOUT)
      LOG.level = Logger::WARN

      # @returns [Boolean] whether the datastore is currently set up to track any data operations
      def tracking_operations?
        raise NotImplementedError
      end

      def data_mtime
        raise NotImplementedError
      end

      def [](name)
        Data.new(name, self)
      end

      def operations_supported
        {
          :by_db => [],
          :by_app => []
        }
      end

    end

  end
end
