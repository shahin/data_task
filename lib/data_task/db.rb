require 'yaml'
require 'logger'
require_relative './sql'
require_relative './util'
require_relative './adapter'

module Rake
  module DataTask

    class Db < Adapter
      # This is the base class for SQL-compliant relational databases. It contains utility methods
      # that probably don't vary across databases, and it shouldn't be instantiated.

      LOG = Logger.new(STDOUT)
      LOG.level = Logger::WARN

      TABLE_TRACKER_NAME = 'last_operations'

      # enumerate case-sensitive, DBMS-specific values that we store in tracking tables
      # this can be overridden in child classes for specific databases
      TABLE_TRACKER_COLUMNS = { 
        :relation_name => { :data_type => :text },
        :relation_type => {
          :data_type => :text,
          :values => {
            :table => 'table', 
            :view => 'view'
          }
        },
        :operation => {
          :data_type => :text,
          :values => {
            :create => 'create', 
            :insert => 'insert',
            :update => 'update',
            :truncate => 'truncate',
            :delete => 'delete'
          }
        },
        :time => { :data_type => :timestamp }
      }

      def table_tracker_columns
        TABLE_TRACKER_COLUMNS
      end

      def operation_values
        table_tracker_columns[:operation][:values]
      end

      def relation_type_values
        table_tracker_columns[:relation_type][:values]
      end

    end

  end
end
