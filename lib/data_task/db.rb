require 'yaml'
require 'logger'
require_relative './sql'
require_relative './util'

module Rake
  module DataTask

    class Db

      @connections = {}

      def self.persisted_connection conn_options
        @connections[conn_options]
      end

      def self.persist_connection conn, conn_options
        @connections[conn_options] = conn
      end

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
