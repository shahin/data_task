require 'yaml'
require 'logger'
require_relative './sql'
require_relative './util'

module Rake
  module TableTask

    class Db

      LOG = Logger.new(STDOUT)
      LOG.level = Logger::WARN

      TABLE_TRACKER_NAME = 'last_operations'

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

      @@adapters = Hash.new
      @@config_path = File.join(File.dirname(__FILE__), 'config/database.yml')
      @@config = nil

      @connection = nil

      def self.config
        db_env = ENV['TABLETASK_ENV'] || 'sqlite_test'
        @@config || @@config = YAML.load_file(@@config_path)[db_env]
      end

      def self.table_tracker_columns
        # call super.merge({overrides}) in Adapter.table_tracker_columns
        default_adapter_implementation(adapter, __method__) { TABLE_TRACKER_COLUMNS } || 
          adapter.table_tracker_columns
      end

      def self.operation_values
        table_tracker_columns[:operation][:values]
      end

      def self.relation_type_values
        table_tracker_columns[:relation_type][:values]
      end

      def self.adapter_class adapter_name
        adapter_name = adapter_name.to_sym

        if (klass = @@adapters[adapter_name]).nil?
          begin
            require_relative "adapters/#{adapter_name}"
          rescue LoadError => e
            raise "Failed to load file for adapter #{adapter_name}: #{e.message}"
          end
        end

        if (klass = @@adapters[adapter_name]).nil?
          raise "Failed to load adapter class #{adapter_name}"
        end

        klass
      end
      
      def self.adapter
        adapter_class(config['adapter'])
      end

      def self.connect
        assert_adapter_implementation adapter, __method__
        adapter.connect
      end

      def self.execute sql
        assert_adapter_implementation adapter, __method__
        adapter.execute sql
      end

      def self.truthy_value
        assert_adapter_implementation adapter, __method__
        adapter.truthy_value
      end

      def self.falsey_value
        assert_adapter_implementation adapter, __method__
        adapter.falsey_value
      end

      def self.tracking_tables?
        assert_adapter_implementation adapter, __method__
        adapter.tracking_tables?
      end

      def self.set_up_tracking
        assert_adapter_implementation adapter, __method__
        adapter.set_up_tracking
      end

      def self.tear_down_tracking
        assert_adapter_implementation adapter, __method__
        adapter.tear_down_tracking
      end
      
      def self.reset_tracking
        assert_adapter_implementation adapter, __method__
        adapter.reset_tracking
      end

      # @returns a timestamp with timezone information parseable by Ruby's Time.parse
      def self.table_mtime table_name
        assert_adapter_implementation adapter, __method__
        adapter.table_mtime(table_name)
      end

      def self.create_table table_name, data_definition, column_definitions, track_table=true
        assert_adapter_implementation adapter, __method__
        adapter.create_table(table_name, data_definition, column_definitions, track_table)
      end

      def self.drop_table table_name
        assert_adapter_implementation adapter, __method__
        adapter.drop_table(table_name)
      end

      def self.create_view view_name, view_definition
        assert_adapter_implementation adapter, __method__
        adapter.create_view(view_name, view_definition)
      end

      def self.drop_view view_name
        assert_adapter_implementation adapter, __method__
        adapter.drop_view(view_name)
      end

      def self.truncate_table table_name
        assert_adapter_implementation adapter, __method__
        adapter.truncate_table(table_name)
      end

      def self.table_exists? table_name, options = {}
        assert_adapter_implementation adapter, __method__
        adapter.table_exists?(table_name, options)
      end

      def self.view_exists? table_name, options = {}
        assert_adapter_implementation adapter, __method__
        adapter.view_exists?(table_name, options)
      end

      def self.with_transaction_commit &block
        assert_adapter_implementation adapter, __method__
        adapter.with_transaction_commit &block
      end

      def self.with_transaction_rollback &block
        assert_adapter_implementation adapter, __method__
        adapter.with_transaction_rollback &block
      end

      def self.with_transaction do_commit, &block
        assert_adapter_implementation adapter, __method__
        adapter.with_transaction do_commit, &block
      end


      private
        
        # Prevent infinite recursion due to an unimplmented abstract method
        #
        # When a pass-through abstract method is unimplemented in the child class, the base class
        # calls the method on the child class, which falls back on the base class implementation,
        # which calls the method on the child class, ad infinitum. This assert raises an error
        # if it is called in the context of the adapter (the child class). If we call this assert
        # in a base class's implementation, then raising this error breaks recursion in the second
        # call of the unimplemented method (the base class's call, then the child class's call).
        def self.assert_adapter_implementation adapter, method
          default_adapter_implementation(adapter, method) {
            raise NotImplementedError, 
              "Abstract method #{method} is not implemented in the adapter #{self}."
          }
        end

        def self.default_adapter_implementation adapter, method, &default
          if adapter == self
            default.call
          end
        end

    end

  end
end
