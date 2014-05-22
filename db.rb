require 'yaml'
require 'logger'
require_relative './sql'
require_relative './util'

module Rake
  module TableTask

    class Db

      LOG = Logger.new(STDOUT)
      LOG.level = Logger::WARN

      TRACKING_TABLE_NAME = 'tracking'

      @@adapters = Hash.new
      @@config_path = 'config/database.yml'
      @@config = nil

      @connection = nil

      def self.config
        @@config || @@config = YAML.load_file(@@config_path)
      end

      def self.tracking_table_columns
        { 
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
      end

      def self.operation_values
        tracking_table_columns[:operation][:values]
      end

      def self.relation_type_values
        tracking_table_columns[:relation_type][:values]
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

      def self.truncate_table table_name
        assert_adapter_implementation adapter, __method__
        adapter.truncate_table(table_name)
      end

      def self.table_exists? table_name, options = {}
        assert_adapter_implementation adapter, __method__
        adapter.table_exists?(table_name, options)
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
        
        def self.assert_adapter_implementation adapter, method
          if adapter == self
            raise NotImplementedError, 
              "Abstract method #{method} is not implemented in the adapter #{self}."
          end
        end

    end

  end
end
