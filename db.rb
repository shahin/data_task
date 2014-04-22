require 'logger'
require_relative './sql'

module Rake
  module TableTask

    class Db

      LOG = Logger.new(STDOUT)
      TRACKING_TABLE_NAME = 'tracking'

      ADAPTERS = %w'postgresql sqlite'.map{ |a| a.to_sym }
      @@adapters = Hash.new
      @@config = nil

      @connection = nil

      def self.config
        @@config || @@config = YAML.load_file('database.yml')
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
          raise "Failed to load adapter class #{adapter_name}: #{e.message}"
        end

        klass
      end
      
      def self.adapter
        adapter_class(config['adapter'])
      end

      def self.connect
        adapter.connect
      end

      def self.execute sql
        adapter.execute sql
      end

      def self.tracking_tables?
        adapter.tracking_tables?
      end

      def self.set_up_tracking
        adapter.set_up_tracking
      end

      def self.tear_down_tracking
        adapter.tear_down_tracking
      end
      
      def self.reset_tracking
        adapter.reset_tracking
      end

      def self.table_mtime table_name
        adapter.table_mtime(table_name)
      end

      def self.create_table table_name, data_definition, column_definitions, track_table=true
        adapter.create_table(table_name, data_definition, column_definitions, track_table)
      end

      def self.drop_table table_name
        adapter.table_mtime(table_name)
      end

      def self.truncate_table table_name
        adapter.table_mtime(table_name)
      end

      def self.table_exists? table_name, schema_names
        adapter.table_exists?(table_name, schema_names)
      end

    end

  end
end
