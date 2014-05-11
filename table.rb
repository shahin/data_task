require_relative './db'

module Rake
  module TableTask

    # Represents a table accessed via a database, roughly analogous to the File class.
    class Table

      def initialize table_name, data_definition=nil, column_definitions=nil
        @name = table_name
        self.class.create_as(@name, data_definition, column_definitions)
      end

      def mtime table_name
        self.class.mtime(@name)
      end

      def self.exists? table_name, schema_names = ['public']
        Db.table_exists?(table_name, schema_names)
      end
      self.singleton_class.send(:alias_method, :exist?, :exists?)

      def self.mtime table_name
        Db.table_mtime(table_name)
      end

      def self.create table_name, column_definitions
        create_as table_name, nil, column_definitions
      end

      def self.create_as table_name, data_definition, column_definitions=nil
        Db.create_table table_name, data_definition, column_definitions
      end

      def self.tnmatch pattern, table_name
        File.fnmatch(pattern, table_name)
      end
      self.singleton_class.send(:alias_method, :tnmatch?, :tnmatch)

    end

  end
end
