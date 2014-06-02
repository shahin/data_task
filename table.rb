require_relative './db'

module Rake
  module TableTask

    # Represents a table accessed via a database, roughly analogous to the File class.
    class Table

      def initialize table_name, data_definition=nil, column_definitions=nil
        @name = table_name
        self.class.create_as(@name, data_definition, column_definitions)
      end

      def mtime
        self.class.mtime(@name)
      end

      def self.exists? table_name, options={}
        Db.table_exists?(table_name, options)
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

      def self.drop table_name
        Db.drop_table table_name
      end

    end

  end
end
