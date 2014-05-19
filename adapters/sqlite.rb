require 'sqlite3'
require_relative 'support/transactions'
require_relative 'support/booleans'

module Rake
  module TableTask

    class Sqlite < Db

      @@adapters[:sqlite] = self

      def self.connect
        @connection = SQLite3::Database.new(config['database'])
      end

      def self.execute sql
        connect if @connection.nil?
        begin
          @connection.execute sql
        rescue SQLite3::SQLException => e
          LOG.info e.message.chomp
          raise e
        end
      end

      extend NumericBooleans
      extend StandardTransactions

      def self.tracking_tables?
        table_exists?(TRACKING_TABLE_NAME)
      end

      def self.set_up_tracking
        Db.execute <<-EOSQL
          create table #{TRACKING_TABLE_NAME} (
            relation_name text,
            relation_type text,
            operation text,
            time timestamp
          )
        EOSQL
      end

      def self.tear_down_tracking
        Db.execute "drop table if exists #{TRACKING_TABLE_NAME}"
      end
      
      def self.reset_tracking
        truncate_table TRACKING_TABLE_NAME
      end

      def self.table_mtime table_name
        Sql.get_single_time <<-EOSQL
          select max(time) 
          from #{TRACKING_TABLE_NAME} 
          where relation_name = '#{table_name}'
        EOSQL
      end

      def self.truncate_table table_name
        Db.execute "delete from #{table_name}"
      end

      def self.drop_table table_name
        Db.execute "drop table if exists #{table_name} cascade"
      end

      def self.table_exists? table_name, options = {}
        n_matches = Sql.get_single_int <<-EOSQL
          select count(*) from sqlite_master
          where 
            name = '#{table_name}' and
            type = 'table'
        EOSQL
        (n_matches > 0)
      end

      def self.create_table table_name, data_definition, column_definitions, track_table
        drop_table table_name
        Db.execute <<-EOSQL
          create table #{table_name} #{column_definitions}
          #{ "as #{data_definition}" if !data_definition.nil? }
        EOSQL
        if track_table
          create_tracking_rules(table_name)
          track_creation table_name, 0
        end
      end



      private

        def self.operations_supported 
          {
            :by_db_rule => ['update', 'insert', 'delete'],
            :by_app => ['create', 'drop']
          }
        end

        def self.rule_name table_name, operation
          "#{table_name}_#{operation}"
        end

        def self.create_tracking_rules table_name
          operations_supported[:by_db_rule].each do |operation|
            Db.execute <<-EOSQL
              create trigger #{self.rule_name(table_name,operation)} as 
                after #{operation} on #{table_name} begin (
                  update #{TRACKING_TABLE_NAME} 
                  set 
                    operation = '#{operation}',
                    mtime = datetime()
                  where 
                    relation_name = '#{table_name}' and 
                    relation_type = 'TABLE'
                );
                end
            EOSQL
          end
        end

        def self.track_creation table_name, n_tuples
          operation = 'create'
          Sql.exec <<-EOSQL
            delete from #{TRACKING_TABLE_NAME} where 
              relation_name = '#{table_name}' and 
              relation_type = 'TABLE' and
              operation = '#{operation}'
              ;
            insert into #{Db::TRACKING_TABLE_NAME} values (
              '#{table_name}', 'TABLE', '#{operation}', now()
            );
          EOSQL
        end

        def self.clear_tracking_rules_for_table table_name
          supported_operations.each do |operation|
            Sql.exec <<-EOSQL
              drop trigger if exists #{self.rule_name(table_name,operation)} on #{table_name}
            EOSQL
          end
        end

    end

  end
end
