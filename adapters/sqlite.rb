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

      def self.tracking_table_columns
        super.merge({
          :time => {:data_type => :timestamp}
        })
      end

      def self.set_up_tracking
        column_definitions = tracking_table_columns.map do |col,col_defn|
          col.to_s + ' ' + col_defn[:data_type].to_s
        end.join(', ')
        create_table TRACKING_TABLE_NAME, nil, " (#{column_definitions})", false
      end

      def self.tear_down_tracking
        drop_table TRACKING_TABLE_NAME
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
        return if table_name.casecmp(TRACKING_TABLE_NAME) == 0
        Db.execute "delete from #{table_name}"
        track_truncate table_name
      end

      def self.track_truncate table_name
        Db.execute <<-EOSQL
          update #{TRACKING_TABLE_NAME}
          set 
            operation = '#{operation_values[:truncate]}',
            time = datetime('now')
          where
            relation_name = '#{table_name}' and
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
      end

      def self.drop_table table_name
        return if table_name.casecmp(TRACKING_TABLE_NAME) == 0
        Db.execute "drop table if exists #{table_name}"
        track_drop table_name
      end

      def self.track_drop table_name
        Db.execute <<-EOSQL
          delete from #{TRACKING_TABLE_NAME} 
          where 
            relation_name = '#{table_name}' and 
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
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

      def self.create_table table_name, data_definition, column_definitions, track_table=true
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

      def self.operations_supported
        {
          :by_db => operations_supported_by_db,
          :by_app => [:truncate, :create]
        }
      end



      private

        def self.operations_supported_by_db
          [:update, :insert, :delete]
        end

        def self.rule_name table_name, operation
          "#{table_name}_#{operation.to_s}"
        end

        def self.create_tracking_rules table_name
          operations_supported_by_db.each do |operation|
            Db.execute <<-EOSQL
              create trigger #{self.rule_name(table_name, operation)}
                after #{operation.to_s} on #{table_name} begin

                  update #{TRACKING_TABLE_NAME} 
                  set 
                    operation = '#{operation_values[operation]}',
                    time = datetime()
                  where 
                    relation_name = '#{table_name}' and 
                    relation_type = '#{relation_type_values[:table]}'

                ;
                end
            EOSQL
          end
        end

        def self.track_creation table_name, n_tuples
          operation = :create
          Db.execute <<-EOSQL
            delete from #{TRACKING_TABLE_NAME} where 
              relation_name = '#{table_name}' and 
              relation_type = '#{relation_type_values[:table]}' and
              operation = '#{operation_values[operation]}'
            ;
          EOSQL
          Db.execute <<-EOSQL
            insert into #{TRACKING_TABLE_NAME} values (
              '#{table_name}', 
              '#{relation_type_values[:table]}', 
              '#{operation_values[operation]}', 
              datetime('now')
            );
          EOSQL
        end

        def self.clear_tracking_rules_for_table table_name
          supported_operations.each do |operation|
            Db.execute <<-EOSQL
              drop trigger if exists #{self.rule_name(table_name,operation)} on #{table_name}
            EOSQL
          end
        end

    end

  end
end
