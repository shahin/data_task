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
        table_exists?(TABLE_TRACKER_NAME)
      end

      def self.table_tracker_columns
        # replace the default datatype for time with SQLite's timestamp
        super.merge({
          :time => {:data_type => :timestamp}
        })
      end

      def self.set_up_tracking options
        tear_down_tracking options
        column_definitions = table_tracker_columns.map do |col,col_defn|
          col.to_s + ' ' + col_defn[:data_type].to_s
        end.join(', ')
        create_table TABLE_TRACKER_NAME, nil, " (#{column_definitions})", false
      end

      def self.tear_down_tracking options
        drop_table TABLE_TRACKER_NAME
      end
      
      def self.reset_tracking options
        truncate_table TABLE_TRACKER_NAME
      end

      def self.table_mtime table_name
        Sql.get_single_time <<-EOSQL
          -- assume time is UTC (Sqlite3 default) and add offset for Ruby's Time.parse 
          select datetime(max(time)) || ' -0000'
          from #{TABLE_TRACKER_NAME} 
          where relation_name = '#{table_name}'
        EOSQL
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

      def self.drop_table table_name
        Db.execute "drop table if exists #{table_name}"

        # manually cascade the drop operation to views for this table
        views_for_dropped_table = Db.execute <<-EOSQL
          select name from sqlite_master 
          where 
            type = 'view' and (
              -- add trailing space for views without where statements
              sql || ' ' like "% from #{table_name} %" or
              sql like "% join #{table_name} %"
            )
        EOSQL
        views_for_dropped_table.flatten.each do |view_name|
          drop_view view_name
        end

        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        track_drop table_name
      end

      def self.create_view view_name, select_stmt
        drop_view view_name
        Db.execute "create view #{view_name} as #{select_stmt}"
      end

      def self.drop_view view_name
        Db.execute "drop view if exists #{view_name}"
      end

      def self.track_drop table_name
        Db.execute <<-EOSQL
          delete from #{TABLE_TRACKER_NAME} 
          where 
            relation_name = '#{table_name}' and 
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
      end

      def self.table_exists? table_name, options = {}
        relation_exists?(table_name, 'table', options)
      end

      def self.view_exists? table_name, options = {}
        relation_exists?(table_name, 'view', options)
      end

      def self.truncate_table table_name
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        Db.execute "delete from #{table_name}"
        track_truncate table_name
      end

      def self.track_truncate table_name
        Db.execute <<-EOSQL
          update #{TABLE_TRACKER_NAME}
          set 
            operation = '#{operation_values[:truncate]}',
            -- Sqlite generates times at UTC and stores them without zone information
            time = datetime('now')
          where
            relation_name = '#{table_name}' and
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
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

                  update #{TABLE_TRACKER_NAME} 
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
            delete from #{TABLE_TRACKER_NAME} where 
              relation_name = '#{table_name}' and 
              relation_type = '#{relation_type_values[:table]}' and
              operation = '#{operation_values[operation]}'
            ;
          EOSQL
          Db.execute <<-EOSQL
            insert into #{TABLE_TRACKER_NAME} values (
              '#{table_name}', 
              '#{relation_type_values[:table]}', 
              '#{operation_values[operation]}', 
              datetime('now')
            );
          EOSQL
        end

        def self.relation_exists? relation_name, relation_type, options = {}
          n_matches = Sql.get_single_int <<-EOSQL
            select count(*) from sqlite_master
            where 
              name = '#{relation_name}' and
              type = '#{relation_type}'
          EOSQL
          (n_matches > 0)
        end

    end

  end
end
