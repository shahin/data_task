require 'sqlite3'
require_relative 'support/transactions'
require_relative 'support/booleans'

module Rake
  module DataTask

    class Sqlite < Db

      # Connect to an Sqlite database.
      #
      # @param [Hash] options the connection parameters
      # @option options [String] 'database' the database name
      # @return [Sqlite] an instance of this adapter
      def initialize options
        @connection = SQLite3::Database.new(options['database'] || 'temp')
      end

      def execute sql
        connect if @connection.nil?
        begin
          @connection.execute sql
        rescue SQLite3::SQLException => e
          LOG.info e.message.chomp
          raise e
        end
      end

      include NumericBooleans
      include StandardTransactions

      def tracking_tables?
        table_exists?(TABLE_TRACKER_NAME)
      end

      def table_tracker_columns
        # replace the default datatype for time with SQLite's timestamp
        super.merge({
          :time => {:data_type => :timestamp}
        })
      end

      def set_up_tracking
        tear_down_tracking
        column_definitions = table_tracker_columns.map do |col,col_defn|
          col.to_s + ' ' + col_defn[:data_type].to_s
        end.join(', ')
        create_table TABLE_TRACKER_NAME, nil, " (#{column_definitions})", false
      end

      def tear_down_tracking
        drop_table TABLE_TRACKER_NAME
      end
      
      def reset_tracking
        truncate_table TABLE_TRACKER_NAME
      end

      def table_mtime table_name
        Sql.get_single_time(
        execute <<-EOSQL
          -- assume time is UTC (Sqlite3 default) and add offset for Ruby's Time.parse 
          select datetime(max(time)) || ' -0000'
          from #{TABLE_TRACKER_NAME} 
          where relation_name = '#{table_name}'
        EOSQL
        )
      end

      alias_method :data_mtime, :table_mtime

      def create_table table_name, data_definition, column_definitions, track_table=true
        drop_table table_name
        execute <<-EOSQL
          create table #{table_name} #{column_definitions}
          #{ "as #{data_definition}" if !data_definition.nil? }
        EOSQL
        if track_table
          create_tracking_rules(table_name)
          track_creation table_name, 0
        end
      end

      alias_method :create_data, :create_table

      def drop_table table_name
        execute "drop table if exists #{table_name}"

        # manually cascade the drop operation to views for this table
        views_for_dropped_table = execute <<-EOSQL
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

      alias_method :drop_data, :drop_table

      def create_view view_name, select_stmt
        drop_view view_name
        execute "create view #{view_name} as #{select_stmt}"
      end

      def drop_view view_name
        execute "drop view if exists #{view_name}"
      end

      def track_drop table_name
        execute <<-EOSQL
          delete from #{TABLE_TRACKER_NAME} 
          where 
            relation_name = '#{table_name}' and 
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
      end

      def table_exists? table_name, options = {}
        relation_exists?(table_name, 'table', options)
      end

      alias_method :data_exists?, :table_exists?

      def view_exists? table_name, options = {}
        relation_exists?(table_name, 'view', options)
      end

      def truncate_table table_name
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        execute "delete from #{table_name}"
        track_truncate table_name
      end

      alias_method :truncate_data, :truncate_table

      def track_truncate table_name
        execute <<-EOSQL
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

      def operations_supported
        {
          :by_db => operations_supported_by_db,
          :by_app => [:truncate, :create]
        }
      end

      def [](name)
        Data.new(name, self)
      end



      private

        def operations_supported_by_db
          [:update, :insert, :delete]
        end

        def rule_name table_name, operation
          "#{table_name}_#{operation.to_s}"
        end

        def create_tracking_rules table_name
          operations_supported_by_db.each do |operation|
            execute <<-EOSQL
              create trigger #{rule_name(table_name, operation)}
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

        def track_creation table_name, n_tuples
          operation = :create
          execute <<-EOSQL
            delete from #{TABLE_TRACKER_NAME} where 
              relation_name = '#{table_name}' and 
              relation_type = '#{relation_type_values[:table]}' and
              operation = '#{operation_values[operation]}'
            ;
          EOSQL
          execute <<-EOSQL
            insert into #{TABLE_TRACKER_NAME} values (
              '#{table_name}', 
              '#{relation_type_values[:table]}', 
              '#{operation_values[operation]}', 
              datetime('now')
            );
          EOSQL
        end

        def relation_exists? relation_name, relation_type, options = {}
          n_matches = Sql.get_single_int(
          execute <<-EOSQL
            select count(*) from sqlite_master
            where 
              name = '#{relation_name}' and
              type = '#{relation_type}'
          EOSQL
          )
          (n_matches > 0)
        end

    end

  end
end
