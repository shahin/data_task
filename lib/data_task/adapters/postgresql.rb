require 'pg'
require_relative 'support/transactions'
require_relative 'support/booleans'
require_relative 'support/connection_persistence'

module Rake
  module DataTask

    class Postgres < Db

      @connections = {}
      extend ConnectionPersistence

      include StandardBooleans
      include StandardTransactions

      # Connect to a PostgreSQL database. 
      #
      # If we've already used this class to connect to the same host, port, and database with the 
      # same username, re-use that connection for this instance.
      #
      # @param [Hash] options the connection parameters
      # @option options [String] 'host' the server hostname or IP address
      # @option options [Integer] 'port' the server port number
      # @option options [String] 'database' the database name
      # @option options [String] 'username' the name of the database user to connect as
      # @option options [String] 'password' the database user's password
      # @return [Sqlite] an instance of this adapter
      def initialize options
        host = options['host'] || 'localhost'
        port = options['port'] || 5432
        database = options['database']
        username = options['username']

        # always reuse an existing connection if it matches on these connection options
        conn_options = {:host => host, :port => port, :database => database, :username => username}
        existing_connection = self.class.persisted_connection(conn_options)

        if existing_connection.nil?
          # create and persist a new connection
          @connection = PG::Connection.new(
            host,
            port,
            nil,
            nil,
            database,
            username,
            options['password'] || ''
          )
          @connection.set_notice_processor do |msg|
            if msg =~ /^ERROR:/
              LOG.error('psql') { msg.gsub(/\n/,'; ') }
            else
              LOG.info('psql') { msg.gsub(/\n/,'; ') }
            end
          end
          self.class.persist_connection(@connection, conn_options)
        else
          # reuse an existing connection
          @connection = existing_connection
        end
      end

      def [](name)
        Data.new(name, self)
      end

      def table_tracker_columns
        # upcase all enum'd column values because system tables store them in upcase
        cols = super
        cols.each do |k1,v1|
          cols[k1].each do |k2, v2|
            if k2 == :values
              cols[k1][k2].each do |k3, v3|
                cols[k1][k2][k3] = v3.upcase
              end
            end
          end
        end

        cols[:relation_type][:values][:table] = 'BASE TABLE'
        cols[:time][:data_type] = :'timestamp with time zone'
        cols
      end

      def execute sql
        connect if @connection.nil?
        begin
          r = @connection.exec sql
          r.values
        rescue PGError => e
          LOG.info e.message.chomp
          raise e
        end
      end

      def tracking_tables?
        data_exists?(TABLE_TRACKER_NAME)
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
            select max(time) 
            from #{TABLE_TRACKER_NAME} 
            where relation_name = '#{table_name}'
          EOSQL
        )
      end

      alias_method :data_mtime, :table_mtime

      def truncate_table table_name
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        execute "truncate table #{table_name}"
        track_truncate table_name
      end
      
      alias_method :truncate_data, :truncate_table

      def drop_table table_name
        execute "drop table if exists #{table_name} cascade"
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        track_drop table_name
      end

      alias_method :drop_data, :drop_table

      def track_drop table_name
        execute <<-EOSQL
          delete from #{TABLE_TRACKER_NAME} 
          where 
            relation_name = '#{table_name}' and 
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
      end

      def table_exists? table_name, options = {}
        relation_exists? table_name, :table, options
      end

      alias_method :data_exists?, :table_exists?

      def view_exists? view_name, options = {}
        relation_exists? view_name, :view, options
      end

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

      def create_view view_name, view_definition
        drop_view view_name
        execute <<-EOSQL
          create view #{view_name} as
          #{view_definition}
        EOSQL
      end

      def drop_view view_name
        execute "drop view if exists #{view_name} cascade"
      end

      def operations_supported
        {
          :by_db => operations_supported_by_db,
          :by_app => [:truncate, :create] - operations_supported_by_db
        }
      end



      private

        def operations_supported_by_db
          operations_supported_by_db_rules
        end

        def operations_supported_by_db_rules
          [:update, :insert, :delete]
        end

        def rule_name table_name, operation
          "#{table_name}_#{operation.to_s}"
        end

        # ways to prevent data integrity problems due to the timestamp being at the beginning of the transaction
        # 1. set transaction isolation level serializable
        # 2. 
        def create_tracking_rules table_name
          operations_supported_by_db_rules.each do |operation|
            execute <<-EOSQL
              create or replace rule #{rule_name(table_name, operation)} as 
                on #{operation.to_s} to #{table_name} do also (

                  delete from #{TABLE_TRACKER_NAME} where 
                    relation_name = '#{table_name}' and 
                    relation_type = '#{relation_type_values[:table]}'
                    ;

                  insert into #{TABLE_TRACKER_NAME} values (
                    '#{table_name}', 
                    '#{relation_type_values[:table]}', 
                    '#{operation_values[operation]}', 
                    clock_timestamp()
                  );

                )
            EOSQL
          end
        end

        def track_creation table_name, n_tuples
          operation = :create
          execute <<-EOSQL
            delete from #{TABLE_TRACKER_NAME} where
              relation_name = '#{table_name}' and
              relation_type = '#{relation_type_values[:table]}'
              ;
            insert into #{TABLE_TRACKER_NAME} values (
              '#{table_name}',
              '#{relation_type_values[:table]}',
              '#{operation_values[operation]}',
              clock_timestamp()
            );
          EOSQL
        end

        def track_truncate table_name
          execute <<-EOSQL
            update #{TABLE_TRACKER_NAME}
            set 
              operation = '#{operation_values[:truncate]}',
              time = clock_timestamp()
            where
              relation_name = '#{table_name}' and
              relation_type = '#{relation_type_values[:table]}'
          EOSQL
        end

        def relation_exists? relation_name, relation_type, options = {}
          options = { :schema_names => nil }.merge(options)

          if !options[:schema_names].nil?
            schema_conditions_sql = "and table_schema in (#{options[:schema_names].to_quoted_s})"
          else
            schema_conditions_sql = 'true'
          end

          n_matches = Sql.get_single_int(
            execute <<-EOSQL 
              select count(*)
              from information_schema.tables 
              where 
                table_name = '#{relation_name}' and
                table_type = '#{relation_type_values[relation_type]}' and
                #{ schema_conditions_sql }
            EOSQL
          )
          (n_matches > 0)
        end

    end

  end
end
