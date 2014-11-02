begin
  require 'pg'
rescue LoadError
  puts "The Postgres adapter requires the pg gem, which can be installed via rubygems."
end
require_relative 'support/transactions'
require_relative 'support/booleans'
require_relative 'support/connection_persistence'
require_relative 'support/qualified_names'
require_relative 'support/tracking_table'
require_relative 'postgres/trigger_manager'
require_relative '../data'

module Rake
  module DataTask

    class Postgres < Db

      @connections = {}
      extend ConnectionPersistence

      include SingleLetterBooleans
      include SqlTransactions
      include QualifiedNames

      def self.connection_options_from_uri uri
        {
          'host' => uri.host,
          'port' => uri.port,
          'username' => uri.user,
          'password' => uri.password,
          'database' => uri.path[1..-1]
        }
      end

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
      def initialize options={}
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

        # set up trackig if it isn't set up already
        set_up_tracking if !tracking_operations?
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

        rescue PG::UndefinedTable => e

          if /ERROR:  relation "(last_operations|.*\.last_operations)" does not exist/ =~ e.message
            LOG.error "Tracking is not set up in this schema. Set up tracking in this schema first."
          end
          execute "rollback;"
          raise e

        rescue PGError => e

          LOG.info e.message.chomp
          execute "rollback;"
          raise e

        end
      end

      # Check whether tracking is set up in this schema. If schema is not specified, check the
      # first schema in which we find a tracking table.
      #
      # @return [Boolean] true if all tracking assets are available in the schema; false otherwise
      def tracking_operations? schema_name=nil
        schema_name ||= first_schema_for(TABLE_TRACKER_NAME)
        return false if schema_name.nil?

        return !@trigger_manager.nil? && @trigger_manager.tracking_operations?(schema_name)
      end

      # Set up tracking mechanisms in a single schema.
      #
      # If no schema name is provided, use the first schema in the search path.
      #
      # @option options [String] :schema_name the name of the schema to begin tracking
      # @option options [Boolean] :force attempt to set up tracking even if it is already set up
      include TrackingTable
      def set_up_tracking options = {}
        schema_name = options[:schema_name]
        force_setup = options[:force]

        return if tracking_operations?(schema_name) && !!force_setup

        target_search_path = [schema_name || current_search_path_schemas.first]
        with_search_path(target_search_path) do
          create_tracking_table TABLE_TRACKER_NAME
          @trigger_manager = TriggerManager.new(self)
          @trigger_manager.set_up_tracking
        end
      end

      # Drop all tracking mechanisms in a schema.
      #
      # Note: does not drop the database-level event trigger used to track drops.
      #
      # @option options [String] :schema_name the name of the schema to stop tracking
      def tear_down_tracking options = {}
        schema_name = options[:schema_name]

        # scope all operations to a single schema
        target_search_path = [schema_name || first_schema_for(TABLE_TRACKER_NAME)]
        with_search_path(target_search_path) do
          drop_table TABLE_TRACKER_NAME
          @trigger_manager.tear_down_tracking
        end
      end

      # Clear tracking data but leave all tracking mechanisms in place.
      # @option options [String] :schema_name the name of the schema to reset tracking in
      def reset_tracking options = {}
        target_search_path = [options[:schema_name] || first_schema_for(TABLE_TRACKER_NAME)]
        with_search_path(target_search_path) do
          truncate_table TABLE_TRACKER_NAME
        end
      end

      # Get the timestamp of the most recent operation on a table.
      #
      # @param qualified_table_name [String] the name of the table, optionally qualified with its
      # schema name. If unqualified, its schema is resolved via the current search path.
      # @return [DateTime] the time of the most recent operation on the table
      def table_mtime qualified_table_name
        schema_name, table_name = parse_schema_and_object_name(qualified_table_name)
        schema_name = first_schema_for(table_name) if schema_name.nil?

        # checking the mtime of a table that does not exist should return nil
        return nil if !table_exists?(table_name) || schema_name.nil?

        with_search_path(schema_name) do
          Sql.get_single_time(
            execute <<-EOSQL
              select max(time)
              from #{schema_name}.#{TABLE_TRACKER_NAME}
              where relation_name = '#{table_name}'
            EOSQL
          )
        end
      end

      alias_method :data_mtime, :table_mtime

      def truncate_table table_name
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        execute "truncate table #{table_name}"
      end
      
      alias_method :truncate_data, :truncate_table

      def drop_table table_name
        execute "drop table if exists #{table_name} cascade"
      end

      alias_method :drop_data, :drop_table

      def table_exists? table_name, options = {}
        relation_exists? table_name, :table, options
      end

      alias_method :data_exists?, :table_exists?

      def view_exists? view_name, options = {}
        relation_exists? view_name, :view, options
      end

      def function_exists? function_name
        schema_name, function_name = parse_schema_and_object_name(function_name)
        schema_name ||= current_search_path_schemas.first

        exists = Sql.get_single_value(
          execute <<-EOSQL
          select exists(
            select TRUE
            from
              pg_catalog.pg_proc pp
              join pg_catalog.pg_namespace pn on (pp.pronamespace = pn.oid)
            where
              pp.proname = '#{function_name}' and
              pn.nspname = '#{schema_name}'
            )
          EOSQL
        )
        true?(exists)
      end

      def create_table table_name, data_definition, column_definitions, track_table=true
        schema_name, table_name = parse_schema_and_object_name(table_name)
        schema_name ||= current_search_path_schemas.first

        with_search_path([schema_name]) do

          if track_table && !tracking_operations?
            set_up_tracking
          end

          drop_table table_name
          execute <<-EOSQL
            create table #{table_name} #{column_definitions}
            #{ "as #{data_definition}" if !data_definition.nil? }
          EOSQL
          if track_table
            operations_supported_by_db_rules.each do |operation| 
              create_tracking_rule table_name, operation
            end
            @trigger_manager.create_truncate_table_trigger table_name
            track_creation table_name
          end

        end
      end
      private :create_table

      def create_view view_name, view_definition
        schema_name, view_name = parse_schema_and_object_name(view_name)
        target_search_path = [schema_name || current_search_path]

        with_search_path(target_search_path) do
          drop_view view_name
          execute <<-EOSQL
            create view #{view_name} as
            #{view_definition}
          EOSQL
        end
      end

      def drop_view view_name
        execute "drop view if exists #{view_name} cascade"
      end

      # Changes the database role for the current connection for the duration of the given block.
      # After block execution, changes back to the original role.
      # 
      # @param role [String] the name of the role to change to
      # @yield executes the block under the given role
      # @return [Object] the return value of the given block
      def with_role role
        original_role = current_user
        execute "set role #{role}"
        r = yield
        execute "set role #{original_role}"
        r
      end



      private

        def operations_supported_by_db_rules
          [:update, :insert, :delete]
        end

        # @return [String] the name of the current database user
        def current_user
          execute("select current_user").first.first
        end

        def rule_name operation
          "_datatask_#{operation.to_s}"
        end

        def create_tracking_rule table_name, operation
          schema_name, unqualified_table_name = parse_schema_and_object_name(table_name)
          qualified_table_tracker = schema_name.nil? ? TABLE_TRACKER_NAME : "#{schema_name}.#{TABLE_TRACKER_NAME}"

          execute <<-EOSQL
            create or replace rule "#{rule_name(operation)}" as
              on #{operation.to_s} to #{unqualified_table_name} do also (

                delete from #{qualified_table_tracker} where
                  relation_name = '#{unqualified_table_name}' and
                  relation_type = '#{relation_type_values[:table]}'
                  ;

                insert into #{qualified_table_tracker} values (
                  '#{unqualified_table_name}',
                  '#{relation_type_values[:table]}',
                  '#{operation_values[operation]}',
                  clock_timestamp()
                );

              )
          EOSQL
        end

        def track_creation table_name
          schema_name, unqualified_table_name = parse_schema_and_object_name(table_name)
          qualified_table_tracker = [schema_name, TABLE_TRACKER_NAME].compact.join('.')

          operation = :create
          execute <<-EOSQL
            delete from #{qualified_table_tracker} where
              relation_name = '#{unqualified_table_name}' and
              relation_type = '#{relation_type_values[:table]}'
              ;
            insert into #{qualified_table_tracker} values (
              '#{unqualified_table_name}',
              '#{relation_type_values[:table]}',
              '#{operation_values[operation]}',
              -- TODO: is this the correct timestamp for doing stuff inside transactions? mult simul txns?
              clock_timestamp()
            );
          EOSQL
        end

        def relation_exists? relation_name, relation_type, options = {}
          schema_name, unqualified_relation_name = parse_schema_and_object_name(relation_name)

          if !schema_name.nil?
            schema_conditions_sql = "table_schema ilike '#{schema_name}'"
          else
            schema_conditions_sql = "table_schema in (#{current_search_path_schemas.to_quoted_s})"
          end

          exists = Sql.get_single_value(
            execute <<-EOSQL 
              select exists(
                select 1
                from information_schema.tables 
                where 
                  table_name = '#{unqualified_relation_name}' and
                  table_type = '#{relation_type_values[relation_type]}' and
                  #{ schema_conditions_sql }
              )
            EOSQL
          )
          true?(exists)
        end

    end

  end
end
