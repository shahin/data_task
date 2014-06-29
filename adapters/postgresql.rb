require 'pg'
require_relative 'support/transactions'
require_relative 'support/booleans'

module Rake
  module TableTask

    class PostgreSQL < Db

      @@adapters[:postgresql] = self

      def self.table_tracker_columns
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

      def self.connect
        @connection = PG::Connection.new(
          config['host'] || 'localhost',
          config['port'] || 5432,
          nil,
          nil,
          config['database'],
          config['username'],
          config['password'] || ''
        )
        @connection.set_notice_processor do |msg|
          if msg =~ /^ERROR:/
            LOG.error('psql') { msg.gsub(/\n/,'; ') }
          else
            LOG.info('psql') { msg.gsub(/\n/,'; ') }
          end
        end
      end

      def self.execute sql
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

      extend StandardBooleans
      extend StandardTransactions

      def self.tracking_tables?
        table_exists?(TABLE_TRACKER_NAME)
      end

      def self.set_up_tracking
        tear_down_tracking
        column_definitions = table_tracker_columns.map do |col,col_defn|
          col.to_s + ' ' + col_defn[:data_type].to_s
        end.join(', ')
        create_table TABLE_TRACKER_NAME, nil, " (#{column_definitions})", false
      end

      def self.tear_down_tracking
        drop_table TABLE_TRACKER_NAME
      end
      
      def self.reset_tracking
        truncate_table TABLE_TRACKER_NAME
      end

      def self.table_mtime qualified_table_name
        schema_name, table_name = parse_schema_and_table_name(qualified_table_name)
        schema_name = first_schema_for(table_name) if schema_name.nil?

        with_search_path(schema_name) do
          Sql.get_single_time <<-EOSQL
            select max(time)
            from #{schema_name}.#{TABLE_TRACKER_NAME}
            where relation_name = '#{table_name}'
          EOSQL
        end
      end

      def self.truncate_table table_name
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        Db.execute "truncate table #{table_name}"
        track_truncate table_name
      end

      def self.drop_table table_name
        Db.execute "drop table if exists #{table_name} cascade"
        return if table_name.casecmp(TABLE_TRACKER_NAME) == 0
        track_drop table_name
      end

      def self.track_drop table_name
        schema_name, unqualified_table_name = parse_schema_and_table_name(table_name)
        table_tracker_name = schema_name.nil? ? TABLE_TRACKER_NAME : "#{schema_name}.#{TABLE_TRACKER_NAME}"

        if table_exists?(table_tracker_name)
          Db.execute <<-EOSQL
            delete from #{table_tracker_name}
            where
              relation_name = '#{unqualified_table_name}' and 
              relation_type = '#{relation_type_values[:table]}'
          EOSQL
        end
      end

      def self.table_exists? table_name, options = {}
        relation_exists? table_name, :table, options
      end

      def self.view_exists? view_name, options = {}
        relation_exists? view_name, :view, options
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

      def self.create_view view_name, view_definition
        drop_view view_name
        Db.execute <<-EOSQL
          create view #{view_name} as
          #{view_definition}
        EOSQL
      end

      def self.drop_view view_name
        Db.execute "drop view if exists #{view_name} cascade"
      end

      def self.operations_supported
        {
          :by_db => operations_supported_by_db,
          :by_app => [:truncate, :create] - operations_supported_by_db
        }
      end



      private

        def self.operations_supported_by_db
          operations_supported_by_db_rules
        end

        def self.operations_supported_by_db_rules
          [:update, :insert, :delete]
        end

        # Split a table name qualified with a schema name into separate strings for schema and 
        # table names.
        #
        # @returns [String, String] the schema name and table name, separately, for table_name. If
        # table_name is unqualified with the schema name, return [nil, table_name].
        def self.parse_schema_and_table_name table_name
          return [nil, table_name] if table_name.count('.') == 0

          if table_name.count('.') > 1
            raise "Invalid relation reference #{table_name} (only one '.' is allowed)"
          end

          schema_name, table_name = table_name.split('.')
          [schema_name, table_name]
        end

        # @returns [Array] the ordered schema names in the search path as strings
        def self.search_path
          current_search_path = Db.execute("show search_path").first.first.split(',').map { |s| s.strip }
          username = current_user

          # the default search path begins with a symbolic reference to the current username
          # if that reference is in the search path, replace it with the resolved current username
          if current_search_path.first == '"$user"'
            user_schema_exists = Db.execute <<-EOSQL
              select 1
              from information_schema.schemata 
              where schema_name = '#{username}'
            EOSQL

            if user_schema_exists.nil? || user_schema_exists.first.nil?
              current_search_path = current_search_path[1..-1]
            else
              current_search_path = [username] + current_search_path[1..-1]
            end
          end

          current_search_path.map(&:downcase)
        end

        # @returns [String] the name of the current database user
        def self.current_user
          Db.execute("select current_user").first.first
        end

        # @returns [String] the name of the first schema in the search path containing table_name
        def self.first_schema_for table_name
          return if !table_exists?(table_name)
          schema_name, unqualified_table_name = parse_schema_and_table_name(table_name)

          search_path_when_stmts = []
          search_path.each_with_index do |s,i| 
            search_path_when_stmts << "when table_schema = '#{s}' then #{(i+1).to_s}"
          end

          schema_name = Db.execute <<-EOSQL
            select 
              table_schema,
              search_order
            from (
              select 
                table_schema, 
                table_name,
                case 
                  #{search_path_when_stmts.join(' ')}
                  else 'NaN'::float 
                end as search_order
              from information_schema.tables
              where table_name ilike '#{unqualified_table_name}'
              ) a
            order by search_order
            limit 1
          EOSQL
          schema_name.first.first
        end

        def self.rule_name table_name, operation
          "#{table_name}_#{operation.to_s}"
        end

        def self.create_tracking_rules table_name
          schema_name, unqualified_table_name = parse_schema_and_table_name(table_name)
          qualified_table_tracker = schema_name.nil? ? TABLE_TRACKER_NAME : "#{schema_name}.#{TABLE_TRACKER_NAME}"

          operations_supported_by_db_rules.each do |operation|
            Db.execute <<-EOSQL
              create or replace rule "#{self.rule_name(table_name, operation)}" as
                on #{operation.to_s} to #{table_name} do also (

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
        end

        def self.track_creation table_name, n_tuples
          schema_name, unqualified_table_name = parse_schema_and_table_name(table_name)
          qualified_table_tracker = schema_name.nil? ? TABLE_TRACKER_NAME : "#{schema_name}.#{TABLE_TRACKER_NAME}"

          operation = :create
          Db.execute <<-EOSQL
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
          EOSQL
        end

        def self.track_truncate table_name
          schema_name, unqualified_table_name = parse_schema_and_table_name(table_name)
          qualified_table_tracker = schema_name.nil? ? TABLE_TRACKER_NAME : "#{schema_name}.#{TABLE_TRACKER_NAME}"

          Db.execute <<-EOSQL
            update #{qualified_table_tracker}
            set 
              operation = '#{operation_values[:truncate]}',
              time = clock_timestamp()
            where
              relation_name = '#{unqualified_table_name}' and
              relation_type = '#{relation_type_values[:table]}'
          EOSQL
        end

        def self.relation_exists? relation_name, relation_type, options = {}
          schema_name, unqualified_relation_name = parse_schema_and_table_name(relation_name)

          if !schema_name.nil?
            schema_conditions_sql = "table_schema ilike '#{schema_name}'"
          else
            schema_conditions_sql = "table_schema in (#{search_path.to_quoted_s})"
          end

          n_matches = Sql.get_single_int <<-EOSQL 
            select count(*)
            from information_schema.tables 
            where 
              table_name = '#{unqualified_relation_name}' and
              table_type = '#{relation_type_values[relation_type]}' and
              #{ schema_conditions_sql }
          EOSQL
          (n_matches > 0)
        end

        def self.with_search_path schemas
          original_search_path = search_path
          Db.execute "set search_path to #{Array(schemas).join(',')}"
          r = yield
          Db.execute "set search_path to #{original_search_path.join(',')}"
          r
        end

        def self.with_role role
          original_role = current_user
          Db.execute "set role #{role}"
          r = yield
          Db.execute "set role #{original_role}"
          r
        end

    end

  end
end
