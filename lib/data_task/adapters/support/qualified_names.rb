module Rake
  module DataTask
    class Db
      
      module QualifiedNames

        # Split a table name qualified with a schema name into separate strings for schema and 
        # table names.
        #
        # @return [String, String] the schema name and table name, separately, for table_name. If
        # table_name is unqualified with the schema name, return [nil, table_name].
        def parse_schema_and_object_name table_name
          return [nil, table_name] if table_name.count('.') == 0

          if table_name.count('.') > 1
            raise "Invalid relation reference #{table_name} (only one '.' is allowed)"
          end

          schema_name, table_name = table_name.split('.')
          [schema_name, table_name]
        end

        # Changes the database search path for the current connection for the duration of the given
        # block. After block execution, changes back to the original search path.
        # 
        # @param schemas [Array] a list of schemas by search path position to set as the search path
        # @yield executes the block under the search path defined by the given schema list
        # @return [Object] the return value of the given block
        def with_search_path schemas
          original_search_path = current_search_path
          execute "set search_path to #{Array(schemas).join(',')}"
          r = yield
          execute "set search_path to #{original_search_path}"
          r
        end

        # @return [String] the name of the first schema in the search path containing table_name
        def first_schema_for table_name
          return if !table_exists?(table_name)
          schema_name, unqualified_table_name = parse_schema_and_object_name(table_name)

          search_path_when_stmts = []
          current_search_path_schemas.each_with_index do |s,i| 
            search_path_when_stmts << "when table_schema = '#{s}' then #{(i+1).to_s}"
          end

          schema_name = execute <<-EOSQL
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
        
        # @return [String] the full current search path
        def current_search_path
          execute("show search_path").first.first
        end

        # @return [Array] the ordered schema names in the search path as strings
        def current_search_path_schemas
          search_path_schemas = current_search_path.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/).
            map(&:strip)
          username = current_user

          # the default search path begins with a symbolic reference to the current username
          # if that reference is in the search path, replace it with the resolved current username
          if search_path_schemas.first == '"$user"'
            user_schema_exists = execute <<-EOSQL
              select TRUE
              from information_schema.schemata 
              where schema_name = '#{username}'
            EOSQL

            if user_schema_exists.nil? || user_schema_exists.first.nil?
              search_path_schemas = search_path_schemas[1..-1]
            else
              search_path_schemas = [username] + search_path_schemas[1..-1]
            end
          end

          search_path_schemas.map(&:downcase)
        end
      end

    end
  end
end
