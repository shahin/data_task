begin
  require 'pg'
rescue LoadError
  puts "The Greenplum adapter requires the pg gem, which can be installed via rubygems."
end
require_relative 'support/transactions'
require_relative 'support/booleans'
require_relative './postgres'

module Rake
  module DataTask

    # DBMS-consistent operations and their mechanisms:
    # - create (view on system catalog)
    # - drop (explicit delete operation using a join on system catalog)
    # x insert (table rule) (inherited)
    # x update (table rule) (inherited)
    # x delete (table rule) (inherited)
    # - truncate (view on system catalog)
    class Greenplum < Postgres

      TABLE_TRACKER_HELPER_NAME = "operations"

      def initialize *args
        super
        @trigger_manager = TriggerManager.new(self)
      end

      def set_up_tracking options = {}
        schema_name = options[:schema_name]
        force_setup = options[:force]

        return if tracking_operations?(schema_name) && !!force_setup

        target_search_path = [schema_name || current_search_path_schemas.first]
        with_search_path(target_search_path) do

          create_tracking_table TABLE_TRACKER_HELPER_NAME

          # Greenplum tracks CREATE and TRUNCATE operations in its pg_stat_operations system view.
          # Join this view with the tracking table so that we can track CREATE and TRUNCATE from 
          # within the database instead of from application code.

          execute <<-EOSQL
            create view fixed_pg_stat_operations as
            -- GP's pg_stat_operations enum values like 'TABLE' are inconsistent with our 
            -- configuration for the table tracking table, so translate them here
            select
              pso.classname, 
              pso.objname,
              pso.objid,
              pso.schemaname,
              pso.usestatus,
              pso.usename,
              pso.actionname,
              case 
                when pso.actionname = 'TRUNCATE' then '#{relation_type_values[:table]}'
                when pso.subtype = 'TABLE' then '#{relation_type_values[:table]}'
                else pso.subtype
              end as subtype,
              pso.statime
            from pg_stat_operations pso
          EOSQL

          execute <<-EOSQL
            create view #{TABLE_TRACKER_NAME} as 
            select
              relation_name,
              relation_type,
              operation,
              time
            from (

              select
                a.*,
                rank() over (partition by relation_name, relation_type order by time desc)
              from (

                -- select all CREATE and TRUNCATE operations tracked by Greenplum
                select
                  pso.objname as relation_name,
                  pso.subtype as relation_type,
                  pso.actionname as operation,
                  pso.statime as time
                from fixed_pg_stat_operations pso
                where pso.actionname not in ('ANALYZE', 'VACUUM')

                union all

                -- select all operations tracked by Greenplum (PostgreSQL) table rules 
                select
                  ttb.relation_name,
                  ttb.relation_type,
                  ttb.operation,
                  ttb.time
                from 
                  #{TABLE_TRACKER_HELPER_NAME} ttb
                  -- return only operations for objects that exist in system views so that we 
                  -- exclude dropped objects that don't exist anymore
                  join fixed_pg_stat_operations pso on (
                    ttb.relation_name = pso.objname and
                    ttb.relation_type = pso.subtype and
                    pso.actionname = 'CREATE'
                  )

                ) a
              ) b 
            -- take only the latest operation per table
            where rank = 1
          EOSQL

          # In this Greenplum adapter, the table tracker isn't a table itself: it's a view over the
          # real table tracking table (TABLE_TRACKER_HELPER_NAME) and a system view. The inherited
          # adapter interface expects a writeable table tracking table, though, so redirect any
          # writes from the table tracker view (TABLE_TRACKER_NAME) to the table that backs it.
          execute <<-EOSQL
            create rule delete_operation_record as on delete to #{TABLE_TRACKER_NAME} 
              do instead
              delete from #{TABLE_TRACKER_HELPER_NAME} 
              where
                relation_name = OLD.relation_name and
                relation_type = OLD.relation_type and
                operation = OLD.operation
            ;

            create rule insert_operation_record as on insert to #{TABLE_TRACKER_NAME} 
              do instead
              insert into #{TABLE_TRACKER_HELPER_NAME} values (
                NEW.relation_name,
                NEW.relation_type,
                NEW.operation,
                NEW.time
              )
            ;
          EOSQL
        end
      end

      def tear_down_tracking options
        drop_view "fixed_pg_stat_operations"
        drop_view TABLE_TRACKER_NAME
        drop_table TABLE_TRACKER_HELPER_NAME
      end

      # Check whether tracking is set up in this schema. If schema is not specified, check the
      # first schema in which we find a tracking table.
      #
      # @return [Boolean] true if all tracking assets are available in the schema; false otherwise
      def tracking_operations? schema_name=nil
        schema_name ||= first_schema_for(TABLE_TRACKER_NAME)
        return false if schema_name.nil?

        view_exists?(TABLE_TRACKER_NAME)
      end


      private

        def track_creation table_name, n_tuples
          # nothing to do; Greenplum tracks this operation in system tables already
          return nil
        end

        # Triggers are not supported by Greenplum, so override the inherited delegate class
        class TriggerManager
          # implement the public interface for TriggerManager since it's expected by some parent
          # class instance methods
          def set_up_tracking schema_name=nil; nil; end
          def tear_down_tracking schema_name=nil; nil; end
          def create_truncate_table_trigger table_name; nil; end
          def tracking_operations? schema_name=nil; true; end
        end

    end

  end
end
