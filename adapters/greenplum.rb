require 'pg'
require_relative 'support/transactions'
require_relative 'support/booleans'
require_relative './postgresql'

module Rake
  module TableTask

    class Greenplum < PostgreSQL

      TABLE_TRACKER_HELPER_NAME = "operations"
      @@adapters[:greenplum] = self

      def self.table_tracker_columns
        cols = super
        cols[:relation_type][:values][:table] = 'TABLE'
        cols
      end

      def self.set_up_tracking
        super

        Db.execute "alter table #{TABLE_TRACKER_NAME} rename to #{TABLE_TRACKER_HELPER_NAME}"

        # Greenplum tracks CREATE and TRUNCATE operations in its pg_stat_operations system view.
        # Join this view with the tracking table so that we can track CREATE and TRUNCATE from within
        # the database instead of from application code.
        Db.execute <<-EOSQL
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
                pg_stat_operations.objname as relation_name,
                case 
                  when actionname = 'TRUNCATE' then '#{relation_type_values[:table]}'
                  else pg_stat_operations.subtype
                end as relation_type,
                pg_stat_operations.actionname as operation,
                pg_stat_operations.statime as time
              from pg_stat_operations
              where pg_stat_operations.actionname not in ('ANALYZE', 'VACUUM')

              union all

              -- select all operations tracked by Greenplum (PostgreSQL) table rules 
              select
                ttb.relation_name,
                ttb.relation_type,
                ttb.operation,
                ttb.time
              from 
                #{TABLE_TRACKER_HELPER_NAME} ttb
                -- return only operations for tables that exist in system tables
                join pg_stat_operations pso on (
                  ttb.relation_name = pso.objname and
                  ttb.relation_type = pso.subtype and
                  pso.actionname = 'CREATE'
                )

              ) a
            ) b 
          -- take only the latest operation per table
          where rank = 1
        EOSQL

        # make sure we do deletes and inserts on the helper table, not the view
        Db.execute <<-EOSQL
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

      def self.tear_down_tracking
        drop_view TABLE_TRACKER_NAME
        drop_table TABLE_TRACKER_HELPER_NAME
      end

      def self.drop_table table_name
        Db.execute "drop table if exists #{table_name} cascade"
        return if table_name.casecmp(TABLE_TRACKER_HELPER_NAME) == 0 || 
          table_name.casecmp(TABLE_TRACKER_NAME) == 0
        track_drop table_name
      end

      def self.track_drop table_name
        Db.execute <<-EOSQL
          delete from #{TABLE_TRACKER_HELPER_NAME} 
          where 
            relation_name = '#{table_name}' and 
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
      end

      private

        def self.track_creation table_name, n_tuples
          # nothing to do; Greenplum tracks this operation in system tables already
          return nil
        end

        def self.track_truncate table_name
          # nothing to do; Greenplum tracks this operation in system tables already
          return nil
        end

    end

  end
end
