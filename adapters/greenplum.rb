require 'pg'
require_relative 'support/transactions'
require_relative 'support/booleans'
require_relative './postgresql'

module Rake
  module TableTask

    class Greenplum < PostgreSQL

      TABLE_TRACKER_HELPER_NAME = "operations"
      @@adapters[:greenplum] = self

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
              rank() over (partition by relation_name, relation_type order by time)
            from (

              -- select all CREATE and TRUNCATE operations tracked by Greenplum
              select
                pg_stat_operations.objname as relation_name,
                pg_stat_operations.subtype as relation_type,
                pg_stat_operations.actionname as operation,
                pg_stat_operations.statime as time
              from pg_stat_operations

              union all

              -- select all operations tracked by Greenplum (PostgreSQL) table rules 
              select
                relation_name,
                relation_type,
                operation,
                time
              from #{TABLE_TRACKER_HELPER_NAME} ttb

              ) a
            ) b 
          -- take only the latest operation per table
          where rank = 1
        EOSQL
      end

      def self.tear_down_tracking
        Db.execute "drop view #{TABLE_TRACKER_NAME} cascade"
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
