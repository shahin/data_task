require 'pg'
require_relative 'support/transactions'
require_relative 'support/booleans'

module Rake
  module TableTask

    class Greenplum < PostgreSQL

      TRACKING_VIEW_NAME = "tracking"
      @@adapters[:greenplum] = self

      def self.set_up_tracking
        super

        # Greenplum tracks CREATE and TRUNCATE operations in its pg_stat_operations system view.
        # Join this view with the tracking table so that we can track CREATE and TRUNCATE from within
        # the database instead of from application code.
        Sql.exec <<-EOSQL
          create view #{TRACKING_VIEW_NAME} as 
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
              from #{TRACKING_TABLE_NAME} ttb

              ) a
            ) 
          -- take only the latest operation per table
          where rank = 1
        EOSQL
      end

      def self.tear_down_tracking
        Db.execute "drop view #{TRACKING_VIEW_NAME} cascade"
        drop_table TRACKING_TABLE_NAME
      end

      def self.truncate_table table_name
        return if table_name.casecmp(TRACKING_TABLE_NAME) == 0
        Db.execute "truncate table #{table_name}"
      end

      def self.drop_table table_name
        Db.execute "drop table if exists #{table_name} cascade"
        return if table_name.casecmp("#{TRACKING_TABLE_NAME}_base") == 0
        track_drop table_name
      end

      def self.create_table table_name, data_definition, column_definitions, track_table=true
        drop_table table_name
        Db.execute <<-EOSQL
          create table #{table_name} #{column_definitions}
          #{ "as #{data_definition}" if !data_definition.nil? }
        EOSQL
        if track_table
          create_tracking_rules(table_name)
        end
      end

      def self.operations_supported
        {
          :by_db => operations_supported_by_db,
          :by_app => []
        }
      end


      private

        def self.operations_supported_by_db
          [:update, :insert, :delete, :truncate, :create]
        end

    end

  end
end
