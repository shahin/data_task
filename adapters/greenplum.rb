require 'pg'

module Rake
  module TableTask

    class Greenplum < Db

      TRACKING_TABLE_NAME = 'tracking'
      TABLES_TO_TRACK = 'tables_to_track'
      @@adapters[:greenplum] = self

      def self.connect
        @connection = PG::Connection.new(
          config['host'], 
          config['port'], 
          nil, 
          nil, 
          config['database'], 
          config['user'], 
          config['password']
        )
        @connection.set_notice_processor do |msg|
          LOG.info('psql') { msg.chomp }
        end
      end

      def self.execute sql
        connect if @connection.nil?
        begin
          @connection.exec sql
        rescue PGError => e
          LOG.info e.message.chomp
          e
        end
      end

      def self.tracking_tables?
        tracking_table_exists = Sql.exec <<-EOSQL
          select 1 from information_schema.views where table_name = '#{TRACKING_TABLE_NAME}'
        EOSQL
        !tracking_table_exists.values.empty?
      end

      def self.set_up_tracking
        Table.create TABLES_TO_TRACK, nil, "(table_name text)"
        Sql.exec <<-EOSQL
          create view #{TRACKING_TABLE_NAME} as (
            select
              objname as relation_name,
              subtype as relation_type,
              actionname as operation,
              statime as time
            from 
              pg_stat_operations pso
              join #{TABLES_TO_TRACK} ttt on (pso.objname = ttt.table_name)
          )
        EOSQL
      end

      def self.tear_down_tracking
        Sql.exec "drop view #{TRACKING_TABLE_NAME} cascade"
        drop_table TABLES_TO_TRACK
      end
      
      def self.reset_tracking
        truncate_table TABLES_TO_TRACK
      end

      def self.table_mtime table_name
        Sql.get_single_time <<-EOSQL
          select max(time)
          from #{TRACKING_TABLE_NAME} 
          where relation_name = '#{table_name}'
        EOSQL
      end

      def self.truncate_table table_name
        Sql.exec "truncate table #{table_name}"
      end

      def self.drop_table table_name
        Sql.exec "drop table if exists #{table_name} cascade"
      end

      def self.table_exists? table_name, schema_names
        n_matches = Sql.get_single_int <<-EOSQL
          select count(*)
          from information_schema.tables 
          where 
            table_name = '#{table_name}' and
            table_schema in (#{schema_names.to_quoted_s})
        EOSQL
        (n_matches > 0)
      end

      def self.create_table table_name, data_definition, column_definitions, track_table
        drop_table table_name
        Sql.exec <<-EOSQL
          create table #{table_name} #{column_definitions}
          #{ "as #{data_definition}" if !data_definition.nil? }
        EOSQL
        if track_table
          add_table_to_tracked_tables(table_name)
        end
      end



      private

        def self.add_table_to_tracked_tables table_name
          Sql.exec <<-EOSQL
            delete from #{TABLES_TO_TRACK} where table_name = '#{table_name}';
            insert into #{TABLES_TO_TRACK} values ('#{table_name}');
          EOSQL
        end

    end

  end
end
