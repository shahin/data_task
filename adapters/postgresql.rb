require 'pg'

module Rake
  module TableTask

    class PostgreSQL < Db

      @@adapters[:postgresql] = self

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
          select 1 from information_schema.tables where table_name = '#{TRACKING_TABLE_NAME}'
        EOSQL
        !tracking_table_exists.values.empty?
      end

      def self.set_up_tracking
        Sql.exec <<-EOSQL
          create table #{TRACKING_TABLE_NAME} (
            relation_name text,
            relation_type text,
            operation text,
            time timestamp
          )
        EOSQL
      end

      def self.tear_down_tracking
        Sql.exec "drop table #{TRACKING_TABLE_NAME} cascade"
      end
      
      def self.reset_tracking
        truncate_table TRACKING_TABLE_NAME
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
          create_tracking_rules(table_name)
          track_creation table_name, 0
        end
      end



      private

        def self.operations_supported 
          {
            :by_db_rule => ['update','insert','delete'],
            :by_app => ['truncate', 'create', 'drop']
          }
        end

        def self.rule_name table_name, operation
          "#{table_name}_#{operation}"
        end

        def self.create_tracking_rules table_name
          operations_supported[:by_db_rule].each do |operation|
            Sql.exec <<-EOSQL
              create or replace rule #{self.rule_name(table_name,operation)} as 
                on #{operation} to #{table_name} do also (
                delete from #{TRACKING_TABLE_NAME} where 
                  relation_name = '#{table_name}' and 
                  relation_type = 'TABLE' and
                  operation = '#{operation}'
                  ;
                insert into #{TRACKING_TABLE_NAME} values (
                  '#{table_name}', 'TABLE', '#{operation}', now()
                  );
                )
            EOSQL
          end
        end

        def self.track_creation table_name, n_tuples
          operation = 'create'
          Sql.exec <<-EOSQL
            delete from #{TRACKING_TABLE_NAME} where 
              relation_name = '#{table_name}' and 
              relation_type = 'TABLE' and
              operation = '#{operation}'
              ;
            insert into #{Db::TRACKING_TABLE_NAME} values (
              '#{table_name}', 'TABLE', '#{operation}', now()
            );
          EOSQL
        end

        def self.clear_tracking_rules_for_table table_name
          supported_operations.each do |operation|
            Sql.exec <<-EOSQL
              drop rule #{self.rule_name(table_name,operation)} on #{table_name}
            EOSQL
          end
        end

    end

  end
end
