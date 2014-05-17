require 'pg'

module Rake
  module TableTask

    class PostgreSQL < Db

      @@adapters[:postgresql] = self

      def self.connect
        @connection = PG::Connection.new(
          config['host'] || localhost,
          config['port'] || 5432,
          nil,
          nil,
          config['database'],
          config['user'],
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
        rescue PGError => e
          e
        end
      end

      def self.with_transaction do_commit, &block
        Db.execute "begin;"
        yield
        close_command = do_commit ? "commit;" : "rollback;"
        Db.execute close_command
      end

      def self.with_transaction_commit &block
        with_transaction true, &block
      end

      def self.with_transaction_rollback &block
        with_transaction false, &block
      end

      def self.tracking_tables?
        table_exists?(TRACKING_TABLE_NAME)
      end

      def self.set_up_tracking
        create_table TRACKING_TABLE_NAME, nil, "
          (
            relation_name text,
            relation_type text,
            operation text,
            time timestamp
          )
        ", false
      end

      def self.tear_down_tracking
        drop_table TRACKING_TABLE_NAME
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
        Db.execute "truncate table #{table_name}"
      end

      def self.drop_table table_name
        Db.execute "drop table if exists #{table_name} cascade"
      end

      def self.table_exists? table_name, schema_names=nil
        n_matches = Sql.get_single_int <<-EOSQL
          select count(*)
          from information_schema.tables 
          where 
            table_name = '#{table_name}'
            #{ "and table_schema in (#{schema_names.to_quoted_s})" if !schema_names.nil?}
        EOSQL
        (n_matches > 0)
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
            Db.execute <<-EOSQL
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
          Db.execute <<-EOSQL
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
            Db.execute <<-EOSQL
              drop rule #{self.rule_name(table_name,operation)} on #{table_name}
            EOSQL
          end
        end

    end

  end
end
