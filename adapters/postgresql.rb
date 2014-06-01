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
        rescue PGError => e
          LOG.info e.message.chomp
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

      def self.table_mtime table_name
        Sql.get_single_time <<-EOSQL
          select max(time) 
          from #{TABLE_TRACKER_NAME} 
          where relation_name = '#{table_name}'
        EOSQL
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
        Db.execute <<-EOSQL
          delete from #{TABLE_TRACKER_NAME} 
          where 
            relation_name = '#{table_name}' and 
            relation_type = '#{relation_type_values[:table]}'
        EOSQL
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

        def self.rule_name table_name, operation
          "#{table_name}_#{operation.to_s}"
        end

        def self.create_tracking_rules table_name
          operations_supported_by_db_rules.each do |operation|
            Db.execute <<-EOSQL
              create or replace rule #{self.rule_name(table_name, operation)} as 
                on #{operation.to_s} to #{table_name} do also (

                  delete from #{TABLE_TRACKER_NAME} where 
                    relation_name = '#{table_name}' and 
                    relation_type = '#{relation_type_values[:table]}'
                    ;

                  insert into #{TABLE_TRACKER_NAME} values (
                    '#{table_name}', 
                    '#{relation_type_values[:table]}', 
                    '#{operation_values[operation]}', 
                    clock_timestamp()
                  );

                )
            EOSQL
          end
        end

        def self.track_creation table_name, n_tuples
          operation = :create
          Db.execute <<-EOSQL
            delete from #{TABLE_TRACKER_NAME} where
              relation_name = '#{table_name}' and
              relation_type = '#{relation_type_values[:table]}'
              ;
            insert into #{TABLE_TRACKER_NAME} values (
              '#{table_name}',
              '#{relation_type_values[:table]}',
              '#{operation_values[operation]}',
              clock_timestamp()
            );
          EOSQL
        end

        def self.track_truncate table_name
          Db.execute <<-EOSQL
            update #{TABLE_TRACKER_NAME}
            set 
              operation = '#{operation_values[:truncate]}',
              time = clock_timestamp()
            where
              relation_name = '#{table_name}' and
              relation_type = '#{relation_type_values[:table]}'
          EOSQL
        end

        def self.clear_tracking_rules_for_table table_name
          operations_support_by_db_rules.each do |operation|
            Db.execute <<-EOSQL
              drop rule #{self.rule_name(table_name, operation)} on #{table_name}
            EOSQL
          end
        end

        def self.relation_exists? relation_name, relation_type, options = {}
          options = { :schema_names => nil }.merge(options)

          if !options[:schema_names].nil?
            schema_conditions_sql = "and table_schema in (#{options[:schema_names].to_quoted_s})"
          else
            schema_conditions_sql = 'true'
          end

          n_matches = Sql.get_single_int <<-EOSQL 
            select count(*)
            from information_schema.tables 
            where 
              table_name = '#{relation_name}' and
              table_type = '#{relation_type_values[relation_type]}' and
              #{ schema_conditions_sql }
          EOSQL
          (n_matches > 0)
        end

    end

  end
end
