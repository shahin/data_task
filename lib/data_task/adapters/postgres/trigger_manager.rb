module Rake
  module DataTask
    class Postgres < Db
      class TriggerManager

        def initialize db_instance
          @db = db_instance
        end

        def tracking_operations? schema_name=nil
          (
            event_trigger_exists?('datatask_track_drop_trigger') &&
            @db.function_exists?([schema_name, 'datatask_track_truncate'].compact.join('.')) &&
            @db.function_exists?([schema_name, 'datatask_track_drop'].compact.join('.'))
          )
        end

        def set_up_tracking
          create_tracking_functions
          create_drop_tracking_event_trigger
        end

        def tear_down_tracking

          # Table triggers relying on the truncate tracking function will drop by cascade
          drop_truncate_tracking_function

          # The event trigger relying on the drop tracking function will not drop by cascade
          drop_drop_tracking_function
          @db.class::LOG.info "Event trigger for DROP is not torn down. Drop it manually."

        end

        def create_truncate_table_trigger table_name
          schema_name, table_name = @db.parse_schema_and_object_name(table_name)
          schema_name = @db.first_schema_for(table_name) if schema_name.nil?

          @db.execute <<-EOSQL
            create trigger datatask_track_truncate_trigger after truncate
            on #{table_name}
            for each statement
            execute procedure #{schema_name}.datatask_track_truncate()
          EOSQL
        end

        private

          def create_tracking_functions
            schema_name = @db.current_search_path_schemas.first

            @db.execute <<-EOSQL
              create or replace function #{schema_name}.datatask_track_truncate() returns trigger as
              $$
                begin

                  delete from #{schema_name}.last_operations where 
                    relation_name = TG_TABLE_NAME and
                    relation_type = '#{@db.relation_type_values[:table]}'
                    ;
                  
                  insert into #{schema_name}.last_operations values (
                    TG_TABLE_NAME,
                    '#{@db.relation_type_values[:table]}',
                    'TRUNCATE',
                    clock_timestamp()
                    );

                return null; end;
              $$ language plpgsql
            EOSQL

            @db.execute <<-EOSQL
              create or replace function #{schema_name}.datatask_track_drop() returns event_trigger as
              $fn$
                declare
                  obj record;
                  dropped_from_schema varchar;
                  tracking_dropped_from_schema boolean;
                  is_table boolean;
                  q varchar;
                begin

                  for obj in select * from pg_event_trigger_dropped_objects()
                  loop

                    dropped_from_schema := obj.schema_name;

                    -- raise notice 'dropped_from_schema: %', dropped_from_schema;
                    if dropped_from_schema is not null then

                      execute 'select exists(
                        select 1
                        from information_schema.tables
                        where
                          table_schema = '
                          || quote_literal(dropped_from_schema) 
                          || ' and table_name = '
                          || quote_literal('last_operations') 
                          || ')' into tracking_dropped_from_schema;

                      -- raise notice 'tracking_dropped_from_schema: %', tracking_dropped_from_schema;
                      if tracking_dropped_from_schema then

                        is_table := (obj.object_type = 'table');
                        -- raise notice 'is_table: %', is_table;
                        if is_table then

                          q := 'delete from '
                            || quote_ident(dropped_from_schema)
                            || '.last_operations lo
                          where 
                            lo.relation_name = '
                            || quote_literal(obj.object_name) 
                            || ' and relation_type = '
                            || quote_literal($$#{@db.relation_type_values[:table]}$$)
                            ;
                        
                          -- raise notice 'query: %', q;
                          execute q;

                        end if;

                      end if;

                    end if;

                  end loop;

                end;
              $fn$ language plpgsql
            EOSQL
          end

          def drop_drop_tracking_function
            @db.execute "drop function if exists datatask_track_drop() cascade"
          end

          def drop_truncate_tracking_function
            @db.execute "drop function if exists datatask_track_truncate() cascade"
          end

          def create_drop_tracking_event_trigger
            drop_drop_tracking_event_trigger
            @db.execute <<-EOSQL
              create event trigger datatask_track_drop_trigger
              on sql_drop
              when TAG in ('DROP TABLE')
              execute procedure datatask_track_drop()
            EOSQL
          end

          def event_trigger_exists? event_trigger_name
            exists = Sql.get_single_value(
              @db.execute <<-EOSQL
                select exists( 
                  select TRUE from pg_catalog.pg_event_trigger
                  where evtname = '#{event_trigger_name}'
                  )
              EOSQL
            )
            @db.true?(exists)
          end

          def drop_drop_tracking_event_trigger
            @db.execute "drop event trigger if exists datatask_track_drop_trigger cascade"
          end


      end
    end
  end
end
