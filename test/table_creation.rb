module Rake
  module TableTask
    module TableCreation
      OLDTABLE = "old_table"
      NEWTABLE = "new_table"

      def create_timed_tables(adapter, old_table_name, *new_table_names)
        old_table = Table.new(old_table_name, adapter)
        return if (old_table.exists? &&
          new_table_names.all? do |new_table_name|
            new_table = Table.new(new_table_name, adapter)
            new_table.exists? && new_table.mtime > old_table.mtime
          end)
        now = Time.now

        create_table(adapter, old_table_name)
        sleep(1.0)

        new_table_names.each do |new_table_name|
          create_table(adapter, new_table_name)
        end
      end

      def create_table(adapter, name)
        adapter.create_table name, nil, '(var1 integer, var2 integer)' unless adapter.table_exists?(name)
        adapter.table_mtime(name)
      end

      def drop_table(adapter, name)
        adapter.drop_table(name) rescue nil
      end
    end
  end
end
