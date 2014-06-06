module Rake
  module TableTask
    module TableCreation
      OLDTABLE = "old_table"
      NEWTABLE = "new_table"

      def create_timed_tables(old_table, *new_tables)
        return if (Table.exist?(old_table) &&
          new_tables.all? { |new_table|
            Table.exist?(new_table) && Table.mtime(new_table) > Table.mtime(old_table)
          })
        now = DateTime.now

        create_table(old_table)
        sleep(1.0)

        new_tables.each do |new_table|
          create_table(new_table)
        end
      end

      def create_table(name)
        Table.new name, nil, '(var1 integer, var2 integer)' unless Table.exist?(name)
        Table.mtime(name)
      end

      def drop_table(name)
        Table.drop(name) rescue nil
      end
    end
  end
end
