module Rake
  module DataTask
    module DataCreation
      OLDDATA = "old_data"
      NEWDATA = "new_data"

      def create_timed_data(adapter, old_data_name, *new_data_names)
        old_data = Data.new(old_data_name, adapter)
        return if (old_data.exists? &&
          new_data_names.all? do |new_data_name|
            new_data = Data.new(new_data_name, adapter)
            new_data.exists? && new_data.mtime > old_data.mtime
          end)
        now = Time.now

        create_data(adapter, old_data_name)
        sleep(1.0)

        new_data_names.each do |new_data_name|
          create_data(adapter, new_data_name)
        end
      end

      def create_data(adapter, name)
        adapter.create_data name, nil, '(var1 integer, var2 integer)' unless adapter.data_exists?(name)
        adapter.data_mtime(name)
      end

      def drop_data(adapter, name)
        adapter.drop_data(name) rescue nil
      end
    end
  end
end
