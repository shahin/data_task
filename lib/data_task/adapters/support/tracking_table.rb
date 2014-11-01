module Rake
  module DataTask
    class Db
      module TrackingTable

        def create_tracking_table table_name
          column_definitions = table_tracker_columns.map do |col, col_defn|
            col.to_s + ' ' + col_defn[:data_type].to_s
          end.join(', ')
          create_table table_name, nil, " (#{column_definitions})", false
        end

      end
    end
  end
end

