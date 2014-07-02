require 'table_task/version'
require 'table_task/table'

module Rake
  # #########################################################################
  # A TableTask is a task that includes time based dependencies. If any of a
  # TableTask's prerequisites has a timestamp that is later than the table
  # represented by this task, then the table must be rebuilt (using the
  # supplied actions).
  #
  module TableTask

    class TableTask < Task

      def define_task(table, *args, &block)
        @table = table
        super([table.name] + args, &block)
      end

      # Is this table task needed? Yes if it doesn't exist, or if its time stamp
      # is out of date.
      def needed?
        !@table.exist? || out_of_date?(timestamp)
      end

      # Time stamp for table task.
      def timestamp
        if @table.exist?(name)
          mtime = @table.mtime(name.to_s)
          raise "Table #{name} exists but modified time is unavailable." if mtime.nil?
          mtime
        else
          Rake::EARLY
        end
      end

      private

      # Are there any prerequisites with a later time than the given time stamp?
      def out_of_date?(stamp)
        @prerequisites.any? { |n| application[n, @scope].timestamp > stamp}
      end

      # ----------------------------------------------------------------
      # Task class methods.
      #
      class << self
        # Apply the scope to the task name according to the rules for this kind
        # of task. Table based tasks ignore the scope when creating the name.
        def scope_name(scope, task_name)
          task_name
        end
      end

    end

  end
end

def table(*args, &block)
  table = args.shift
  Rake::TableTask::TableTask.define_task(table, *args, &block)
end