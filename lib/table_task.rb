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

      attr_accessor :table

      # Is this table task needed? Yes if it doesn't exist, or if its time stamp
      # is out of date.
      def needed?
        !@table.exist? || out_of_date?(timestamp)
      end

      # Time stamp for table task.
      def timestamp
        if @table.exist?
          mtime = @table.mtime
          raise "Table #{name} exists but modified time is unavailable." if mtime.nil?
          mtime
        else
          Rake::EARLY
        end
      end

      private

      # Are there any prerequisites with a later time than the given time stamp?
      def out_of_date?(stamp)
        @prerequisites.any? { |n| application[n, @scope].timestamp > stamp }
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

        def define_task(table, *args, &block)
          table_task = super(*args, &block)
          table_task.table = table
          table_task
        end

      end

    end

  end
end

def table(*args, &block)
  # Rake's resolve_args modifies args in-place, so send it a copy to keep the original intact
  args_to_resolve = args.clone
  task_name, arg_names, deps = Rake.application.resolve_args(args_to_resolve)

  if args.first.is_a?(Hash)
    # have no task arguments, so the task name keys the prerequisites
    args[0] = { task_name.to_s => args[0][task_name] }
  else
    # have task arguments, so the task name is just a value and the arguments key the prereqs
    args[0] = task_name.to_s
  end
  Rake::TableTask::TableTask.define_task(task_name, *args, &block)
end
