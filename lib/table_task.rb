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

      def initialize(task_name, app)
        super
        @table = task_name
      end

      # Is this table task needed? Yes if it doesn't exist, or if its time stamp
      # is out of date.
      def needed?
        !@table.exist? || out_of_date?(timestamp)
      end

      # Time stamp for table task.
      def timestamp
        if @table.exist?
          mtime = @table.mtime.to_time
          raise "Table #{name} exists but modified time is unavailable." if mtime.nil?
          mtime
        else
          Rake::EARLY
        end
      end

      private

      # Are there any prerequisites with a later time than the given time stamp?
      def out_of_date?(stamp)
        @prerequisites.any? do |n| 
          prereq_time = application[n, @scope].timestamp
          return false if prereq_time == Rake::EARLY

          prereq_time > stamp
        end
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
  # The task name in *args here is a Table returned by the adapter. Rake will key this task by 
  # Table.to_s in @tasks [Array]. All task recording and lookup in Rake is already done via to_s 
  # already to accomdate tasks named by symbols.
  Rake::TableTask::TableTask.define_task(*args, &block)
end
