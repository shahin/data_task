require 'rake'
require 'data_task/version'
require 'data_task/data'
require 'data_task/data_store'

module Rake
  # #########################################################################
  # A DataTask is a task that includes time based dependencies. If any of a
  # DataTask's prerequisites has a timestamp that is later than the data
  # represented by this task, then the data must be rebuilt (using the
  # supplied actions).
  module DataTask

    class DataTask < ::Rake::Task

      # Instantiate a new DataTask.
      #
      # @param [Data] data the Data object that keeps track of existence and modification
      # @param [Rake::Application] app required by the parent class's constructor
      def initialize(task_name, app)
        super

        scope = datastore_scope(task_name)
        raise "This task has no datastore scope." if scope.nil?

        adapter = DataStore[scope.to_sym]
        data_name = task_name.split(':').last
        @data = Data.new(data_name, adapter)
      end

      # @returns [String] the innermost valid DataStore scope name for this task
      def datastore_scope task_name
        self.class.scope_name(@scope, task_name).split(':').reverse.find do |s|
          !DataStore[s.to_sym].nil?
        end
      end

      # Is this table task needed? Yes if it doesn't exist, or if its time stamp
      # is out of date.
      def needed?
        !@data.exist? || out_of_date?(timestamp)
      end

      # Time stamp for data task.
      def timestamp
        if @data.exist?
          mtime = @data.mtime.to_time
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
        # Step up through scopes and return the first DataStore scope we find, or nil.
        def self.datastore_in_scope task_name
          datastore_names = task_name.split(':')[0..-2].select do |scope|
            DataStore.has_key?(scope.to_sym)
          end
          DataStore[datastore_names.last.to_sym]
        end
      end

    end

  end
end

def data(*args, &block)
  # The task name in *args here is a Data returned by the adapter. Rake will key this task by 
  # Data.to_s in @tasks [Array]. All task recording and lookup in Rake is already done via to_s 
  # already to accomdate tasks named by symbols.
  Rake::DataTask::DataTask.define_task(*args, &block)
end
