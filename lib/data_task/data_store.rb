module Rake

  module TaskManager

    # Evaluate the block in a nested namespace named +name+.  Create
    # an anonymous namespace if +name+ is nil.
    def in_datastore(name, adapter)
      name ||= generate_name
      @scope.push(name)
      ds = ::Rake::DataTask::DataStore.new(self, @scope, adapter)
      yield(adapter)
      ds
    ensure
      @scope.pop
    end

  end

  module DataTask

    # The DataStore class will look up task names in the the scope
    # defined by a +datastore+ command.
    #
    class DataStore < ::Rake::NameSpace

      @adapters = {}

      # Create a datastore lookup object using the given task manager
      # and the list of scopes.
      def initialize(task_manager, scope_list, adapter)
        super(task_manager, scope_list)
        @adapter = adapter
        self.class[scope_list.last] = adapter
      end

      # Keep track of all opened adapters for convenience.
      def self.[]=(name, adapter)
        @adapters[name] = adapter
      end

      def self.[](name)
        @adapters[name]
      end

    end
  end
end

# Create a new datastore and use it for evaluating the given
# block. Returns a DataStore object that can be used to lookup
# tasks defined in the datastore.
#
# E.g.
#
#   ds = datastore "nested" do
#     data 'blob'
#   end
#   data_blob = ds['blob'] # find 'blob' in the given datastore.
#
def datastore(name, adapter, &block)
  name = name.to_s if name.kind_of?(Symbol)
  name = name.to_str if name.respond_to?(:to_str)
  Rake::DataTask::DataStore[name.to_sym] = adapter
  Rake.application.in_datastore(name, adapter, &block)
end
