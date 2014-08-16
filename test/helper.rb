# https://coveralls.io
require 'coveralls'
Coveralls.wear!

require 'minitest/autorun'
require 'minitest/around/spec'
require 'minitest/around/unit'
require 'minitest-spec-context'

require 'logger'

require 'data_task/sql'
require 'data_task/db'
require 'data_task/data'
require 'data_task'

require 'data_task/adapters/sqlite'
require 'data_task/adapters/postgres'

module TestHelper

  module TrackingSetupTeardownHelper
    def with_tracking &ops
      set_up_tracking
      ops.call
      tear_down_tracking
    end
  end

  def self.get_adapter_to_test_db
    # connect an adapter to the configured database for testing
    config = YAML.load_file('test/config/database.yml')[ENV['DATATASK_ENV'] || 'postgres_test']
    klass = "Rake::DataTask::#{config['adapter'].capitalize}".split('::').inject(Object) {|memo, name| memo = memo.const_get(name); memo}
    adapter = klass.new(config)

    # extend the adapter to enable clean tracking setup/teardown within each test
    adapter.extend(TrackingSetupTeardownHelper)

    adapter
  end
end

