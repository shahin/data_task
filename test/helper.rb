# https://coveralls.io
require 'coveralls'
Coveralls.wear!

require 'minitest/autorun'
require 'minitest/around/spec'
require 'minitest-spec-context'

require 'logger'

require 'table_task/sql'
require 'table_task/db'
require 'table_task/table'
require 'table_task'

require 'table_task/adapters/sqlite'
require 'table_task/adapters/postgresql'


module TrackingSetupTeardownHelper
  def with_tracking &ops
    set_up_tracking
    ops.call
    tear_down_tracking
  end
end
