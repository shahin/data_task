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

def with_tracking &ops
  Rake::TableTask::Db.set_up_tracking
  ops.call
  Rake::TableTask::Db.tear_down_tracking
end
