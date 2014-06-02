# https://coveralls.io
require 'coveralls'
Coveralls.wear!

require 'minitest/autorun'
require 'minitest/around/spec'
require 'minitest-spec-context'

require 'logger'

require_relative '../sql.rb'
require_relative '../db.rb'
require_relative '../table.rb'
require_relative '../table_task.rb'

def with_tracking &ops
  Rake::TableTask::Db.set_up_tracking
  ops.call
  Rake::TableTask::Db.tear_down_tracking
end
