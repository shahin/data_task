require_relative '../sql.rb'
require_relative '../db.rb'

RSpec.configure do |config|

  # Use color in STDOUT
  config.color_enabled = true

  config.order = "random"
  config.around :each do |test|
    Rake::TableTask::Db.with_transaction_rollback do
      test.call
    end
  end

end

def with_tracking &ops
  Rake::TableTask::Db.set_up_tracking
  ops.call
  Rake::TableTask::Db.tear_down_tracking
end
