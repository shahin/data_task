require_relative '../sql.rb'
require_relative '../db.rb'

RSpec.configure do |config|

  # Use color in STDOUT
  config.color_enabled = true

  config.order = "random"
  config.around :each do |test|
    Rake::TableTask::Db.with_transaction_rollback &test
  end

end
