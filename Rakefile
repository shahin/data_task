require_relative './db.rb'
require_relative './table_task.rb'

task :reset_tracking do
  Rake::TableTask::Db.reset_tracking
end

task :tear_down_tracking do
  Rake::TableTask::Db.tear_down_tracking
end

task :set_up_tracking do
  Rake::TableTask::Db.set_up_tracking
end

file 'pcp.txt' do |t|
end

table :precipitation => 'pcp.txt' do |t|
  Rake::TableTask::Table.create 'precipitation', '(var1 text)'
end

table :cumulative_precipitation => :precipitation do |t|
  Rake::TableTask::Table.create('cumulative_precipitation', '(var1 text)')
end
