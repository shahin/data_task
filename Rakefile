require 'rake/testtask'
require_relative './db.rb'
require_relative './table_task.rb'

Rake::TestTask.new do |t|
  t.libs << "spec"
  t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
  t.verbose
end

task :default => :test

task :reset_tracking do
  Rake::TableTask::Db.reset_tracking
end

task :tear_down_tracking do
  Rake::TableTask::Db.tear_down_tracking
end

task :set_up_tracking do
  Rake::TableTask::Db.set_up_tracking
end
