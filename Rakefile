require 'rake/testtask'
require_relative './db.rb'
require_relative './table_task.rb'

Rake::TestTask.new do |t|
  t.libs << "spec"
  t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
  t.verbose
end

task :default => :test

task :reset_tracking, [:search_path] do |t, args|
  args.with_defaults(:search_path => nil)
  Rake::TableTask::Db.reset_tracking args
end

task :tear_down_tracking, [:search_path] do |t, args|
  args.with_defaults(:search_path => nil)
  Rake::TableTask::Db.tear_down_tracking args
end

task :set_up_tracking, [:search_path] do |t, args|
  args.with_defaults(:search_path => nil)
  Rake::TableTask::Db.set_up_tracking args
end
