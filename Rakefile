require 'rake/testtask'
require 'bundler/gem_tasks'
require 'db'
require 'table_task'

Rake::TestTask.new do |t|
  t.libs << "spec"
  t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
  t.verbose
end

task :default => :test

desc "Clear tracking history."
task :reset_tracking do
  Rake::TableTask::Db.reset_tracking
end

desc "Drop tracking relations."
task :tear_down_tracking do
  Rake::TableTask::Db.tear_down_tracking
end

desc "Set up tracking relations."
task :set_up_tracking do
  Rake::TableTask::Db.set_up_tracking
end
