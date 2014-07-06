require 'rake/testtask'
require 'bundler/gem_tasks'
require 'data_task'

Rake::TestTask.new do |t|
  t.libs << "spec"
  t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
  t.verbose
end

desc "Run tests"
task :default => :test
