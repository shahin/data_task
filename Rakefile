require 'data_task'

desc "Run tests"
task :data_task => :'data_task:test'

namespace :data_task do

  require 'bundler/gem_tasks'
  require 'rake/testtask'

  Rake::TestTask.new do |t|
    t.libs << "spec"
    t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
    t.verbose
  end

end

load 'data_task/tasks/examples.rake'
