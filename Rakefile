require 'data_task'

desc "Run tests"
task :default => :'data_task:test'

namespace :data_task do

  require 'bundler/gem_tasks'
  require 'rake/testtask'

  Rake::TestTask.new do |t|
    t.libs << "spec"
    t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
    t.verbose
  end

end
