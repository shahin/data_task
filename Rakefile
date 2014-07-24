require 'data_task'
require 'data_task/adapters/postgres'
require 'data_task/data_store'

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

postgres = Rake::DataTask::Postgres.new(
  'host' => 'localhost', 
  'port' => 5432, 
  'database' => 'example', 
  'username' => 'postgres'
  )

datastore :postgres, postgres do |data|

  data 'raw1' do |ds|
    ds.create_table 'raw1', nil, '(var1 integer)'
  end

  data 'raw2' => 'raw1' do
    p = Rake::DataTask::DataStore[:postgres]
    p.create_table 'raw2', nil, '(var1 integer)'
  end

end
