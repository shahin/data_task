require 'data_task'
require 'data_task/adapters/postgres'
require 'data_task/adapters/sqlite'
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

sqlite = Rake::DataTask::Sqlite.new({ 'database' => 'tester' })

datastore :postgres, postgres do |ds|

  data 'rawzero' do
    ds.create_table 'rawzero', nil, '(var1 integer)'
  end

  datastore :sqlite, sqlite do |ds|

    data 'raw0' => '^rawzero' do
      ds.create_table 'raw0', nil, '(var1 integer)'
    end

  end

  data 'raw1' => 'sqlite:raw0' do
    ds.create_table 'raw1', nil, '(var1 integer)'
  end

  data 'sqlite:raw2' => 'raw1' do
    ds.create_table 'raw2', nil, '(var1 integer)'
  end

end
