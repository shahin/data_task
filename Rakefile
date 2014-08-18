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
    t.test_files = FileList['test/**/*_test.rb']
    t.verbose
  end

end

datastore :postgres, 'postgres://postgres@localhost:5432/example' do |ds|

  data 'rawzero' do
    ds.create_table 'rawzero', nil, '(var1 integer)'
  end

  datastore :sqlite, 'sqlite://example' do |ds|

    data 'raw0' => 'postgres:rawzero' do
      ds.create_table 'raw0', nil, '(var1 integer)'
    end

  end

  data 'raw1' => 'sqlite:raw0' do
    ds.create_table 'raw1', nil, '(var1 integer)'
  end

  data 'sqlite:raw2' => 'postgres:raw1' do
    sqlite.create_table 'raw2', nil, '(var1 integer)'
  end

end


# both approaches (datastore vs adapter hash) have problems.
#
# adapter hash pro:
# - simple, readable definition of where a table is and what it's called
# adapter hash con:
# - can't have two tables with the same name in different sources
#
# datastore namespace pro:
# - can have two tables with the same name in different stores
# datastore namespace con:
# - scope resolution might be confusing, since we mix datastores and namespaces
#
# hybrid:
# - make adapter[] 
