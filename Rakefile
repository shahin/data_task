require 'rake/testtask'
require 'bundler/gem_tasks'
require 'table_task/db'
require 'table_task'

Rake::TestTask.new do |t|
  t.libs << "spec"
  t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
  t.verbose
end
task :default => :test

require 'table_task/adapters/postgresql'
require 'table_task/adapters/sqlite'

revenge = Rake::TableTask::PostgreSQL.new('localhost', 5432, 'ci_test', 'postgres')
cdw = Rake::TableTask::Sqlite.new('temp')

file 'precipitation.csv' do
  puts "precipitation.csv"
end

table revenge['precipitation'] => 'precipitation.csv' do
  revenge.create_table "precipitation", nil, "(var1 text)"
  puts "revenge data task"
end

table revenge['precipitations'] => revenge['precipitation'] do
  revenge.create_table "precipitations", nil, "(var1 text)"
  puts "revenge on revenge"
end

table cdw['precipitationss'], [:myarg] => revenge['precipitations'] do |t,args|
  puts args[:myarg]
  cdw.create_table "precipitationss", nil, "(var1 text)"
  puts "sqlite on postgres"
end
