require 'rake/testtask'
require 'bundler/gem_tasks'
require 'table_task/db'
require 'table_task'

require 'table_task/adapters/postgresql'
require 'table_task/adapters/sqlite'

Rake::TestTask.new do |t|
  t.libs << "spec"
  t.test_files = FileList['test/**/*_spec.rb', 'test/test_*.rb']
  t.verbose
end
task :default => :test


revenge = Rake::TableTask::PostgreSQL.new(
  'host' => 'localhost', 'port' => 5432, 'database' => 'ci_test', 'username' => 'postgres')
cdw = Rake::TableTask::Sqlite.new('database' => 'temp')

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

table cdw['precipitationss'] => revenge['precipitations'] do
  cdw.create_table "precipitationss", nil, "(var1 text)"
  puts "sqlite on postgres"
end
