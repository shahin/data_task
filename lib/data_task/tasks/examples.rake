require 'data_task/adapters/postgres'
require 'data_task/adapters/sqlite'

# set up adapters to two databases
postgres = Rake::DataTask::Postgres.new(
  'host' => 'localhost', 'port' => 5432, 'database' => 'example', 'username' => 'postgres')
sqlite = Rake::DataTask::Sqlite.new('database' => 'example')


desc "Build a data file."
file 'raw.txt' do
  File.open('raw.txt', 'w') { |file| file.write("v1") }
end

desc "Load a data file into PostgreSQL for analysis."
data postgres['raw'] => 'raw.txt' do
  postgres.create_table "raw", nil, "(var1 text)"
  postgres.execute "copy raw from '#{File.expand_path('raw.txt',Dir.pwd)}'"
end

desc "Perform analysis in PostgreSQL."
data postgres['analyzed'] => postgres['raw'] do
  # perform analysis ...
  postgres.create_table "analyzed", "select * from raw", nil
end

desc "Archive analysis results in SQLite."
data sqlite['analyzed_archive'] => postgres['analyzed'] do
  sqlite.create_table "analyzed_archive", nil, "(var1 text)"
  r = postgres.execute "select var1 from analyzed"
  sqlite.execute <<-EOSQL
    insert into analyzed_archive values 
    ('#{ r.flatten.join("'),('") }')
  EOSQL
end
