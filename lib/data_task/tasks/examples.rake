require 'data_task/adapters/postgres'
require 'data_task/adapters/sqlite'

desc "Build a data file."
file 'raw.txt' do
  File.open('raw.txt', 'w') { |file| file.write("v1") }
end

datastore :postgres => Postgres.new('postgres@localhost:5432/example') do

  desc "Load a data file into PostgreSQL for analysis."
  data 'raw' => 'raw.txt' do |ds, t, args|
    ds.create_table "raw", nil, "(var1 text)"
    ds.execute "copy raw from '#{File.expand_path('raw.txt',Dir.pwd)}'"
  end

  desc "Perform analysis in PostgreSQL."
  data 'analyzed' => 'raw' do
    # perform analysis ...
    ds.create_table "analyzed", "select * from raw", nil
  end

end

datastore :sqlite => Sqlite.new('./example') do

  desc "Archive analysis results in SQLite."
  data 'analyzed_archive' => 'postgres:analyzed' do |ds, t, args|
    ds.create_table "analyzed_archive", nil, "(var1 text)"
    r = DataStore[:postgres].execute "select var1 from analyzed"
    ds.execute <<-EOSQL
      insert into analyzed_archive values 
      ('#{ r.flatten.join("'),('") }')
    EOSQL
  end

end
