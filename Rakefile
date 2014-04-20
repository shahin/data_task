require_relative './db.rb'
require_relative './table_task.rb'

table :precipitation do |t|
  Table.create 'precipitation', '(var1 text)'
end

table :cumulative_precipitation => :precipitation do |t|
  Table.create('cumulative_precipitation', '(var1 text)')
end
