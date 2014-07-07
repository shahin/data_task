[![Build Status](https://travis-ci.org/shahin/data_task.svg?branch=master)](https://travis-ci.org/shahin/data_task)
[![Coverage Status](https://img.shields.io/coveralls/shahin/data_task.svg)](https://coveralls.io/r/shahin/data_task?branch=master)

# DataTask

DataTask enables dependency-based programming for data workflows on top of the Rake build tool. This gem provides the DataTask, analogous to Rake's built-in FileTask but extended to work with pluggable backends beyond the local filesystem.

Adapters are included for Sqlite3, PostgreSQL, and Greenplum.

## Installation

Add this line to your application's Gemfile:

    gem 'data_task'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install data_task

## Usage

To write your first dependency-based data loader, connect to your database by instantiating an adapter:

```
postgres = Rake::DataTask::Postgres.new(
  'host' => 'localhost', 
  'port' => 5432, 
  'database' => 'example', 
  'username' => 'postgres'
  )
```

Then use this adapter instance as the target for a data task:

```
desc "Load a data file into PostgreSQL for analysis."
data postgres['raw'] => 'raw.txt' do
  # Add loading logic here
end
```

Rake will run this task if and only if (a) the table 'raw' is does not exist yet, or (b) the table 'raw' exists but has a timestamp earlier than the file 'raw.txt'. Since database tables now have timestamps associated with them, they can serve as targets or as dependencies in data tasks.

See lib/data_task/tasks/examples.rake for a more complete example workflow.

## Contributing

1. Fork it ( https://github.com/shahin/data_task/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
