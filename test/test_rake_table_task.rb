require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)
require 'minitest/around/unit'
require_relative './table_creation.rb'
require_relative './helper.rb'

module Rake
  module TableTask

    class TestRakeTableTask < Rake::TestCase
      include Rake
      include TableCreation

      def around(&block)
        # connect an adapter to the configured database for testing
        config = YAML.load_file('config/database.yml')[ENV['TABLETASK_ENV']]
        klass = "Rake::TableTask::#{config['adapter'].capitalize}".split('::').inject(Object) {|memo, name| memo = memo.const_get(name); memo}
        @adapter = klass.new(config)

        @adapter.extend(TrackingSetupTeardownHelper)

        @adapter.with_transaction_rollback do
          yield
        end
      end

      def setup
        super

        Task.clear
        @runs = Array.new
      end

      def test_table_need
        @adapter.with_tracking do
          name = "dummy"
          table @adapter[name]
          ttask = Task[name]

          Table.drop(ttask.name) rescue nil
          assert ttask.needed?, "table should be needed"

          Db.create_table name, nil, '(var1 integer)'

          assert_equal nil, ttask.prerequisites.collect{|n| Task[n].timestamp}.max
          assert ! ttask.needed?, "table should not be needed"
        end
      end

      def test_table_times_new_depends_on_old
        puts @adapter.method(:with_tracking)
        @adapter.with_tracking do
          create_timed_tables(@adapter, OLDTABLE, NEWTABLE)

          t1 = Rake.application.intern(TableTask, @adapter[NEWTABLE]).enhance([@adapter[OLDTABLE]])
          t2 = Rake.application.intern(TableTask, @adapter[OLDTABLE])
          assert ! t2.needed?, "Should not need to build old table"
          assert ! t1.needed?, "Should not need to rebuild new table because of old"
        end
      end

      def test_table_times_new_depend_on_regular_task_timestamps
        @adapter.with_tracking do
          load_phony

          name = "dummy"
          task name

          create_timed_tables(@adapter, NEWTABLE)

          t1 = Rake.application.intern(TableTask, NEWTABLE).enhance([name])

          assert t1.needed?, "depending on non-table task uses Time.now"

          task(name => :phony)

          assert t1.needed?, "unless the non-table task has a timestamp"
        end
      end

      def test_table_times_old_depends_on_new
        @adapter.with_tracking do
          create_timed_tables(@adapter, OLDTABLE, NEWTABLE)

          t1 = Rake.application.intern(TableTask, OLDTABLE).enhance([NEWTABLE])
          t2 = Rake.application.intern(TableTask, NEWTABLE)
          assert ! t2.needed?, "Should not need to build new table"
          preq_stamp = t1.prerequisites.collect{|t| Task[t].timestamp}.max
          assert_equal t2.timestamp, preq_stamp
          assert t1.timestamp < preq_stamp, "T1 should be older"
          assert t1.needed?, "Should need to rebuild old table because of new"
        end
      end

      def test_table_depends_on_task_depend_on_table
        @adapter.with_tracking do
          create_timed_tables(@adapter, OLDTABLE, NEWTABLE)

          table @adapter[NEWTABLE] => [:obj] do |t| @runs << t.name end
          task :obj => [OLDTABLE] do |t| @runs << t.name end
          table @adapter[OLDTABLE] do |t| @runs << t.name end

          Task[:obj].invoke
          Task[NEWTABLE].invoke
          assert @runs.include?(NEWTABLE)
        end
      end

      def test_existing_table_depends_on_non_existing_table
        @adapter.with_tracking do
          @ran = false

          create_table(@adapter, OLDTABLE)
          drop_table(@adapter, NEWTABLE)
          table @adapter[NEWTABLE] do
            @ran = true
          end

          table @adapter[OLDTABLE] => NEWTABLE

          Task[OLDTABLE].invoke

          assert @ran
        end
      end

      def test_table_depends_on_new_file
        @adapter.with_tracking do
          create_timed_tables(@adapter, OLDTABLE, NEWTABLE)
          sleep(1)

          file NEWFILE do
            create_file(NEWFILE)
          end
          Task[NEWFILE].invoke

          @ran = false
          table NEWTABLE => NEWFILE do
            @ran = true
          end

          Task[NEWTABLE].invoke
          assert @ran, "Should have run the table task with an updated file dependency."
        end
      end

      def test_table_depends_on_new_file
        @adapter.with_tracking do
          file NEWFILE do
            create_file(NEWFILE)
          end
          Task[NEWFILE].invoke

          sleep(1)
          create_timed_tables(@adapter, OLDTABLE, NEWTABLE)

          @ran = false
          table NEWTABLE => NEWFILE do
            @ran = true
          end

          Task[@adapter[NEWTABLE]].invoke
          assert !@ran, "Should not have run the table task with an old file dependency."
        end
      end

      def test_file_depends_on_new_table
        @adapter.with_tracking do
          create_file(NEWFILE)
          sleep(1)

          table NEWTABLE do
            create_timed_tables(@adapter, OLDTABLE, NEWTABLE)
          end
          Task[@adapter[NEWTABLE]].invoke

          @ran = false
          file NEWFILE => NEWTABLE do
            @ran = true
          end

          Task[@adapter[NEWFILE]].invoke
          assert @ran, "Should have run the file task with an updated table dependency."
        end
      end

      def test_file_depends_on_old_table
        @adapter.with_tracking do
          table NEWTABLE do
            create_timed_tables(@adapter, OLDTABLE, NEWTABLE)
          end
          Task[@adapter[NEWTABLE]].invoke

          sleep(1)
          create_file(NEWFILE)

          @ran = false
          file NEWFILE => NEWTABLE do
            @ran = true
          end

          Task[@adapter[NEWFILE]].invoke
          assert !@ran, "Should not have run the file task with an old table dependency."
        end
      end

      def load_phony
        load File.join(@rake_lib, "rake/phony.rb")
      end

    end

  end
end
