require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)
require 'minitest/around/unit'
require_relative './table_creation.rb'
require_relative './helper.rb'

module Rake
  module DataTask

    class TestRakeDataTask < Rake::TestCase
      include Rake
      include DataCreation

      def around(&block)
        @adapter = get_adapter
        @adapter.with_transaction_rollback do
          yield
        end
        DataSource[:test] = @adapter
      end

      def setup
        super

        Task.clear
        @runs = Array.new
      end

      def test_data_need
        @adapter.with_tracking do
          name = "dummy"
          data @adapter[name]
          ttask = Task[name]

          Data.drop(ttask.name) rescue nil
          assert ttask.needed?, "data should be needed"

          @adapter.create_data name, nil, '(var1 integer)'

          assert_equal nil, ttask.prerequisites.collect{|n| Task[n].timestamp}.max
          assert ! ttask.needed?, "data should not be needed"
        end
      end

      def test_data_times_new_depends_on_old
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)

          t1 = Rake.application.intern(DataTask, @adapter[NEWDATA])
          #t1 = Rake.application.intern(DataTask, @adapter[NEWDATA]).enhance([@adapter[OLDDATA]])
          #t2 = Rake.application.intern(DataTask, @adapter[OLDDATA])
          #assert ! t2.needed?, "Should not need to build old data"
          #assert ! t1.needed?, "Should not need to rebuild new data because of old"
        end
      end

      def test_data_times_new_depend_on_regular_task_timestamps
        @adapter.with_tracking do
          load_phony

          name = "dummy"
          task name

          create_timed_data(@adapter, NEWDATA)

          t1 = Rake.application.intern(DataTask, @adapter[NEWDATA]).enhance([name])

          assert t1.needed?, "depending on non-data task uses Time.now"

          task(name => :phony)

          assert t1.needed?, "unless the non-data task has a timestamp"
        end
      end

      def test_data_times_old_depends_on_new
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)

          t1 = Rake.application.intern(DataTask, @adapter[OLDDATA]).enhance([@adapter[NEWDATA]])
          t2 = Rake.application.intern(DataTask, @adapter[NEWDATA])
          assert ! t2.needed?, "Should not need to build new data"
          preq_stamp = t1.prerequisites.collect{|t| Task[t].timestamp}.max
          assert_equal t2.timestamp, preq_stamp
          assert t1.timestamp < preq_stamp, "T1 should be older"
          assert t1.needed?, "Should need to rebuild old data because of new"
        end
      end

      def test_data_depends_on_task_depend_on_data
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)

          data @adapter[NEWDATA] => [:obj] do |t| @runs << t.name end
          task :obj => [OLDDATA] do |t| @runs << t.name end
          data @adapter[OLDDATA] do |t| @runs << t.name end

          Task[:obj].invoke
          Task[NEWDATA].invoke
          assert @runs.include?(NEWDATA)
        end
      end

      def test_existing_data_depends_on_non_existing_data
        @adapter.with_tracking do
          @ran = false

          create_data(@adapter, OLDDATA)
          drop_data(@adapter, NEWDATA)
          data @adapter[NEWDATA] do
            @ran = true
          end

          data @adapter[OLDDATA] => NEWDATA

          Task[OLDDATA].invoke

          assert @ran
        end
      end

      def test_data_depends_on_new_file
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)
          sleep(1)

          file NEWFILE do
            create_file(NEWFILE)
          end
          Task[NEWFILE].invoke

          @ran = false
          data NEWDATA => NEWFILE do
            @ran = true
          end

          Task[NEWDATA].invoke
          assert @ran, "Should have run the data task with an updated file dependency."
        end
      end

      def test_data_depends_on_new_file
        @adapter.with_tracking do
          file NEWFILE do
            create_file(NEWFILE)
          end
          Task[NEWFILE].invoke

          sleep(1)
          create_timed_data(@adapter, OLDDATA, NEWDATA)

          @ran = false
          data @adapter[NEWDATA] => NEWFILE do
            @ran = true
          end

          Task[@adapter[NEWDATA]].invoke
          assert !@ran, "Should not have run the data task with an old file dependency."
        end
      end

      def test_file_depends_on_new_data
        @adapter.with_tracking do
          create_file(NEWFILE)
          sleep(1)

          data @adapter[NEWDATA] do
            create_timed_data(@adapter, OLDDATA, NEWDATA)
          end
          Task[@adapter[NEWDATA]].invoke

          @ran = false
          file NEWFILE => @adapter[NEWDATA] do
            @ran = true
          end

          Task[@adapter[NEWFILE]].invoke
          assert @ran, "Should have run the file task with an updated data dependency."
        end
      end

      def test_file_depends_on_old_data
        @adapter.with_tracking do
          data @adapter[NEWDATA] do
            create_timed_data(@adapter, OLDDATA, NEWDATA)
          end
          Task[@adapter[NEWDATA]].invoke

          sleep(1)
          create_file(NEWFILE)

          @ran = false
          file NEWFILE => @adapter[NEWDATA] do
            @ran = true
          end

          Task[@adapter[NEWFILE]].invoke
          assert !@ran, "Should not have run the file task with an old data dependency."
        end
      end

      def load_phony
        load File.join(@rake_lib, "rake/phony.rb")
      end

    end

  end
end
