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
        @adapter = TestHelper.get_adapter_to_test_db
        @adapter_scope = 'test'
        DataStore[@adapter_scope.to_sym] = @adapter
        @adapter.with_transaction_rollback do
          yield
        end
      end

      def setup
        super

        Task.clear
        @runs = Array.new
      end

      def test_data_need
        @adapter.with_tracking do
          name = "dummy"
          scoped_name = [@adapter_scope, name].join(':')
          data scoped_name
          ttask = Task[scoped_name]

          @adapter.drop(name) rescue nil
          assert ttask.needed?, "data should be needed"

          @adapter.create_data name, nil, '(var1 integer)'

          assert_equal nil, ttask.prerequisites.collect{|n| Task[n].timestamp}.max
          assert ! ttask.needed?, "data should not be needed"
        end
      end

      def test_data_times_new_depends_on_old
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)
          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          t1 = Rake.application.intern(DataTask, scoped_newdata).enhance([scoped_olddata])
          t2 = Rake.application.intern(DataTask, scoped_olddata)
          assert ! t2.needed?, "Should not need to build old data"
          assert ! t1.needed?, "Should not need to rebuild new data because of old"
        end
      end

      def test_data_times_new_depend_on_regular_task_timestamps
        @adapter.with_tracking do
          load_phony

          name = "dummy"
          task name

          create_timed_data(@adapter, NEWDATA)
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          t1 = Rake.application.intern(DataTask, scoped_newdata).enhance([name])

          assert t1.needed?, "depending on non-data task uses Time.now"

          task(name => :phony)

          assert t1.needed?, "unless the non-data task has a timestamp"
        end
      end

      def test_data_times_old_depends_on_new
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)
          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          t1 = Rake.application.intern(DataTask, scoped_olddata).enhance([scoped_newdata])
          t2 = Rake.application.intern(DataTask, scoped_newdata)
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
          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          data scoped_newdata => [:obj] do |t| @runs << t.name end
          task :obj => [scoped_olddata] do |t| @runs << t.name end
          data scoped_olddata do |t| @runs << t.name end

          Task[:obj].invoke
          Task[scoped_newdata].invoke
          assert @runs.include?(scoped_newdata)
        end
      end

      def test_existing_data_depends_on_non_existing_data
        @adapter.with_tracking do
          @ran = false

          create_data(@adapter, OLDDATA)
          drop_data(@adapter, NEWDATA)
          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          data scoped_newdata do
            @ran = true
          end

          data scoped_olddata => scoped_newdata

          Task[scoped_olddata].invoke

          assert @ran
        end
      end

      def test_data_depends_on_new_file
        @adapter.with_tracking do
          create_timed_data(@adapter, OLDDATA, NEWDATA)
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"
          sleep(1)

          file NEWFILE do
            create_file(NEWFILE)
          end
          Task[NEWFILE].invoke

          @ran = false
          data scoped_newdata => NEWFILE do
            @ran = true
          end

          Task[scoped_newdata].invoke
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
          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          @ran = false
          data scoped_newdata => NEWFILE do
            @ran = true
          end

          Task[scoped_newdata].invoke
          assert !@ran, "Should not have run the data task with an old file dependency."
        end
      end

      def test_file_depends_on_new_data
        @adapter.with_tracking do
          create_file(NEWFILE)
          sleep(1)

          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          data scoped_newdata do
            create_timed_data(@adapter, OLDDATA, NEWDATA)
          end
          Task[scoped_newdata].invoke

          @ran = false
          file NEWFILE => scoped_newdata do
            @ran = true
          end

          Task[NEWFILE].invoke
          assert @ran, "Should have run the file task with an updated data dependency."
        end
      end

      def test_file_depends_on_old_data
        @adapter.with_tracking do
          scoped_olddata = "#{@adapter_scope}:#{OLDDATA}"
          scoped_newdata = "#{@adapter_scope}:#{NEWDATA}"

          data scoped_newdata do
            create_timed_data(@adapter, OLDDATA, NEWDATA)
          end
          Task[scoped_newdata].invoke

          sleep(1)
          create_file(NEWFILE)

          @ran = false
          file NEWFILE => scoped_newdata do
            @ran = true
          end

          Task[NEWFILE].invoke
          assert !@ran, "Should not have run the file task with an old data dependency."
        end
      end

      def load_phony
        load File.join(@rake_lib, "rake/phony.rb")
      end

    end

  end
end
