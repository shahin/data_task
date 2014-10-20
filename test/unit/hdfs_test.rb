require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)

require_relative '../helper.rb'
require 'data_task/adapters/hdfs'


module Rake
  module DataTask

    class HdfsTest < Minitest::Test
      
      def before_setup
        if !@adapter.kind_of?(Rake::DataTask::Hdfs)
          skip("Using adapter #{@adapter}, so skipping #{self.class} tests.")
        end
      end

      def test_finds_data_when_it_exists
        if !@adapter.data_exists?(@adapter.test_data_name)
          @adapter.create_test_data @adapter.test_data_name
        end
        assert @adapter.data_exists?(@adapter.test_data_name)
      end

      def test_does_not_find_data_when_it_does_not_exist
        if @adapter.data_exists?(@adapter.test_data_name)
          @adapter.drop_data @adapter.test_data_name
        end
        assert !@adapter.data_exists?(@adapter.test_data_name)
      end

      def test_creates_data_when_called_to
        @adapter.with_tracking do

          @adapter.drop_data @adapter.test_data_name
          @adapter.create_test_data @adapter.test_data_name
          assert @adapter.data_exists?(@adapter.test_data_name)

        end
      end

      def test_deletes_data_when_called_to
        @adapter.with_tracking do

          @adapter.create_test_data @adapter.test_data_name
          @adapter.drop_data @adapter.test_data_name
          assert !@adapter.data_exists?(@adapter.test_data_name)

        end
      end

    end

  end
end
