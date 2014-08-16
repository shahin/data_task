require File.expand_path(
  File.join(Gem::Specification.find_by_name('rake').gem_dir,'test/helper.rb'), __FILE__)

require_relative './helper.rb'

module Rake
  module DataTask

    class SqlTest < Rake::TestCase

      def around(&block)
        @adapter = TestHelper.get_adapter_to_test_db
        @adapter.with_transaction_rollback do
          yield
        end
      end

      def test_when_asked_to_parse_a_single_value_raises_an_error_if_the_results_array_contains_more_than_one_column
        r = @adapter.execute('select 1,2')
        assert_raises(TypeError) { Sql.parse_single_value(r) }
      end

      def test_when_asked_to_parse_a_single_value_raises_an_error_if_the_results_array_contains_more_than_one_row
        r = @adapter.execute('select 1 union all select 2')
        assert_raises(TypeError) { Sql.parse_single_value(r) }
      end

      def test_when_asked_to_parse_a_single_value_returns_nil_if_the_results_array_contains_no_rows
        r = @adapter.execute("select 1 where #{@adapter.falsey_value}")
        assert_nil Sql.parse_single_value(r)
      end

      def test_when_asked_to_parse_a_single_value_returns_nil_if_the_results_array_contains_a_null_value
        r = @adapter.execute('select NULL')
        assert_nil Sql.parse_single_value(r)
      end

      def test_when_asked_for_a_single_int_returns_a_single_int_if_the_query_result_is_a_single_value_convertible_to_an_int
        assert_kind_of(Integer, Sql.get_single_int(@adapter.execute('select 1')))
      end

      def test_when_asked_for_a_single_int_raises_an_error_if_the_query_results_in_a_single_non_int
        assert_raises(ArgumentError) { Sql.get_single_int(@adapter.execute("select 'a'")) }
      end

    end

  end
end
