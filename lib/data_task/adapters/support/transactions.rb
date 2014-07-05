require_relative '../../db'

module Rake
  module DataTask

    class Db
      module StandardTransactions

        def with_transaction do_commit, &block
          execute "begin;"
          yield
          close_command = do_commit ? "commit;" : "rollback;"
          execute close_command
        end

        def with_transaction_commit &block
          with_transaction true, &block
        end

        def with_transaction_rollback &block
          with_transaction false, &block
        end

      end
    end

  end
end
