module Rake
  module DataTask

    class Db

      module BooleanChecking
        def true?(val); val == truthy_value; end
        def false?(val); val == falsey_value; end
      end
      
      module StandardBooleans
        def truthy_value; 'true'; end
        def falsey_value; 'false'; end
        include BooleanChecking
      end

      module SingleLetterBooleans
        def truthy_value; 't'; end
        def falsey_value; 'f'; end
        include BooleanChecking
      end

      module NumericBooleans
        def truthy_value; 1; end
        def falsey_value; 0; end
        include BooleanChecking
      end
      
    end

  end
end
