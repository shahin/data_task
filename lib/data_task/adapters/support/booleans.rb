module Rake
  module DataTask

    class Db
      
      module StandardBooleans
        def truthy_value; 'true'; end
        def falsey_value; 'false'; end
      end

      module NumericBooleans
        def truthy_value; 1; end
        def falsey_value; 0; end
      end
      
    end

  end
end
