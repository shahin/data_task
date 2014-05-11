require 'rake'
require_relative './table'
require_relative './db'

######################################################################
module Rake

  # #########################################################################
  # A TableList is analogous to a FileList, for Tables.
  class TableList < FileList

    class << self
      # Get a sorted list of tables matching the pattern.
      def glob(pattern, *args)
        TableTask::Db.glob(pattern).sort
      end
    end

    # Redefine * to return either a string or a new table list.
    def *(other)
      result = @items * other
      case result
      when Array
        TableList.new.import(result)
      else
        result
      end
    end

    # Return a new TableList with the results of running +sub+ against each
    # element of the original list.
    #
    # Example:
    #   TableList['a.c', 'b.c'].sub(/\.c$/, '.o')  => ['a.o', 'b.o']
    #
    def sub(pat, rep)
      inject(TableList.new) { |res, tn | res << tn.sub(pat,rep) }
    end

    # Return a new TableList with the results of running +gsub+ against each
    # element of the original list.
    #
    # Example:
    #   TableList['lib/test/file', 'x/y'].gsub(/\//, "\\")
    #      => ['lib\\test\\file', 'x\\y']
    #
    def gsub(pat, rep)
      inject(TableList.new) { |res, tn | res << tn.gsub(pat,rep) }
    end

    # FileList#egrep is defined in the parent class but there's no obvious 
    # analogue for a TableList.
    def egrep(pattern, *options)
      raise NotImplementedError
    end

    # Return a new table list that only contains table names from the current
    # table list that exist in the database.
    def existing
      select { |tn| Table.exist?(tn) }
    end

    # Modify the current table list so that it contains only table names that
    # exist on in the database.
    def existing!
      resolve
      @items = @items.select { |tn| Table.exist?(tn) }
      self
    end

    # TableList version of partition.  Needed because the nested arrays should
    # be TableLists in this version.
    def partition(&block)       # :nodoc:
      resolve
      result = @items.partition(&block)
      [
        TableList.new.import(result[0]),
        TableList.new.import(result[1]),
      ]
    end

    # Add matching glob patterns.
    def add_matching(pattern)
      TableList.glob(pattern).each do |tn|
        self << tn unless exclude?(tn)
      end
    end
    private :add_matching

    # Should the given table name be excluded?
    def exclude?(tn)
      return true if @exclude_patterns.any? do |pat|
        case pat
        when Regexp
          tn =~ pat
        when /[*?]/
          Table.tnmatch?(pat, tn)
        else
          tn == pat
        end
      end
      @exclude_procs.any? { |p| p.call(tn) }
    end

    # TODO: implement a reasonable version of this for tables, probably
    # database-adapter-specific
    DEFAULT_IGNORE_PATTERNS = []
    DEFAULT_IGNORE_PROCS = []

  end
end
