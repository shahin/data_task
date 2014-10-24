require 'logger'
require_relative './adapter'

module Rake
  module DataTask

    class Filesystem < Adapter
      # This is the base class for filesystem-like data storage. It contains utility methods
      # that probably don't vary across filesystems, and it shouldn't be instantiated.

      LOG = Logger.new(STDOUT)
      LOG.level = Logger::WARN

      # Filesystems track file modification time internally, so it is always tracking, setup 
      # always succeeds, and attempts to stop or reset tracking always fail.
      def tracking_operations?; true; end
      def set_up_tracking options = {}; true; end
      def tear_down_tracking options = {}; false; end
      def reset_tracking options = {}; false; end

    end

  end
end
