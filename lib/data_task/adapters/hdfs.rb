require 'webhdfs'

module Rake
  module DataTask

    class Hdfs < Filesystem

      def self.connection_options_from_uri uri
        { 
          :host => uri.host,
          :port => uri.port
        }
      end

      # Connect to an HDFS server.
      #
      # @param [Hash] options the connection parameters
      # @option options [String] 'host' the namenode's hostname (default: localhost)
      # @option options [String] 'port' the namenode's port (default: 50070)
      # @return [Hdfs] an instance of this adapter
      def initialize options
        @base_path = options[:base_path] || ''
        @connection = WebHDFS::Client.new(
          options['host'] || 'localhost',
          options['port'] || 50070,
          'shahin'
        )
      end

      # HDFS always tracks table modification time internally (see HADOOP-1377), so it is always
      # tracking, setup always succeeds, and attempts to stop or reset tracking always fail.
      def tracking_operations?; true; end
      def set_up_tracking options = {}; true; end
      def tear_down_tracking options = {}; false; end
      def reset_tracking options = {}; false; end

      # @returns [DateTime] the modification time of the file to second precision
      def file_mtime file_path
        file_path = File.join(@base_path, file_path)
        attrs = @connection.stat(file_path)
        DateTime.parse(Time.at(attrs['modificationTime'] / 1000.0).to_s)
      end

      alias_method :data_mtime, :file_mtime

      def create_file file_path, data=''
        file_path = File.join(@base_path, file_path)
        @connection.create file_path, data
      end

      alias_method :create_data, :create_file

      def delete_file file_path
        file_path = File.join(@base_path, file_path)
        @connection.delete(file_path, :recursive => true)
      end

      alias_method :drop_data, :delete_file

      def file_exists? file_path, options = {}
        file_path = File.join(@base_path, file_path)
        begin
          @connection.stat(file_path)
          true
        rescue WebHDFS::FileNotFoundError
          false
        end
      end

      alias_method :data_exists?, :file_exists?

      def operations_supported
        {
          :by_db => operations_supported_by_db
        }
      end

      def method_missing(method_name, *args, &block)
        @connection.send(method_name, *args)
      end



      private

        def operations_supported_by_db
          [:create, :delete, :append]
        end

    end

  end
end
