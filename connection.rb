require 'pg'
require 'stringio'
require 'logger'

module Connection

  LOG = Logger.new(STDOUT)

  @@connection = nil
  @@config = nil

  def self.config
    @@config || @config = YAML.load_file('database.yml')
  end

  def self.connect
    @@connection = PG::Connection.new(
      config['host'], 
      config['port'], 
      nil, 
      nil, 
      config['database'], 
      config['user'], 
      config['password']
    )
    @@connection.set_notice_processor do |msg|
      LOG.info('psql') { msg.chomp }
    end
  end

  def self.execute sql
    connect if @@connection.nil?
    begin
      @@connection.exec sql
    rescue PGError => e
      LOG.info e.message.chomp
    end
  end

end
