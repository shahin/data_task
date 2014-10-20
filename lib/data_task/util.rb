class Array
  def to_quoted_s
    commad = self.join("','")
    "'#{commad}'"
  end
end

class String
  def constantize
    klass = self.split('::').inject(Object) {|memo, name| memo = memo.const_get(name); memo}
    klass
  end
end

# @returns [klass, Hash] the adapter klass specified by the URI scheme and the connection 
# options in the URI
#
# For example, the following connection_str will return the Postgres adapter class and options:
#
#   connection_options_from_str('postgres://postgres@localhost:5432/example')
# 
def connection_options_from_str connection_str
  uri = URI.parse(connection_str)
  adapter_name = uri.scheme.capitalize
  adapter_klass = "Rake::DataTask::#{adapter_name}".split('::').inject(Object) do |memo, name|
    memo = memo.const_get(name)
    memo
  end

  [adapter_klass, adapter_klass.connection_options_from_uri(uri)]
end
