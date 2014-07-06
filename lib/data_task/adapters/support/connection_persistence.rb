module ConnectionPersistence
  # Adapters for datastores with long-lived connections should re-use existing connections instead
  # of creating redundant connections. This module provides methods for persisting a connection
  # across instances of an adapter and retrieving a persisted connection from the adapter class.
  #
  # A class that extends this module should declare an empty @connections = {} class instance var.

  # Retrieve a connection by the hash of options that uniquely identify it.
  def persisted_connection conn_options
    @connections[conn_options]
  end

  # Save a connection and key it by a hash of options that uniquely identify it.
  def persist_connection conn, conn_options
    @connections[conn_options] = conn
  end

end
