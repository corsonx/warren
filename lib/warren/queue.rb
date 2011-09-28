module Warren
  class Queue
    @@connection = nil
    @@adapter    = nil
    @@logger     = nil

    #
    # Raised if no connection has been defined yet.
    #
    NoConnectionDetails = Class.new(StandardError)

    #
    # Raised if a block is expected by the method but none is given.
    #
    NoBlockGiven = Class.new(StandardError)

    # 
    # Raised if an adapter isn't set
    # 
    NoAdapterSet = Class.new(StandardError)

    # 
    # Raised if the adapter is missing a method
    # Check the message for details of the missing method.
    # 
    InvalidAdapter = Class.new(StandardError)

    # 
    # Sets the current connection
    # 
    def self.connection= conn
      @@connection = (conn.is_a?(Warren::Connection) ? conn : Warren::Connection.new(conn) )
    end

    #
    # Returns the current connection details
    #
    def self.connection
      @@connection ||= Warren::Connection.new
    end

    # 
    # Sets the adapter when this class is subclassed.
    # 
    def self.inherited klass
      @@adapter = klass
    end

    # 
    # Sets the adapter manually
    # 
    def self.adapter= klass
      @@adapter = klass
    end

    # 
    # Returns the current adapter or raises NoAdapterSet exception
    # 
    def self.adapter
      @@adapter || raise(NoAdapterSet)
    end

    # 
    # Publishes the message to the queue
    # 
    def self.publish *args, &blk
      raise(InvalidAdapter, "publish method missing") unless @@adapter.respond_to?(:publish)
      self.adapter.publish(*args, &blk)
    end

    # 
    # Sends the subscribe message to the adapter class
    # 
    def self.subscribe *args, &blk
      raise(InvalidAdapter.new("subscribe method missing")) unless @@adapter.respond_to?(:subscribe)
      self.adapter.subscribe(*args, &blk)
    end

    #
    # Sends the stay_connected message to the adapter class
    #
    def self.stay_connected *args, &blk
      raise(InvalidAdapter, "stay_connected method missing") unless @@adapter.respond_to?(:stay_connected)
      self.adapter.stay_connected(*args, &blk)
    end

    #
    # Sends the client message to the adapter class
    #
    def self.client *args, &blk
      raise(InvalidAdapter.new("client method missing")) unless @@adapter.respond_to?(:client)
      self.adapter.client(*args, &blk)
    end

    #
    # Sends the reset message to the adapter class
    #
    def self.reset *args, &blk
      raise(InvalidAdapter.new("reset method missing")) unless @@adapter.respond_to?(:reset)
      self.adapter.reset(*args, &blk)
    end

		def self.logger
			unless @@logger
				@@logger = Logger.new(nil)
			end
			@@logger
		end

		def self.logger=(l)
			@@logger = l
		end
  end
end