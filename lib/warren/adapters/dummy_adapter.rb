class DummyAdapter < Warren::Queue

  def self.publish queue_name, payload, options = {}, &blk
    logger.info "publishing #{payload.inspect} to #{queue_name}"
  end

  def self.subscribe queue_name, options = {}, &block
    logger.info "subscribing to #{queue_name}"
  end

  def self.stay_connected &blk
    logger.info "staying connected (begin)"
    blk.arity == 1 ? blk.call(self) : blk.call
    logger.info "staying connected (end)"
  end

  def self.client
    nil
  end

  def self.reset
  end
end
