require "rubygems"

class DummyAdapter < Warren::Queue

  def self.publish queue_name, payload, options = {}, &blk
    logger.info "publishing #{payload.inspect} to #{queue_name}"
  end

  def self.subscribe queue_name, options = {}, &block
    logger.info "subscribing to #{queue_name}"
  end
end
