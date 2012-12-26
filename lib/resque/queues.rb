require 'singleton'

module Resque

  # Track all the queues that Resque manages. This is an internal API,
  # application code shouldn't need to use it.
  class QueueCollection
    include Singleton

    # Create a new instance. Note that QueueCollection is a singleton, so you
    # shouldn't need to invoke this.
    def initialize
      @queues = Hash.new do |hsh, name|
        queue_name = name.to_s
        hsh[queue_name] = Resque::Queue.new(queue_name, Resque.redis,
                                            Resque.coder)
      end
    end

    # Public: Lookup a queue by name.
    def [](name)
      @queues[name]
    end

    # Public: Remove a queue by name.
    def delete(name)
      @queues.delete(name)
    end

    # Public: Count the queues.
    def size
      @queues.length
    end

    def clear
      @queues.clear
    end
  end

  Queues = QueueCollection.instance

end
