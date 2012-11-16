module Resque

  # An adapter for talking to Redis
  module Backend

    module_function

    # Public: update the tracking metadata for a worker
    def working_on(worker_id, queue_name, payload)
      data = encode \
        :queue   => queue_name,
        :run_at  => Time.now.rfc2822,
        :payload => payload
      redis.set("worker:#{worker_id}", data)
    end

    # Private
    def redis
      Resque.redis
    end

    # XXX copypasta, deprecate Resque::Helpers.encode
    def encode(object)
      Resque.coder.encode(object)
    end

  end

end
