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

    # Public: remove `worker_id` from the list of current workers
    def remove_worker_info(worker_id)
      redis.srem(:workers, worker_id)
      redis.del("worker:#{worker_id}")
      redis.del("worker:#{worker_id}:started")
    end

    # Public: delete stats for `worker_id`
    def clear_worker_stats(worker_id)
      Stat.clear("processed:#{worker_id}")
      Stat.clear("failed:#{worker_id}")
    end

    # Public: fetch the job being processed by `worker_id`
    def current_job(worker_id)
      decode(redis.get("worker:#{worker_id}")) || {}
    end

    # Private
    def redis
      Resque.redis
    end

    # XXX copypasta, deprecate Resque::Helpers.encode
    def encode(object)
      Resque.coder.encode(object)
    end

    # XXX copypasta, deprecate Resque::Helpers.decode
    def decode(object)
      Resque.coder.decode(object)
    end
  end

end
