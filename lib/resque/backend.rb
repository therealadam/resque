module Resque

  # Helpers for talking to Redis.
  module Backend
    module_function

    def all_workers
      Array(redis.smembers(:workers))
    end

    def reportedly_working(names)
      working = {}
      working = redis.mapped_mget(*names).reject do |key, value|
        value.nil? || value.empty?
      end
      working
    rescue Redis::Distributed::CannotDistribute
      names.each do |name|
        value = redis.get name
        working[name] = value unless value.nil? || value.empty?
      end
      working
    end

    def worker_exists?(worker_id)
      redis.sismember(:workers, worker_id)
    end

    # Reconnect to Redis to avoid sharing a connection with the parent,
    # retry up to 3 times with increasing delay before giving up.
    def reconnect
      tries = 0
      begin
        redis.client.reconnect
      rescue Redis::BaseConnectionError
        if (tries += 1) <= 3
          Resque.logger.info "Error reconnecting to Redis; retrying"
          sleep(tries)
          retry
        else
          Resque.logger.info "Error reconnecting to Redis; quitting"
          raise
        end
      end
    end

    def add_worker(worker)
      redis.sadd(:workers, worker)
    end

    def remove_worker(worker)
      redis.srem(:workers, worker)
      redis.del("worker:#{worker}")
      redis.del("worker:#{worker}:started")
    end

    # Given a job, tells Redis we're working on it. Useful for seeing
    # what workers are doing and when.
    def working_on(job, worker)
      data = Resque.coder.encode \
        :queue   => job.queue,
        :run_at  => Time.now.rfc2822,
        :payload => job.payload
      redis.set("worker:#{worker}", data)
    end

    def remove_working_entry(worker)
      redis.del("worker:#{worker}")
    end

    def set_worker_started_at(worker)
      redis.set("worker:#{worker}:started", Time.now.rfc2822)
    end

    def get_worker_started_at(worker)
      redis.get "worker:#{worker}:started"
    end

    def fetch_job(worker)
      Resque.coder.decode(redis.get("worker:#{worker}")) || {}
    end

    def worker_state(worker)
      redis.exists("worker:#{worker}")
    end

    # Private: Just an internal helper.
    def redis
      Resque.redis
    end

  end

end

