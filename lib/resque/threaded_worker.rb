module Resque
  class ThreadedWorker
    CORES = 4 # XXX do this right

    # Public
    def initialize(queue_names, executor=Resque::ThreadedExecutorPool)
      @queue_names = Array(queue_names)
      @executor = executor.new(CORES)
    end

    # Public
    def work(polling_interval=5.0)
      perform(fetch)
    end

    # Public
    def perform(job)
      dispatch(job)
    end

    # Public
    def shutdown
      @executor.shutdown
    end

    # Private
    def fetch
      queue = MultiQueue.new(
        @queue_names.map { |queue| Queue.new(queue, Resque.pool, Resque.coder) },
        Resque.pool)
      queue, job = queue.pop(true)
      Job.new(queue.name, job)
    end

    # Private
    def dispatch(job)
      runnable = -> {
        begin
          job.perform
        rescue Object => e
          job.fail(e)
        end
      }
      @executor.execute(runnable)
    end

    include Resque::Helpers # XXX extract a real helper object?

    def worker_id
      @worker_id ||= begin
                queues = @queue_names.map { |q| q.to_s.strip }.join(',')
                [Resque::Environment.hostname,
                 Resque::Environment.pid,queues].join(':')
              end
    end

    # Private: Given a job, tells Redis we're working on it. Useful for seeing
    # what workers are doing and when.
    def working_on(job)
      backend.working_on(worker_id, job.queue, job.payload)
    end

    def backend
      Resque::Backend
    end

    # Unregisters ourself as a worker. Useful when shutting down.
    def unregister_worker(exception = nil)
      processing = JSON.load(redis.get("worker:#{worker_id}")) # XXX no idea how processing is set in Worker
      # XXX copypasta
      # If we're still processing a job, make sure it gets logged as a
      # failure.
      if (hash = processing) && !hash.empty?
        job = Job.new(hash['queue'], hash['payload'])
        # Ensure the proper worker is attached to this job, even if
        # it's not the precise instance that died.
        job.worker = self
        job.fail(exception || DirtyExit.new)
      end

      redis.srem(:workers, worker_id)
      redis.del("worker:#{worker_id}")
      redis.del("worker:#{worker_id}:started")

      Stat.clear("processed:#{worker_id}")
      Stat.clear("failed:#{worker_id}")
    end

  end
end

