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

    # Private: Unregisters ourself as a worker. Useful when shutting down.
    def unregister_worker(exception = nil)
      fail_current_job(exception)
      backend.remove_worker_info(worker_id)
      backend.clear_worker_stats(worker_id)
    end

    # Private: If we're still processing a job, make sure it gets logged as a
    # failure.
    def fail_current_job(exception)
      current_job = backend.current_job(worker_id)
      return if current_job.empty?

      job = Job.new(current_job['queue'], current_job['payload'])

      # Ensure the proper worker is attached to this job, even if
      # it's not the precise instance that died.
      job.worker = self
      job.fail(exception || DirtyExit.new)
    end

  end
end

