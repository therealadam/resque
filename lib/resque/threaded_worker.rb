module Resque
  class ThreadedWorker
    CORES = 4 # XXX do this right

    def initialize(queue_names, executor=Resque::ThreadedExecutorPool)
      @queue_names = Array(queue_names)
      @executor = executor.new(CORES)
    end

    def fetch
      queue = MultiQueue.new(
        @queue_names.map { |queue| Queue.new(queue, Resque.pool, Resque.coder) },
        Resque.pool)
      queue, job = queue.pop(true)
      Job.new(queue.name, job)
    end

    def work
      job      = fetch
      runnable = -> { job.perform }
      @executor.execute(runnable)
    end

  end
end

