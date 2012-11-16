module Resque
  # Helper methods for fetching information about the runtime environment.
  module Environment
    module_function

    # Returns the String hostname for the worker host
    def hostname
      Socket.gethostname
    end

    # Returns Integer PID of running worker
    def pid
      # Use pid+thread_num
      Process.pid
    end

  end
end
