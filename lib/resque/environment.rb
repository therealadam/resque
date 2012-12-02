module Resque

  module Environment

    module_function

    # Given a string, sets the procline ($0) and logs.
    # Procline is always in the format of:
    #   resque-VERSION: STRING
    def procline(string)
      $0 = "resque-#{Resque::Version}: #{string}"
      Resque.logger.debug $0
    end

    # Returns an Array of string pids of all the other workers on this
    # machine. Useful when pruning dead workers on startup.
    def worker_pids
      if RUBY_PLATFORM =~ /solaris/
        solaris_worker_pids
      elsif RUBY_PLATFORM =~ /mingw32/
        windows_worker_pids
      else
        linux_worker_pids
      end
    end

    # Find Resque worker pids on Windows.
    #
    # Returns an Array of string pids of all the other workers on this
    # machine. Useful when pruning dead workers on startup.
    def windows_worker_pids
      `tasklist  /FI "IMAGENAME eq ruby.exe" /FO list`.split($/).select { |line| line =~ /^PID:/}.collect{ |line| line.gsub /PID:\s+/, '' }
    end

    # Find Resque worker pids on Linux and OS X.
    #
    def linux_worker_pids
      get_worker_pids('ps -A -o pid,command')
    end

    # Find Resque worker pids on Solaris.
    #
    def solaris_worker_pids
      get_worker_pids('ps -A -o pid,args')
    end

    # Find worker pids - platform independent
    #
    # Returns an Array of string pids of all the other workers on this
    # machine. Useful when pruning dead workers on startup.
    def get_worker_pids(command)
       active_worker_pids = []
       output = %x[#{command}]  # output format of ps must be ^<PID> <COMMAND WITH ARGS>
       raise 'System call for ps command failed. Please make sure that you have a compatible ps command in the path!' unless $?.success?
       output.split($/).each{|line|
        next unless line =~ /resque/i
        next if line =~ /resque-web/
        active_worker_pids.push line.split(' ')[0]
       }
       active_worker_pids
    end

    def hostname
      Socket.gethostname
    end

    # Returns Integer PID of running worker
    def pid
      Process.pid
    end

  end

end
