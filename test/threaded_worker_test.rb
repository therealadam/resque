require 'test_helper'

describe "Resque::ThreadedWorker" do

  before do
    Resque.create_pool(Resque.redis) # reset state in Resque object

    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
    @worker = Resque::ThreadedWorker.new(:jobs)
  end

  it "fetches jobs" do
    job = @worker.fetch
    assert_equal SomeJob, job.payload_class
  end

  it "dispatches jobs" do
    @worker = Resque::ThreadedWorker.new(:jobs, FakeExecutor)
    Resque::Job.create(:jobs, SomeJob, 20)
    @worker.work
    assert_equal 1, FakeExecutor.called
  end

  class FakeExecutor
    @@called = 0

    def initialize(cores); end
    def execute(job)
      @@called += 1
    end

    def self.called
      @@called
    end
  end

end
