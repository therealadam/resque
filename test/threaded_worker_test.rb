require 'test_helper'

# Think of this like an acceptance test. It (will) exercise everything as its
# wired up when you really run jobs.
describe "Resque::ThreadedWorker" do

  before do
    Resque.create_pool(Resque.redis) # reset state in Resque object

    @worker = Resque::ThreadedWorker.new(:jobs)
    # Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
  end

  # XXX maybe move to a unit test
  it "fetches jobs" do
    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
    job = @worker.fetch
    assert_equal SomeJob, job.payload_class
  end

  # XXX maybe move to a unit test
  it "dispatches jobs" do
    @worker = Resque::ThreadedWorker.new(:jobs, FakeExecutor)
    Resque::Job.create(:jobs, SomeJob, 20)
    @worker.work
    assert_equal 1, FakeExecutor.called
  end

  # XXX copypasta below
  it "can fail jobs" do
    Resque::Job.create(:jobs, BadJob)
    @worker.work
    @worker.shutdown
    assert_equal 1, Resque::Failure.count
  end

  it "failed jobs report exception and message" do
    Resque::Job.create(:jobs, BadJobWithSyntaxError)
    @worker.work(0)
    @worker.shutdown
    assert_equal('SyntaxError', Resque::Failure.all['exception'])
    assert_equal('Extra Bad job!', Resque::Failure.all['error'])
  end

  it "unavailable job definition reports exception and message" do
    Resque::Job.create(:jobs, 'NoJobDefinition')
    @worker.work(0)
    @worker.shutdown

    assert_equal 1, Resque::Failure.count, 'failure not reported'
    assert_equal('NameError', Resque::Failure.all['exception'])
    assert_match('uninitialized constant', Resque::Failure.all['error'])
  end

  it "does not allow exceptions from failure backend to escape" do
    job = Resque::Job.new(:jobs, {})
    with_failure_backend BadFailureBackend do
      @worker.perform job
    end
  end

  it "fails uncompleted jobs with DirtyExit by default on exit" do
    job = Resque::Job.new(:jobs, {'class' => 'GoodJob', 'args' => "blah"})
    @worker.working_on(job)
    @worker.unregister_worker
    @worker.shutdown
    assert_equal 1, Resque::Failure.count
    assert_equal('Resque::DirtyExit', Resque::Failure.all['exception'])
  end

  # TODO replace with a real executor
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
