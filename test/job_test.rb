require 'test_helper'

$callable_invoked = 0
CallableJob = lambda { |*args| $callable_invoked = 1 }

class Callable

  def initialize
    @called = false
  end

  def call(*args)
    @called = true
  end

  def called?
    @called
  end

end

ObjectJob = Callable.new

class ClassJob

  @called = false
  def self.called?
    @called
  end

  def self.call(*args)
    @called = true
  end

end

class PerformableJob
  def self.perform
    raise "whups"
  end
end

describe "Resque::Job" do

  it "runs callable jobs" do
    job = Resque::Job.new(:jobs, {'class' => 'CallableJob', 'args' => []})
    job.perform
    assert_equal 1, $callable_invoked
  end

  it "runs job objects" do
    job = Resque::Job.new(:jobs, {'class' => 'ObjectJob', 'args' => []})
    job.perform
    assert ObjectJob.called?
  end

  it "runs job classes" do
    job = Resque::Job.new(:jobs, {'class' => 'ClassJob', 'args' => []})
    job.perform
    assert ClassJob.called?
  end

  it "runs classes with a perform method" do
    job = Resque::Job.new(:jobs, {'class' => 'PerformableJob', 'args' => []})
    assert_raises(RuntimeError) { job.perform }
  end

end

