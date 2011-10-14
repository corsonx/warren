require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')
require "#{File.dirname(__FILE__)}/../../../lib/warren/adapters/bunny_adapter.rb"

describe Warren::Queue::BunnyAdapter do

  before do
    # play nicely with other adapters loaded
    Warren::Queue.adapter = Warren::Queue::BunnyAdapter
    setup_config_object
    Warren::Queue.stub(:connection).and_return(Warren::Connection.new(@config))
  end

  describe "connection details" do

    it "should override check_connection_details" do
      Warren::Queue::BunnyAdapter.methods(false).should include("check_connection_details")
    end
    
    it "should require a username" do
      @options.delete(:user)
      lambda {
        Warren::Connection.new(@config)
      }.should raise_error(Warren::Connection::InvalidConnectionDetails, "User not specified")
    end

    it "should require a password" do
      @options.delete(:pass)
      lambda {
        Warren::Connection.new(@config)
      }.should raise_error(Warren::Connection::InvalidConnectionDetails, "Pass not specified")
    end

    it "should require a vhost" do
      @options.delete(:vhost)
      lambda {
        Warren::Connection.new(@config)
      }.should raise_error(Warren::Connection::InvalidConnectionDetails, "Vhost not specified")
    end
  end

  describe "subscribe" do
    it "should override subscribe" do
      Warren::Queue::BunnyAdapter.methods(false).should include("subscribe")
    end

    it "should accept a subscribe block with one argument" do
      blk = lambda do |one|
        one.should == "my message"
      end

      send_headers_to_bunny_with &blk
    end

    it "should accept a subscribe block with two arguments" do
      blk = lambda do | message, headers |
        message.should == "my message"
        headers.should == {:some => :header}
      end

      headers = {
        :payload => "my message",
        :some => :header
      }
      Warren::Queue::BunnyAdapter.__send__(:handle_bunny_message, headers, &blk)
    end

    it "should only reset the connection once after the subscribe block when publishing in the subscribe-block" do
      @client = fake_bunny(:status => :connected)
      Warren::Queue::BunnyAdapter.stub(:client).and_return(@client)

      @client.should_receive(:stop).exactly(:once)

      Warren::Queue::BunnyAdapter.subscribe("foo") do
        Warren::Queue::BunnyAdapter.publish("bar", "blah")
      end
    end

    def send_headers_to_bunny_with &blk
      headers = {
        :payload => "my message",
        :some => :header
      }
      Warren::Queue::BunnyAdapter.__send__(:handle_bunny_message, headers, &blk)
    end
  end

  describe 'client' do

    it 'should return the bunny client' do
      Warren::Queue::BunnyAdapter.client.should be_kind_of(Bunny::Client)
    end

    it 'should return the same bunny client on consecutive calls' do
      client = Warren::Queue::BunnyAdapter.client
      client.should == Warren::Queue::BunnyAdapter.client
    end

    it 'should have the correct host and vhost set' do
      Warren::Queue::BunnyAdapter.client.host.should == 'localhost'
      Warren::Queue::BunnyAdapter.client.vhost.should == '/'
    end

  end

  describe 'reset' do

    it 'should stop the bunny' do
      Warren::Queue::BunnyAdapter.client.should_receive(:stop)
      Warren::Queue::BunnyAdapter.reset
    end

    it 'should force a new bunny instance' do
      old_client = Warren::Queue::BunnyAdapter.client
      Warren::Queue::BunnyAdapter.reset
      Warren::Queue::BunnyAdapter.client.should_not == old_client
    end

  end

  describe 'stay_connected' do

    before do
      @client = fake_bunny(:status => :connected)
      Warren::Queue::BunnyAdapter.stub(:client).and_return(@client)

      @client.should_receive(:stop).exactly(:once)
    end

    it 'should not stop the connection after every publish' do
      Warren::Queue.stay_connected do
        3.times do
          Warren::Queue.publish "testq", :foo => "bar"
        end
      end
    end

    it 'should yield the adapter if the block accept one argument' do
      Warren::Queue.stay_connected do |a|
        a.should == Warren::Queue.adapter
      end
    end

    it 'should allow nesting' do
      Warren::Queue.stay_connected do
        Warren::Queue.stay_connected do
          Warren::Queue.stay_connected do
          end
        end
      end
    end

  end

  protected

  def setup_config_object
    @options = {
      :host => "localhost",
      :user => "rspec",
      :pass => "password",
      :vhost => "/",
      :logging => false
    }
    @config = {
      :development => @options
    }
  end

  def fake_bunny(stubs = {})
    stub({:status   => :not_connected, :qos => nil,
          :queue    => fake_queue,
          :exchange => stub(:publish => nil)}.merge(stubs))
  end

  def fake_queue
    queue = stub
    def queue.subscribe(queue, &block) block.call({:payload => StringIO.new}) end
    queue
  end

end
