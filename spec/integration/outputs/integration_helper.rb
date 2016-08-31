# encoding: utf-8
require_relative '../../cassandra_spec_helper'
require 'longshoreman'
require 'cassandra'

CONTAINER_NAME = "logstash-output-cassandra-#{rand(999).to_s}"
CONTAINER_IMAGE = 'cassandra'
CONTAINER_TAG = '3.0'

module CassandraHelper
  def get_host_ip
    Longshoreman.new.get_host_ip
  end

  def get_port
    container = Longshoreman::Container.new
    container.get(CONTAINER_NAME)
    container.rport(9042)
  end

  def get_session
    cluster = ::Cassandra.cluster(
        username: 'cassandra',
        password: 'cassandra',
        port: get_port,
        hosts: [get_host_ip]
    )
    cluster.connect
  end
end


RSpec.configure do |config|
  config.include CassandraHelper

  # this :all hook gets run before every describe block that is tagged with :integration => true.
  config.before(:all, :docker => true) do
    # check if container exists already before creating new one.
    begin
      ls = Longshoreman::new
      ls.container.get(CONTAINER_NAME)
    rescue Docker::Error::NotFoundError
      create_retry = 0
      begin
        Longshoreman.new("#{CONTAINER_IMAGE}:#{CONTAINER_TAG}", CONTAINER_NAME, {
            'HostConfig' => {
                'PublishAllPorts' => true
            }
        })
        connect_retry = 0
        begin
          get_session
        rescue ::Cassandra::Errors::NoHostsAvailable
          # retry connecting for a minute
          connect_retry += 1
          if connect_retry <= 60
            sleep(1)
            retry
          else
            raise
          end
        end
      rescue Docker::Error::NotFoundError
        # try to pull the image once if it does not exist
        create_retry += 1
        if create_retry <= 1
          Longshoreman.pull_image(CONTAINER_IMAGE, CONTAINER_TAG)
          retry
        else
          raise
        end
      end
    end
  end

  # we want to do a final cleanup after all :integration runs,
  # but we don't want to clean up before the last block.
  # This is a final blind check to see if the ES docker container is running and
  # needs to be cleaned up. If no container can be found and/or docker is not
  # running on the system, we do nothing.
  config.after(:suite) do
    # only cleanup docker container if system has docker and the container is running
    begin
      ls = Longshoreman::new
      ls.container.get(CONTAINER_NAME)
      ls.cleanup
    rescue Docker::Error::NotFoundError, Excon::Errors::SocketError
      # do nothing
    end
  end
end
