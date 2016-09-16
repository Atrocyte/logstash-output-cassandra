# encoding: utf-8
require 'thread'
require 'cassandra'
require 'logstash/outputs/cassandra/backoff_retry_policy'

module LogStash; module Outputs; module Cassandra
  # Responsible for submitting parsed actions to cassandra (with or without a retry mechanism)
  class SafeSubmitter
    def initialize(options)
      @statement_cache = {}
      @logger = options['logger']
      setup_cassandra_session(options)
    end

    def submit(actions)
      queries = prepare_queries(actions)
      execute_queries_with_retries(queries)
    end

    private
    def setup_cassandra_session(options)
      @retry_policy = get_retry_policy(options['retry_policy'])
      @consistency = options['consistency'].to_sym
      cluster = options['cassandra'].cluster(
        username: options['username'],
        password: options['password'],
        protocol_version: options['protocol_version'],
        hosts: options['hosts'],
        port: options['port'],
        consistency: @consistency,
        timeout: options['request_timeout'],
        retry_policy: @retry_policy,
        logger: options['logger']
      )
      @session = cluster.connect(options['keyspace'])
    end

    def get_retry_policy(retry_policy)
      case retry_policy['type']
        when 'default'
          return ::Cassandra::Retry::Policies::Default.new
        when 'downgrading_consistency'
          return ::Cassandra::Retry::Policies::DowngradingConsistency.new
        when 'failthrough'
          return ::Cassandra::Retry::Policies::Fallthrough.new
        when 'backoff'
          return ::Cassandra::Retry::Policies::Backoff.new({
            'backoff_type' => retry_policy['backoff_type'], 'backoff_size' => retry_policy['backoff_size'],
            'retry_limit' => retry_policy['retry_limit'], 'logger' => @logger
          })
        else
          raise ArgumentError, "unknown retry policy type: #{retry_policy['type']}"
      end
    end

    def prepare_queries(actions)
      remaining_queries = Queue.new
      actions.each do |action|
        begin
          if action
            query = get_query(action)
            remaining_queries << { :query => query, :arguments => action['data'].values }
          end
        rescue Exception => e
          @logger.error('Failed to prepare query', :action => action, :exception => e, :backtrace => e.backtrace)
        end
      end
      remaining_queries
    end

    def get_query(action)
      @logger.debug('generating query for action', :action => action)
      action_data = action['data']
      query =
"INSERT INTO #{action['table']} (#{action_data.keys.join(', ')})
VALUES (#{('?' * action_data.keys.count).split(//) * ', '})"
      unless @statement_cache.has_key?(query)
        @logger.debug('preparing new query', :query => query)
        @statement_cache[query] = @session.prepare(query)
      end
      @statement_cache[query]
    end

    def execute_queries_with_retries(queries)
      retries = 0
      while queries.length > 0
        execute_queries(queries, retries)
        retries += 1
      end
    end

    def execute_queries(queries, retries)
      futures = []
      while queries.length > 0
        query = queries.pop
        begin
          future = execute_async(query, retries, queries)
          futures << future
        rescue Exception => e
          @logger.error('Failed to send query', :query => query, :exception => e, :backtrace => e.backtrace)
        end
      end
      futures.each(&:join)
    end

    def execute_async(query, retries, queries)
      future = @session.execute_async(query[:query], arguments: query[:arguments])
      future.on_failure { |error|
        @logger.error('Failed to execute query', :query => query, :error => error)
        if @retry_policy.is_a?(::Cassandra::Retry::Policies::Backoff)
          decision = @retry_policy.retry_with_backoff({ :retries => retries, :consistency => @consistency })
          if decision.is_a?(::Cassandra::Retry::Decisions::Retry)
            queries << query
          end
        end
      }
      future
    end
  end
end end end
