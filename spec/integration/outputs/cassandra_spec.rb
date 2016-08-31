# encoding: utf-8
require_relative './integration_helper'
require 'logstash/outputs/cassandra'

module Helper
  def self.get_assert_timestamp_equallity
    Proc.new do |expect, row, type_to_test|
      expect.call(row['value_column'].to_s).to(eq(Time.at(type_to_test[:value]).to_s))
    end
  end

  def self.get_assert_set_equallity
    Proc.new do |expect, row, type_to_test|
      set_from_cassandra = row['value_column']
      original_value = type_to_test[:value]
      expect.call(set_from_cassandra.size).to(eq(original_value.size))
      set_from_cassandra.to_a.each { |item|
        expect.call(original_value).to(include(item.to_s))
      }
    end
  end
end

describe 'client create actions', :docker => true do
  before(:each) do
    get_session.execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
  end

  after(:each) do
    get_session.execute('DROP KEYSPACE test;')
  end

  def get_sut
    options = {
      'hosts' => [get_host_ip],
      'port' => get_port,
      'keyspace' => 'test',
      'table' => '%{[cassandra_table]}',
      'username' => 'cassandra',
      'password' => 'cassandra',
      'filter_transform_event_key' => 'cassandra_filter'
    }
    sut = LogStash::Outputs::CassandraOutput.new(options)
    return sut
  end

  def create_table(type_to_test)
    get_session.execute("
      CREATE TABLE test.simple(
        idish_column text,
        value_column #{type_to_test[:type]},
        PRIMARY KEY  (idish_column)
      );")
  end

  def build_event(type_to_test)
    options = {
        'cassandra_table' => 'simple',
        'idish_field' => 'some text',
        'value_field' => type_to_test[:value],
        'cassandra_filter' => [
            { 'event_key' => 'idish_field', 'column_name' => 'idish_column' },
            { 'event_key' => 'value_field', 'column_name' => 'value_column', 'cassandra_type' => type_to_test[:type] }
        ]
    }
    LogStash::Event.new(options)
  end

  def assert_proper_insert(type_to_test)
    result = get_session.execute('SELECT * FROM test.simple')
    expect(result.size).to((eq(1)))
    result.each { |row|
      expect(row['idish_column']).to(eq('some text'))
      if type_to_test.has_key?(:assert_override)
        expect_proc = Proc.new do |value|
            return expect(value)
        end
        type_to_test[:assert_override].call(expect_proc, row, type_to_test)
      else
        expect(row['value_column'].to_s).to(eq(type_to_test[:value].to_s))
      end
    }
  end

  [
      { type: 'timestamp', value: 1457606758, assert_override: Helper::get_assert_timestamp_equallity() },
      { type: 'inet', value: '192.168.99.100' },
      { type: 'float', value: '10.050000190734863' },
      { type: 'text', value: 'some text' },
      { type: 'blob', value: 'a blob' },
      { type: 'ascii', value: 'some ascii' },
      { type: 'bigint', value: '123456789' },
      { type: 'int', value: '12345' },
      { type: 'varint', value: '12345678' },
      { type: 'boolean', value: 'true' },
      { type: 'decimal', value: '0.1015E2' },
      { type: 'double', value: '200.54' },
      { type: 'timeuuid', value: 'd2177dd0-eaa2-11de-a572-001b779c76e3' },
      { type: 'set<timeuuid>',
        value: %w(d2177dd0-eaa2-11de-a572-001b779c76e3 d2177dd0-eaa2-11de-a572-001b779c76e4 d2177dd0-eaa2-11de-a572-001b779c76e5), assert_override: Helper::get_assert_set_equallity }
  ].each { |type_to_test|
    it "properly inserts data of type #{type_to_test[:type]}" do
      create_table(type_to_test)
      sut = get_sut
      sut.register
      event = build_event(type_to_test)

      sut.receive(event)
      sut.flush

      assert_proper_insert(type_to_test)
    end
  }
end
