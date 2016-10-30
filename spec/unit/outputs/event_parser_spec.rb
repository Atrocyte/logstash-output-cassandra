# encoding: utf-8
require_relative '../../cassandra_spec_helper'
require 'logstash/outputs/cassandra/event_parser'

RSpec.describe LogStash::Outputs::Cassandra::EventParser do
  let(:sut) { LogStash::Outputs::Cassandra::EventParser }
  let(:default_opts) {
    logger = double
    allow(logger).to(receive(:debug))
    return {
      'logger' => logger,
      'table' => 'dummy',
      'filter_transform_event_key' => nil,
      'filter_transform' => [],
      'hints' => {},
      'ignore_bad_values' => false
    }
  }
  let(:sample_event) { LogStash::Event.new('message' => 'sample message here') }

  describe 'table name parsing' do
    it 'leaves regular table names unchanged' do
      sut_instance = sut.new(default_opts.update({ 'table' => 'simple' }))

      action = sut_instance.parse(sample_event)

      expect(action['table']).to(eq('simple'))
    end

    it 'allows for string expansion in table names' do
      sut_instance = sut.new(default_opts.update({ 'table' => '%{[a_field]}' }))
      sample_event.set('a_field', 'a_value')

      action = sut_instance.parse(sample_event)

      expect(action['table']).to(eq('a_value'))
    end
  end

  describe 'filter transforms' do
    describe 'from config' do
      describe 'malformed configurations' do
        it 'fails if the transform has no event_key setting' do
          expect { sut.new(default_opts.update({ 'filter_transform' => [{ 'column_name' => '' }] })) }.to raise_error(/item is incorrectly configured/)
        end

        it 'fails if the transform has no column_name setting' do
          expect { sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => '' }] })) }.to raise_error(/item is incorrectly configured/)
        end
      end

      describe 'properly configured' do
        it 'maps the event key to the column' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column' }] }))
          sample_event.set('a_field', 'a_value')

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column']).to(eq('a_value'))
        end

        it 'works with multiple filter transforms' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column' }, { 'event_key' => 'another_field', 'column_name' => 'a_different_column' }] }))
          sample_event.set('a_field', 'a_value')
          sample_event.set('another_field', 'a_second_value')

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column']).to(eq('a_value'))
          expect(action['data']['a_different_column']).to(eq('a_second_value'))
        end

        it 'allows for string expansion in event keys' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => '%{[pointer_to_another_field]}', 'column_name' => 'a_column' }] }))
          sample_event.set('pointer_to_another_field', 'another_field')
          sample_event.set('another_field', 'a_value')

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column']).to(eq('a_value'))
        end

        it 'allows for string expansion only filters for things like date string formats' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => '%{+yyyyMMddHHmm}', 'expansion_only' => true, 'column_name' => 'a_column' }] }))
          expected_value = Time.now.getutc.strftime('%Y%m%d%H%M')

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column']).to(eq(expected_value))
        end

        it 'allows for string expansion in column names' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => '%{[pointer_to_another_field]}' }] }))
          sample_event.set('a_field', 'a_value')
          sample_event.set('pointer_to_another_field', 'a_different_column')

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_different_column']).to(eq('a_value'))
        end

      end

      describe 'cassandra type mapping' do
        [
          { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => Time::parse('1979-07-27 00:00:00 +0000'), expected: Time::parse('1979-07-27 00:00:00 +0000').utc},
          { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => '1982-05-04 00:00:00 +0300', expected: Time::parse('1982-05-04 00:00:00 +0300') },
          { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => 1457606758, expected: Time.at(1457606758) },
          { :name => 'inet',      :type => ::Cassandra::Types::Inet,      :value => '0.0.0.0' },
          { :name => 'float',     :type => ::Cassandra::Types::Float,     :value => '10.15' },
          { :name => 'text',      :type => ::Cassandra::Types::Text,      :value => 'some text' },
          { :name => 'blob',      :type => ::Cassandra::Types::Blob,      :value => '12345678' },
          { :name => 'ascii',     :type => ::Cassandra::Types::Ascii,     :value => 'some ascii' },
          { :name => 'bigint',    :type => ::Cassandra::Types::Bigint,    :value => '100' },
          { :name => 'counter',   :type => ::Cassandra::Types::Counter,   :value => '15' },
          { :name => 'int',       :type => ::Cassandra::Types::Int,       :value => '123' },
          { :name => 'varint',    :type => ::Cassandra::Types::Varint,    :value => '345' },
          { :name => 'boolean',   :type => ::Cassandra::Types::Boolean,   :value => 'true' },
          { :name => 'decimal',   :type => ::Cassandra::Types::Decimal,   :value => '0.12E2' },
          { :name => 'double',    :type => ::Cassandra::Types::Double,    :value => '123.65' },
          { :name => 'timeuuid',  :type => ::Cassandra::Types::Timeuuid,  :value => '00000000-0000-0000-0000-000000000000' }
        ].each { |mapping|
          # NOTE: this is not the best test there is, but it is the best / simplest I could think of :/
          it "properly maps #{mapping[:name]} to #{mapping[:type]}" do
            sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column', 'cassandra_type' => mapping[:name] }] }))
            sample_event.set('a_field', mapping[:value])

            action = sut_instance.parse(sample_event)

            expected_value = mapping.has_key?(:expected) ? mapping[:expected] : mapping[:value]
            expect(action['data']['a_column'].to_s).to(eq(expected_value.to_s))
          end
        }

        it 'properly maps sets to their specific set types' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column', 'cassandra_type' => 'set<int>' }] }))
          original_value = [ 1, 2, 3 ]
          sample_event.set('a_field', original_value)

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column'].to_a).to(eq(original_value))
        end

        it 'properly maps sets to their specific set types for type which also require actual conversion' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column', 'cassandra_type' => 'set<timeuuid>' }] }))
          original_value = %w(00000000-0000-0000-0000-000000000000 00000000-0000-0000-0000-000000000001 00000000-0000-0000-0000-000000000002)
          sample_event.set('a_field', original_value)

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column'].size).to(eq(original_value.size))
          action['data']['a_column'].to_a.each { |item|
            expect(original_value).to(include(item.to_s))
          }
        end

        it 'allows for string expansion in cassandra types' do
          sut_instance = sut.new(default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column', 'cassandra_type' => '%{[pointer_to_a_field]}' }] }))
          sample_event.set('a_field', '123')
          sample_event.set('pointer_to_a_field', 'int')

          action = sut_instance.parse(sample_event)

          expect(action['data']['a_column']).to(eq(123))
        end

        it 'fails in case of an unknown type' do
          options = default_opts.update({ 'filter_transform' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column', 'cassandra_type' => 'what?!' }] })
          sut_instance = sut.new(options)
          sample_event.set('a_field', 'a_value')
          expect(options['logger']).to(receive(:error)).at_least(:once)

          result = sut_instance.parse(sample_event)
          expect(result).to be_nil
        end
      end
    end

    describe 'from event' do
      it 'obtains the filter transform from the event if defined' do
        sut_instance = sut.new(default_opts.update({ 'filter_transform_event_key' => 'an_event_filter' }))
        sample_event.set('a_field', 'a_value')
        sample_event.set('an_event_filter', [{ 'event_key' => 'a_field', 'column_name' => 'a_column' }])

        action = sut_instance.parse(sample_event)

        expect(action['data']['a_column']).to(eq('a_value'))
      end

      it 'obtains the filter transform from the event even when it is in the metadata' do
        sut_instance = sut.new(default_opts.update({ 'filter_transform_event_key' => '[@metadata][the_filter]' }))
        sample_event.set('a_field', 'a_value')
        sample_event.set('@metadata', { 'the_filter' => [{ 'event_key' => 'a_field', 'column_name' => 'a_column' }] })

        action = sut_instance.parse(sample_event)

        expect(action['data']['a_column']).to(eq('a_value'))
      end
    end
  end

  describe 'hints' do
    it 'removes fields starting with @' do
      sut_instance = sut.new(default_opts.update({ 'hints' => {} }))
      sample_event.set('leave', 'a_value')
      sample_event.set('@remove', 'another_value')

      action = sut_instance.parse(sample_event)

      expect(action['data']['leave']).to(eq('a_value'))
      expect(action['data']).not_to(include('@remove'))
    end

    it 'does not attempt to change items with no hints' do
      sut_instance = sut.new(default_opts.update({ 'hints' => {} }))
      expected_value = [ 1, 2, 3 ]
      sample_event.set('no_hint_here', expected_value)

      action = sut_instance.parse(sample_event)

      expect(action['data']['no_hint_here']).to(match_array(expected_value))
    end

    it 'converts items with hints' do
      sut_instance = sut.new(default_opts.update({ 'hints' => { 'a_set' => 'set<int>', 'an_int' => 'int' } }))
      original_set = [ 1, 2, 3 ]
      sample_event.set('a_set', original_set)
      sample_event.set('an_int', '123')

      action = sut_instance.parse(sample_event)

      expect(action['data']['a_set']).to(be_a(Set))
      expect(action['data']['a_set'].to_a).to(eql(original_set))
      expect(action['data']['an_int']).to(eql(123))
    end

    it 'fails for unknown hint types' do
      options = default_opts.update({ 'hints' => { 'a_field' => 'not_a_real_type' } })
      sut_instance = sut.new(options)
      expect(options['logger']).to(receive(:error)).at_least(:once)
      sample_event.set('a_field', 'a value')

      result = sut_instance.parse(sample_event)

      expect(result).to be_nil
    end

    it 'fails for unsuccessful hint conversion' do
      options = default_opts.update({ 'hints' => { 'a_field' => 'int' } })
      expect(options['logger']).to(receive(:error)).at_least(:once)
      sut_instance = sut.new(options)
      sample_event.set('a_field', 'i am not an int!!!')

      result = sut_instance.parse(sample_event)

      expect(result).to be_nil
    end
  end

  describe 'ignore_bad_values is turned on' do
    [
        { :name => 'timestamp', :value => 'i dont have to_time',      :expected => Time::parse('1970-01-01 00:00:00 +0000').localtime },
        { :name => 'inet',      :value => 'i am not an inet address', :expected => '0.0.0.0' },
        { :name => 'float',     :value => 'i am not a float',         :expected => 0.0 },
        { :name => 'bigint',    :value => 'i am not a bigint',        :expected => 0 },
        { :name => 'counter',   :value => 'i am not a counter',       :expected => 0 },
        { :name => 'int',       :value => 'i am not a int',           :expected => 0 },
        { :name => 'varint',    :value => 'i am not a varint',        :expected => 0 },
        { :name => 'double',    :value => 'i am not a double',        :expected => 0.0 },
        { :name => 'timeuuid',  :value => 'i am not a timeuuid',      :expected => '00000000-0000-0000-0000-000000000000' }
    ].each { |mapping|
      # NOTE: this is not the best test there is, but it is the best / simplest I could think of :/
      it "properly defaults #{mapping[:name]}" do
        options = default_opts.update({ 'ignore_bad_values' => true, 'hints' => { 'a_field' => mapping[:name] } })
        expect(options['logger']).to(receive(:warn))
        sut_instance = sut.new(options)
        sample_event.set('a_field', mapping[:value])

        action = sut_instance.parse(sample_event)

        expect(action['data']['a_field'].to_s).to(eq(mapping[:expected].to_s))
      end
    }

    it 'properly default sets' do
      options = default_opts.update({ 'ignore_bad_values' => true, 'hints' => { 'a_field' => 'set<float>' } })
      expect(options['logger']).to(receive(:warn))
      sut_instance = sut.new(options)
      sample_event.set('a_field', 'i am not a set')

      action = sut_instance.parse(sample_event)

      expect(action['data']['a_field'].size).to(be(0))
      expect(action['data']['a_field']).to(be_an_instance_of(Set))
    end

    it 'raises an ArgumentError in case we try to default a type we dont know' do
      options = default_opts.update({ 'ignore_bad_values' => true, 'hints' => { 'a_field' => 'map<float>' } })
      sut_instance = sut.new(options)
      sample_event.set('a_field', 'i am not a set')
      expect(options['logger']).to(receive(:error))

      result = sut_instance.parse(sample_event)

      expect(result).to be_nil
    end
  end
end
