# encoding: utf-8
require_relative "../cassandra_spec_helper"
require "logstash/outputs/cassandra/event_parser"

RSpec.describe LogStash::Outputs::Cassandra::EventParser do
  let(:sut) { LogStash::Outputs::Cassandra::EventParser }
  let(:default_opts) {{
    'logger' => double(),
    'table' => 'dummy',
    'filter_transform_event_key' => nil,
    'filter_transform' => nil,
    'hints' => {},
    'ignore_bad_values' => false
  }}
  let(:sample_event) { LogStash::Event.new("message" => "sample message here") }

  describe "table name parsing" do
    it "leaves regular table names unchanged" do
      sut_instance = sut().new(default_opts.update({ "table" => "simple" }))

      action = sut_instance.parse(sample_event)

      expect(action["table"]).to(eq("simple"))
    end

    it "parses table names with data from the event" do
      sut_instance = sut().new(default_opts.update({ "table" => "%{[a_field]}" }))
      sample_event["a_field"] = "a_value"

      action = sut_instance.parse(sample_event)

      expect(action["table"]).to(eq("a_value"))
    end
  end

  describe "filter transforms" do
    describe "from config" do
      describe "malformed configurations" do
        it "fails if the transform has no event_data setting" do
          expect { sut().new(default_opts.update({ "filter_transform" => [{ "column_name" => "" }] })) }.to raise_error(/item is incorrectly configured/)
        end

        it "fails if the transform has no column_name setting" do
          expect { sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "" }] })) }.to raise_error(/item is incorrectly configured/)
        end
      end

      describe "properly configured" do
        it "maps the event key to the column" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column" }] }))
          sample_event["a_field"] = "a_value"

          action = sut_instance.parse(sample_event)

          expect(action["data"]["a_column"]).to(eq("a_value"))
        end

        it "works with multiple filter transforms" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column" }, { "event_key" => "another_field", "column_name" => "a_different_column" }] }))
          sample_event["a_field"] = "a_value"
          sample_event["another_field"] = "a_second_value"

          action = sut_instance.parse(sample_event)

          expect(action["data"]["a_column"]).to(eq("a_value"))
          expect(action["data"]["a_different_column"]).to(eq("a_second_value"))
        end

        it "allows for event specific event keys" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "%{[pointer_to_another_field]}", "column_name" => "a_column" }] }))
          sample_event["pointer_to_another_field"] = "another_field"
          sample_event["another_field"] = "a_value"

          action = sut_instance.parse(sample_event)

          expect(action["data"]["a_column"]).to(eq("a_value"))
        end

        it "allows for event specific column names" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "%{[pointer_to_another_field]}" }] }))
          sample_event["a_field"] = "a_value"
          sample_event["pointer_to_another_field"] = "a_different_column"

          action = sut_instance.parse(sample_event)

          expect(action["data"]["a_different_column"]).to(eq("a_value"))
        end

      end

      describe "cassandra type mapping" do
        [
          { :name => 'timestamp', :type => ::Cassandra::Types::Timestamp, :value => Time::parse("1970-01-01 00:00:00") },
          { :name => 'inet',      :type => ::Cassandra::Types::Inet,      :value => "0.0.0.0" },
          { :name => 'float',     :type => ::Cassandra::Types::Float,     :value => "10.15" },
          { :name => 'varchar',   :type => ::Cassandra::Types::Varchar,   :value => "a varchar" },
          { :name => 'text',      :type => ::Cassandra::Types::Text,      :value => "some text" },
          { :name => 'blob',      :type => ::Cassandra::Types::Blob,      :value => "12345678" },
          { :name => 'ascii',     :type => ::Cassandra::Types::Ascii,     :value => "some ascii" },
          { :name => 'bigint',    :type => ::Cassandra::Types::Bigint,    :value => "100" },
          { :name => 'counter',   :type => ::Cassandra::Types::Counter,   :value => "15" },
          { :name => 'int',       :type => ::Cassandra::Types::Int,       :value => "123" },
          { :name => 'varint',    :type => ::Cassandra::Types::Varint,    :value => "345" },
          { :name => 'boolean',   :type => ::Cassandra::Types::Boolean,   :value => "true" },
          { :name => 'decimal',   :type => ::Cassandra::Types::Decimal,   :value => "0.12E2" },
          { :name => 'double',    :type => ::Cassandra::Types::Double,    :value => "123.65" },
          { :name => 'timeuuid',  :type => ::Cassandra::Types::Timeuuid,  :value => "00000000-0000-0000-0000-000000000000" }
        ].each { |mapping|
          # NOTE: this is not the best test there is, but it is the best / simplest I could think of :/
          it "properly maps #{mapping[:name]} to #{mapping[:type]}" do
            sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column", "cassandra_type" => mapping[:name] }] }))
            sample_event["a_field"] = mapping[:value]

            action = sut_instance.parse(sample_event)

            expect(action["data"]["a_column"].to_s).to(eq(mapping[:value].to_s))
          end
        }

        it "properly maps sets to their specific set types" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column", "cassandra_type" => "set(int)" }] }))
          original_value = [ 1, 2, 3 ]
          sample_event["a_field"] = original_value

          action = sut_instance.parse(sample_event)

          expect(action["data"]["a_column"].to_a).to(eq(original_value))
        end

        it "allows for event specific cassandra types" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column", "cassandra_type" => "%{[pointer_to_a_field]}" }] }))
          sample_event["a_field"] = "123"
          sample_event["pointer_to_a_field"] = "int"

          action = sut_instance.parse(sample_event)

          expect(action["data"]["a_column"]).to(eq(123))
        end

        it "fails in case of an unknown type" do
          sut_instance = sut().new(default_opts.update({ "filter_transform" => [{ "event_key" => "a_field", "column_name" => "a_column", "cassandra_type" => "what?!" }] }))
          sample_event["a_field"] = "a_value"

          expect { sut_instance.parse(sample_event) }.to raise_error(/Unknown cassandra_type/)
        end
      end
    end

    describe "from event" do
      it "obtains the filter transform from the event if defined" do
        sut_instance = sut().new(default_opts.update({ "filter_transform_event_key" => "an_event_filter" }))
        sample_event["a_field"] = "a_value"
        sample_event["an_event_filter"] = [{ "event_key" => "a_field", "column_name" => "a_column" }]

        action = sut_instance.parse(sample_event)

        expect(action["data"]["a_column"]).to(eq("a_value"))
      end
    end
  end

  describe "hints" do
    it "removes fields starting with @" do
      sut_instance = sut().new(default_opts.update({ "hints" => {} }))
      sample_event["leave"] = "a_value"
      sample_event["@remove"] = "another_value"

      action = sut_instance.parse(sample_event)

      expect(action["data"]["leave"]).to(eq("a_value"))
      expect(action["data"]).not_to(include("@remove"))
    end

    it "does not attempt to change items with no hints" do
      sut_instance = sut().new(default_opts.update({ "hints" => {} }))
      expected_value = [ 1, 2, 3 ]
      sample_event["no_hint_here"] = expected_value

      action = sut_instance.parse(sample_event)

      expect(action["data"]["no_hint_here"]).to(equal(expected_value))
    end

    it "converts items with hints" do
      sut_instance = sut().new(default_opts.update({ "hints" => { "a_set" => "set(int)", "an_int" => "int" } }))
      original_set = [ 1, 2, 3 ]
      sample_event["a_set"] = original_set
      sample_event["an_int"] = "123"

      action = sut_instance.parse(sample_event)

      expect(action["data"]["a_set"]).to(be_a(Set))
      expect(action["data"]["a_set"].to_a).to(eql(original_set))
      expect(action["data"]["an_int"]).to(eql(123))
    end

    it "fails for unknown hint types" do
      sut_instance = sut().new(default_opts.update({ "hints" => { "a_field" => "not_a_real_type" } }))

      sample_event["a_field"] = "a value"

      expect { sut_instance.parse(sample_event) }.to raise_error(/Unknown cassandra_type/)
    end

    it "fails for unsuccessful hint conversion" do
      options = default_opts.update({ "hints" => { "a_field" => "int" } })
      expect(options["logger"]).to(receive(:error))

      sut_instance = sut().new(options)

      sample_event["a_field"] = "i am not an int!!!"
      expect { sut_instance.parse(sample_event) }.to raise_error(/Cannot convert/)
    end
  end

  describe "ignore_bad_values is turned on" do
    [
        { :name => 'timestamp', :value => "i dont have to_time",      :expected => Time::parse("1970-01-01 00:00:00") },
        { :name => 'inet',      :value => "i am not an inet address", :expected => "0.0.0.0" },
        { :name => 'float',     :value => "i am not a float",         :expected => 0.0 },
        { :name => 'bigint',    :value => "i am not a bigint",        :expected => 0 },
        { :name => 'counter',   :value => "i am not a counter",       :expected => 0 },
        { :name => 'int',       :value => "i am not a int",           :expected => 0 },
        { :name => 'varint',    :value => "i am not a varint",        :expected => 0 },
        { :name => 'double',    :value => "i am not a double",        :expected => 0 },
        { :name => 'timeuuid',  :value => "i am not a timeuuid",      :expected => "00000000-0000-0000-0000-000000000000" }
    ].each { |mapping|
      # NOTE: this is not the best test there is, but it is the best / simplest I could think of :/
      it "properly defaults #{mapping[:name]}" do
        options = default_opts.update({ "ignore_bad_values" => true, "hints" => { "a_field" => mapping[:name] } })
        expect(options["logger"]).to(receive(:warn))
        sut_instance = sut().new(options)
        sample_event["a_field"] = mapping[:value]

        action = sut_instance.parse(sample_event)

        expect(action["data"]["a_field"].to_s).to(eq(mapping[:expected].to_s))
      end
    }

    it "properly default sets" do
      options = default_opts.update({ "ignore_bad_values" => true, "hints" => { "a_field" => "set(float)" } })
      expect(options["logger"]).to(receive(:warn))
      sut_instance = sut().new(options)
      sample_event["a_field"] = "i am not a set"

      action = sut_instance.parse(sample_event)

      expect(action["data"]["a_field"].to_a).to(eq([]))
    end
  end
end
