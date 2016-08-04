require 'spec_helper'

describe Dynamosaurus::DynamoBase do
  before(:all) do
    ENV['DYNAMODB_SUFFIX'] = "_local"

    Aws.config = {
      :endpoint => "http://localhost:8000",
      :region => 'local_test',
    }
    Dynamosaurus::DynamoBase.create_tables
  end

  after(:all) do
    connect = Dynamosaurus::DynamoBase.dynamo_db
    Dynamosaurus::DynamoBase.tables.each do |table_name|
      connect.delete_table(:table_name => table_name)
    end
  end

  it 'simple ordered kvs test' do
    expect(Dynamosaurus::DynamoBase.all_models).to match_array [Follow, Like, DynamoModelWithoutTableField, DynamoModelWithTableField, Comment, SimpleOrderedKVS, SimpleKVS]
    expect(Dynamosaurus::DynamoBase.tables).to match_array  ["comment_local", "dynamomodelwithouttablefield_local", "follow_local", "like_local", "simplekvs_local", "simpleorderedkvs_local", "table_name"]

    expect(SimpleOrderedKVS.table_name).to match "simpleorderedkvs_local"
    expect(SimpleOrderedKVS.get_key).to match_array [:simple_key, :s, :simple_id, :s]

    expect(SimpleOrderedKVS.schema).to match_array [
      [ :attribute_definitions, [
        {:attribute_name=>"simple_key", :attribute_type=>"S"},
        {:attribute_name=>"simple_id", :attribute_type=>"S"},
        {:attribute_name=>"updated_at", :attribute_type=>"N"}
      ]],
      [ :key_schema, [
        {:key_type=>"HASH", :attribute_name=>"simple_key"}, {:key_type=>"RANGE", :attribute_name=>"simple_id"}
      ]],
      [ :local_secondary_indexes, [
        { :index_name=>:updated_at_index,
          :key_schema=>[ {:key_type=>"HASH", :attribute_name=>:simple_key}, {:key_type=>"RANGE", :attribute_name=>:updated_at}],
          :projection=>{:projection_type=>"KEYS_ONLY"}}
      ]],
      [:provisioned_throughput, {:read_capacity_units=>10, :write_capacity_units=>10}], [:table_name, "simpleorderedkvs_local"]
    ]
    expect(SimpleOrderedKVS.get_global_indexes).to match({})
    expect(SimpleOrderedKVS.get_secondary_indexes).to match({:updated_at_index=>[:simple_key, :s, :updated_at, :n]})
    expect(SimpleOrderedKVS.get_indexes).to match({:updated_at_index=>[:simple_key, :s, :updated_at, :n]})


    expect(SimpleKVS.schema).to match_array [
      [:attribute_definitions, [
        {:attribute_name=>"simple_key", :attribute_type=>"S"}]
      ],
      [:key_schema, [
        {:key_type=>"HASH", :attribute_name=>"simple_key"}
      ]],
      [:provisioned_throughput, {:read_capacity_units=>10, :write_capacity_units=>10}], [:table_name, "simplekvs_local"]
    ]

    expect(Comment.schema).to match_array  [[:attribute_definitions, [{:attribute_name=>"content_id", :attribute_type=>"S"}, {:attribute_name=>"message_id", :attribute_type=>"S"}, {:attribute_name=>"user_id", :attribute_type=>"S"}]], [:global_secondary_indexes, [{:index_name=>:user_index, :key_schema=>[{:key_type=>"HASH", :attribute_name=>:user_id}], :projection=>{:projection_type=>"KEYS_ONLY"}, :provisioned_throughput=>{:read_capacity_units=>10, :write_capacity_units=>10}}]], [:key_schema, [{:key_type=>"HASH", :attribute_name=>"content_id"}, {:key_type=>"RANGE", :attribute_name=>"message_id"}]], [:provisioned_throughput, {:read_capacity_units=>10, :write_capacity_units=>10}], [:table_name, "comment_local"]]

  end
end
