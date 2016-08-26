require 'spec_helper'

describe Dynamosaurus::DynamoBase do
  before(:all) do
    ENV['DYNAMODB_SUFFIX'] = "_local_spec"

    Aws.config = {
      :endpoint => "http://localhost:8000",
      :region => 'local_test_spec',
    }
    Dynamosaurus::DynamoBase.create_tables
  end

  after(:all) do
    connect = Dynamosaurus::DynamoBase.dynamo_db
    Dynamosaurus::DynamoBase.tables.each do |table_name|
      connect.delete_table(:table_name => table_name)
    end
  end

  it 'simple kvs add operation test' do
    kvs = SimpleKVS.save({simple_key: "key1"}, {num: 1})
    expect(kvs.num.to_i).to be (1)

    kvs.add({num: 1})
    expect(kvs.num.to_i).to be (2)
    kvs = SimpleKVS.get("key1")
    expect(kvs.num.to_i).to be (2)

    kvs.add({num: 1})
    expect(kvs.num.to_i).to be (3)
    kvs = SimpleKVS.get("key1")
    expect(kvs.num.to_i).to be (3)

    kvs.add({num: -2})
    expect(kvs.num.to_i).to be (1)
    kvs = SimpleKVS.get("key1")
    expect(kvs.num.to_i).to be (1)

    kvs = SimpleOrderedKVS.save({simple_key: "key", simple_id: "1", num: 10})
    expect(kvs.num.to_i).to be (10)

    kvs.add({num: 1})
    expect(kvs.num.to_i).to be (11)

    kvs = SimpleOrderedKVS.get(["key", "1"])
    expect(kvs.num.to_i).to be (11)


  end

end
