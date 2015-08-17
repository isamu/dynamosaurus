require 'spec_helper'

describe Dynamosaurus do
  before(:all) do  
    ENV['DYNAMODB_SUFFIX'] = "_local"

    Dynamosaurus::Logger.new('log/vm.log', :debug)
    Aws.config = {
      :endpoint => "http://localhost:8000",
      :region => 'local_test',
    }

    Dynamosaurus::DynamoBase.create_table
  end

  after(:all) do  
    connect = Dynamosaurus::DynamoBase.dynamo_db
    Dynamosaurus::DynamoBase.tables.each do |table_name|
      connect.delete_table(:table_name => table_name)
    end
  end

  it 'should have a version number' do
    expect(Dynamosaurus::VERSION).not_to be_nil
  end

  it 'simple ordered kvs test' do
    expect(SimpleOrderedKVS.first).to be_nil

    SimpleOrderedKVS.put({:simple_key => "key", :simple_id => "1"})

    kvs = SimpleOrderedKVS.first
    expect(kvs.simple_key).to eq "key"
    expect(kvs.simple_id).to eq "1"

    SimpleOrderedKVS.get(["key", "1"])
    expect(kvs.simple_id).to eq "1"

    sleep 1
    SimpleOrderedKVS.put({:simple_key => "key", :simple_id => "3"})
    sleep 1
    SimpleOrderedKVS.put({:simple_key => "key", :simple_id => "2"})
    
    orderd_items = SimpleOrderedKVS.get({
      :index => "updated_at_index",
      :simple_key => "key"
    },{
      :scan_index_forward => false,
      :limit => 50,
      })
    expect(orderd_items[0].simple_id).to eq "2"
    expect(orderd_items[1].simple_id).to eq "3"
    expect(orderd_items[2].simple_id).to eq "1"

    expect(SimpleOrderedKVS.get({:simple_key => "key"}).size).to eq 3


    batch_items = SimpleOrderedKVS.batch_get_item({:simple_key => ["key"], :simple_id => ["1", "2", "3"]})
    expect(orderd_items.size).to eq 3

    kvs = SimpleOrderedKVS.first
    kvs2 = SimpleOrderedKVS.get({:simple_key => kvs.simple_key, :updated_at => kvs.updated_at})
    expect(kvs.simple_id).to eq kvs2[0].simple_id

  end

  it 'simple kvs test' do
    expect(SimpleKVS.first).to be_nil
    expect(SimpleKVS.get("key")).to be_nil

    SimpleKVS.put({:simple_key => "key"}, {:num => 1})
    expect(SimpleKVS.first.num).to eq 1

    kvs = SimpleKVS.get("key")
    expect(kvs.num).to eq 1

    kvs = SimpleKVS.get("key2")
    expect(kvs).to be_nil

    kvs = SimpleKVS.first
    expect(kvs.num).to eq 1

    SimpleKVS.add("key", {:num => 1})
    expect(SimpleKVS.first.num).to eq 2

    SimpleKVS.put({:simple_key => "key"}, {:num => 1})
    expect(SimpleKVS.first.num).to eq 1

    kvs = SimpleKVS.first
    kvs.update({}, {:test => 1})
    expect(SimpleKVS.first.test).to eq 1

    kvs = SimpleKVS.first
    kvs.update({:test => "1"})
    expect(SimpleKVS.first.test).to eq "1"

    kvs.attr_delete(["test"])
    expect(kvs["test"]).to be_nil

    kvs = SimpleKVS.first
    expect(kvs["test"]).to be_nil

    kvs.num = 100
    kvs.save

    kvs = SimpleKVS.first
    expect(kvs.num).to eq 100

    SimpleKVS.put({:simple_key => "key1"}, {:num => 1})
    SimpleKVS.put({:simple_key => "key2"}, {:num => 2})
    SimpleKVS.put({:simple_key => "key3"}, {:num => 3})

    expect(SimpleKVS.all.size).to eq 4

    expect(SimpleKVS.batch_get_item(["key1"]).size).to eq 1
    expect(SimpleKVS.batch_get_item(["key1", "key2"]).size).to eq 2
    expect(SimpleKVS.batch_get_item(["key1", "key10"]).size).to eq 1

    old_kvs = SimpleKVS.delete_item("key2")
    expect(old_kvs.simple_key).to eq "key2"
    expect(SimpleKVS.batch_get_item(["key1", "key2"]).size).to eq 1

    kvs = SimpleKVS.get("key3")
    kvs.delete
    kvs = SimpleKVS.get("key3")
    expect(kvs).to be_nil
    
  end

  it 'comment test' do
    expect(Comment.first).to be_nil
    expect(Comment.get(["1", "1"])).to be_nil

    Comment.put({:content_id => "1", :message_id => "1", :user_id => "abc"})
    expect(Comment.first.content_id).to eq "1"

    kvs = Comment.get(["1", "1"])
    expect(kvs.content_id).to eq "1"

    kvs = Comment.get(["2", "2"])
    expect(kvs).to be_nil

    Comment.put({:content_id => "1", :message_id => "2", :user_id => "abc"})
    Comment.put({:content_id => "1", :message_id => "3", :user_id => "xyz"})
    
    # global secondary index
    comments = Comment.get({:user_id => "abc"})
    expect(comments.size).to eq 2

  end

end
