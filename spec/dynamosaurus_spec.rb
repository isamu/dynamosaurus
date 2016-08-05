require 'spec_helper'

describe Dynamosaurus do
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

  it 'should have a version number' do
    expect(Dynamosaurus::VERSION).not_to be_nil
  end

  it 'simple ordered kvs test' do
    expect(SimpleOrderedKVS.first).to be_nil

    SimpleOrderedKVS.put({:simple_key => "key", :simple_id => "1"})

    kvs = SimpleOrderedKVS.first
    expect(kvs.simple_key).to eq "key"
    expect(kvs.simple_id).to eq "1"

    kvs = SimpleOrderedKVS.get(["key", "1"])
    expect(kvs.simple_id).to eq "1"
    expect(kvs["simple_id"]).to eq "1"
    expect(kvs[:simple_id]).to eq "1"

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
    expect(batch_items.size).to eq 3

    kvs = SimpleOrderedKVS.first
    kvs2 = SimpleOrderedKVS.get({:simple_key => kvs.simple_key, :updated_at => kvs.updated_at})
    expect(kvs.simple_id).to eq kvs2[0].simple_id

    put_items = [{simple_key: "key", simple_id: "a"}, {simple_key: "key", simple_id: "b"}]
    delete_keys = {simple_key: ["key"], simple_id: ["1", "2"]}
    batch_put_items = SimpleOrderedKVS.batch_write_item(put_items, delete_keys)

    batch_items = SimpleOrderedKVS.batch_get_item({:simple_key => ["key"], :simple_id => ["a", "b", "c"]})
    expect(batch_items.size).to eq 2

    batch_items = SimpleOrderedKVS.batch_get_item({:simple_key => ["key"], :simple_id => ["1", "2", "3"]})
    expect(batch_items.size).to eq 1

    orderd_items2 = SimpleOrderedKVS.get({
        index: "updated_at_index",
      },
      {
        scan_index_forward: false,
        limit: 50,
        key_condition_expression: "simple_key = :v_simple_key and updated_at > :v_updated_at",
        expression_attribute_values: {
          ":v_simple_key" => "key",
          ":v_updated_at" =>  0
        }
      })

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

    res = SimpleKVS.save({:simple_key => "key4"}, {:num => 3})
    expect(res.simple_key).to eq "key4"
    expect(res.num).to eq 3
  end

  it 'internel method test' do
    conv_keys = SimpleOrderedKVS.conv_key_array({:simple_key => ["key1", "key2"], :simple_id => ["1", "2", "3"]})
    expect(conv_keys).to eq [
      {"simple_key"=>"key1", "simple_id"=>"1"}, {"simple_key"=>"key1", "simple_id"=>"2"}, {"simple_key"=>"key1", "simple_id"=>"3"},
      {"simple_key"=>"key2", "simple_id"=>"1"}, {"simple_key"=>"key2", "simple_id"=>"2"}, {"simple_key"=>"key2", "simple_id"=>"3"}
    ]

    conv_keys = SimpleOrderedKVS.conv_key_array({:simple_key => "key", :simple_id => ["1", "2", "3"]})
    expect(conv_keys).to eq [
      {"simple_key"=>"key", "simple_id"=>"1"}, {"simple_key"=>"key", "simple_id"=>"2"}, {"simple_key"=>"key", "simple_id"=>"3"}
    ]

    conv_keys = SimpleOrderedKVS.conv_key_array({:simple_key => ["key1","key2"], :simple_id => "1"})
    expect(conv_keys).to eq [
      {"simple_key"=>"key1", "simple_id"=>"1"}, {"simple_key"=>"key2", "simple_id"=>"1"}
    ]

    conv_keys = SimpleOrderedKVS.conv_key_array({:simple_key => "key1", :simple_id => "1"})
    expect(conv_keys).to eq [
      {"simple_key"=>"key1", "simple_id"=>"1"}
    ]

    conv_keys = SimpleOrderedKVS.conv_key_array([{:simple_key => "key1", :simple_id => "1"}, {:simple_key => "key1", :simple_id => "2"}])
    expect(conv_keys).to eq [
      {"simple_key"=>"key1", "simple_id"=>"1"}, {"simple_key"=>"key1", "simple_id"=>"2"}
    ]
    conv_keys = SimpleOrderedKVS.conv_key_array(["key1", "1"])
    expect(conv_keys).to eq [
      {"simple_key"=>"key1", "simple_id"=>"1"}
    ]
    conv_keys = SimpleOrderedKVS.conv_key_array([["key1", "1"], ["key1", "2"]])
    expect(conv_keys).to eq [
      {"simple_key"=>"key1", "simple_id"=>"1"}, {"simple_key"=>"key1", "simple_id"=>"2"}
    ]
  end

  shared_examples_for 'Basic CRUD' do
    it 'should work in a sequence' do
      expect(Object.const_get(model_name.to_s).first).to be_nil
      expect(Object.const_get(model_name.to_s).get(["1", "1"])).to be_nil

      Object.const_get(model_name.to_s).put({:content_id => "1", :message_id => "1", :user_id => "abc"})
      expect(Object.const_get(model_name.to_s).first.content_id).to eq "1"

      kvs = Object.const_get(model_name.to_s).get(["1", "1"])
      expect(kvs.content_id).to eq "1"

      kvs = Object.const_get(model_name.to_s).get(["2", "2"])
      expect(kvs).to be_nil
    end
  end

  describe 'global secondary index' do
    it_should_behave_like 'Basic CRUD' do
      let!(:model_name) { 'Comment' }
    end

    context 'when 2 records exist' do
      before do
        Comment.put({:content_id => "1", :message_id => "2", :user_id => "abc"})
        Comment.put({:content_id => "1", :message_id => "3", :user_id => "xyz"})
      end

      subject { Comment.get({:user_id => "abc"}).size }

      it{ is_expected.to eq 2 }
    end
  end

  describe 'table options' do
    context 'when table-name is given' do
      it_should_behave_like 'Basic CRUD' do
        let!(:model_name) { 'DynamoModelWithTableField' }
      end

      it { expect(DynamoModelWithTableField.table_name).to eq 'table_name' }
    end

    context 'when table-name is not given' do
      it_should_behave_like 'Basic CRUD' do
        let!(:model_name) { 'DynamoModelWithoutTableField' }
      end

      it { expect(DynamoModelWithoutTableField.table_name).to eq 'dynamomodelwithouttablefield_local' }
    end
  end

end
