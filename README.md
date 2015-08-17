# Dynamosaurus

## Installation

Add this line to your application's Gemfile:

    gem 'dynamosaurus'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install dynamosaurus

## Usage

    require "dynamosaurus"
    
    class SimpleKVS < Dynamosaurus::DynamoBase
      key :simple_key, :string
    end
    
    aws_config = {
      :access_key_id => 'aws_dynamodb_access_key_id',
      :secret_access_key => 'aws_dynamodb_secret_access_key',
      :region => 'us-west-1',
    }
    Aws.config = aws_config
    
    # create
    SimpleKVS.put({:simple_key => "key"}, {:num => 1})
    
    # read
    kvs = SimpleKVS.get("key")
    
    # update
    kvs.update({:test => 1})
    
    # update
    kvs.num = 100
    kvs.save
    
    # delete
    kvs.delete
    
    class SimpleOrderedKVS < Dynamosaurus::DynamoBase
      key :simple_key, :string, :simple_id, :string
      secondary_index :updated_at_index, :updated_at, :number
    end
    
    # create
    SimpleOrderedKVS.put({:simple_key => "key", :simple_id => "1"})
    
    # get
    SimpleOrderedKVS.get(["key", "1"])
    
    # force use secondary index
    SimpleOrderedKVS.get({
      :index => "updated_at_index",
      :simple_key => "key"
    },{
      :scan_index_forward => false,
      :limit => 50,
      })
    
    # automatically use secondary index
    SimpleOrderedKVS.get({:simple_key => "key"})
    
    class Comment < Dynamosaurus::DynamoBase
      key :content_id, :string, :message_id, :string
      global_index :user_index, :user_id, :string
    end      
    
    Comment.put({:content_id => "1", :message_id => "1", :user_id => "abc"})
    Comment.put({:content_id => "1", :message_id => "2", :user_id => "abc"})
    Comment.put({:content_id => "1", :message_id => "3", :user_id => "xyz"})
    
    # automatically use global index    
    comments = Comment.get({:user_id => "abc"})



## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
