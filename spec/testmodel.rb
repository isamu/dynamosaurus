# -*- coding: utf-8 -*-

class SimpleKVS < Dynamosaurus::DynamoBase
  key :simple_key, :string
end

class SimpleOrderedKVS < Dynamosaurus::DynamoBase
  key :simple_key, :string, :simple_id, :string
  secondary_index :updated_at_index, :updated_at, :number
end

class Comment < Dynamosaurus::DynamoBase
  key :content_id, :string, :message_id, :string
  global_index :user_index, :user_id, :string
end
