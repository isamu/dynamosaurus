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

class DynamoModelWithTableField < Dynamosaurus::DynamoBase
  table name: 'table_name'
  key :content_id, :string, :message_id, :string
end

class DynamoModelWithoutTableField < Dynamosaurus::DynamoBase
  key :content_id, :string, :message_id, :string
end

class Like < Dynamosaurus::DynamoBase
  key :object_id, :string, :user_id, :string
  global_index :user_index, :user_id, :string
  global_index_option :user_index, {projection_type: "ALL"}
end

class Follow < Dynamosaurus::DynamoBase
  key :user_id, :string, :from_user_id, :string

  secondary_index :updated_at_index, :updated_at, :number
  global_index :from_user_index, :from_user_id, :string, :updated_at, :number
end
