# -*- coding: utf-8 -*-

class SimpleKVS < Dynamosaurus::DynamoBase
  key :simple_key, :string

  table_schema   :key_schema => 
    [
    { :key_type => "HASH",
      :attribute_name => "simple_key"
    }
    ],
  :provisioned_throughput => {
    :read_capacity_units => 100,
    :write_capacity_units => 10
  },
  :attribute_definitions => [
    {:attribute_name => "simple_key", :attribute_type => "S"},
  ]


end

class SimpleOrderedKVS < Dynamosaurus::DynamoBase
  key :simple_key, :string, :simple_id, :string
  secondary_index :updated_at_index, :updated_at, :number

  table_schema :key_schema => 
    [
    { :key_type => "HASH",
      :attribute_name => "simple_key"
    },
    { :key_type => "RANGE",
      :attribute_name => "simple_id"},
    ],
  :provisioned_throughput => {
    :read_capacity_units => 20,
    :write_capacity_units => 10
  },
  :attribute_definitions => [
    {:attribute_name => "simple_key", :attribute_type => "S"},
    {:attribute_name => "simple_id", :attribute_type => "S"},
    {:attribute_name => "updated_at", :attribute_type => "N"},
  ],
  :local_secondary_indexes => [
    {
      :index_name => "updated_at_index",
      :key_schema => [
                      { :key_type => "HASH",
                        :attribute_name => "simple_key"},
        
                      {
                        :key_type => "RANGE",
                        :attribute_name => "updated_at"
                      }
      ],
      :projection => {
        :projection_type => "ALL"
      },
    },
  ]


end

class Comment < Dynamosaurus::DynamoBase
  key :content_id, :string, :message_id, :string
  global_index :user_index, :user_id, :string

  table_schema :key_schema => 
    [
    { 
      :key_type => "HASH",
      :attribute_name => "content_id"
        },
        {
          :key_type => "RANGE",
          :attribute_name => "message_id"
        }
        ],
      :provisioned_throughput => {
        :read_capacity_units => 100,
        :write_capacity_units => 10
      },
      :attribute_definitions => [
        {:attribute_name => "content_id", :attribute_type => "S"},
        {:attribute_name => "message_id", :attribute_type => "S"},
        {:attribute_name => "user_id", :attribute_type => "S"},
      ],
      :global_secondary_indexes => [
        {
          :index_name => "user_index",
          :key_schema => [
            {
              :key_type => "HASH",
              :attribute_name => "user_id"
            },
          ],
          :projection => {
            :projection_type => "KEYS_ONLY",
          },
          :provisioned_throughput => {
            :read_capacity_units => 50,
            :write_capacity_units => 10
          },
        },
      ]

  
end
