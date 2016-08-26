module Dynamosaurus
  class DynamoBase
    class << self
      def all_models
        subclasses = []
        ObjectSpace.each_object(Dynamosaurus::DynamoBase.singleton_class) do |k|
          subclasses << k if k.superclass == Dynamosaurus::DynamoBase
        end
        subclasses
      end

      def create_tables
        tables = dynamo_db.list_tables.table_names

        Dynamosaurus::DynamoBase.all_models.each do |model_class|
          if tables.index(model_class.table_name).nil?
            model_class.create_table
          end
        end
      end

      def create_table option={}
        schem = schema.merge(option)
        Dynamosaurus.logger << "create table #{schem}"
        dynamo_db.create_table(schem)
      end

      def tables
        Dynamosaurus::DynamoBase.all_models.map do |model_class|
          model_class.table_name
        end
      end

      def table_name
        if @table && @table.has_key?(:name) && !@table[:name].empty?
          @table[:name].to_s
        else
          (@table_prefix || name.downcase.split("::").last) + (ENV['DYNAMODB_SUFFIX'] || "_local")
        end
      end

      def dynamo_db
        return @dynamo_db if @dynamo_db
        if Aws::config.empty?
          @dynamo_db = Aws::DynamoDB::Client.new(
            :endpoint => "http://localhost:8000",
            :region => "us-west-1"
          )
        else
          @dynamo_db = Aws::DynamoDB::Client.new
        end
      end

      def table options = {}
        @table ||= options
      end

      def key k, type, range_key_name=nil, range_key_type=nil
        @key = [k, Dynamosaurus::DynamoBase::TYPES[type]]
        @key << range_key_name << Dynamosaurus::DynamoBase::TYPES[range_key_type] if range_key_name
      end

      def get_key
       @key
      end

      def has_renge
        @key.size == 4
      end

      def hash_key
        @key[0]
      end

      def range_key
        @key[2]
      end

      def key_schema
        key = [{
          :key_type => "HASH",
          :attribute_name => hash_key.to_s
        }]
        key << {
          :key_type => "RANGE",
          :attribute_name => range_key.to_s
        } if has_renge
        key
      end

      # schema
      def push_attribute_definitions name, type
        if @attribute_definitions.nil?
          @attribute_definitions = []
          @attribute_names = []
        end
        if @attribute_names.index(name).nil?
          @attribute_definitions << {:attribute_name => name, :attribute_type => type}
          @attribute_names << name
        end
      end
      def set_init_attribute_definitions
        push_attribute_definitions(hash_key.to_s,  get_key[1].to_s.upcase)
        push_attribute_definitions(range_key.to_s, get_key[3].to_s.upcase) if has_renge
      end
      def attribute_definitions
        @attribute_definitions
      end

      def local_secondary_schemas
        schema = []
        get_secondary_indexes.each do |index_key, index_value|
          schema << {
            index_name: index_key,
            key_schema: [],
            projection: {
              projection_type: "KEYS_ONLY",
            },
            key_schema: [
              {
                key_type: "HASH",
                attribute_name: index_value[0]
              },
              {
                key_type: "RANGE",
                attribute_name: index_value[2]
              }
            ]
          }
        end
        schema
      end

      def global_index_schemas
        @global_index_option = {} if @global_index_option.nil?
        schema = []
        get_global_indexes.each do |index|
          option = @global_index_option[index[0]] || {}

          schema << {
            :index_name => index[0],
            :key_schema => global_index_key_schema(index),
            :projection => {
              :projection_type => (option[:projection_type] || "KEYS_ONLY"),
            },
            :provisioned_throughput => {
              :read_capacity_units => 10,
              :write_capacity_units => 10
            }
          }
        end
        schema
      end

      def global_index_key_schema index
        key_schema = [{
          :key_type => "HASH",
          :attribute_name => index[1][0]
        }]
        key_schema << {
              :key_type => "RANGE",
              :attribute_name => index[1][2]
            } if index[1].size == 4
        key_schema
      end

      def local_secondary_attribute_definitions
        get_secondary_indexes.each do |index_key, index_value|
          push_attribute_definitions(index_value[2].to_s, index_value[3].to_s.upcase)
        end
      end

      def global_indexes_attribute_definitions
        get_global_indexes.each do |index|
          push_attribute_definitions(index[1][0].to_s, index[1][1].to_s.upcase)
          push_attribute_definitions(index[1][2].to_s, index[1][3].to_s.upcase) if index[1].size == 4
        end
      end

      def schema
        set_init_attribute_definitions

        @schema = {
          table_name: table_name,
          key_schema: key_schema,
          provisioned_throughput: {
            read_capacity_units: 10,
            write_capacity_units: 10
          }
        }

        unless get_global_indexes.empty?
          @schema[:global_secondary_indexes] = global_index_schemas
          global_indexes_attribute_definitions
        end
        unless get_secondary_indexes.empty?
          @schema[:local_secondary_indexes] = local_secondary_schemas
          local_secondary_attribute_definitions
        end
        @schema[:attribute_definitions] = attribute_definitions

        @schema
      end
      # end of schema

      # indexes
      def global_index index_name, key, key_type, range_key_name=nil, range_key_type=nil
        @global_index = {} if @global_index.nil?
        @global_index[index_name] = [key, Dynamosaurus::DynamoBase::TYPES[key_type]]
        @global_index[index_name] << range_key_name <<  Dynamosaurus::DynamoBase::TYPES[range_key_type] if range_key_name
      end

      def global_index_option index_name, option
        @global_index_option = {} if @global_index_option.nil?
        @global_index_option[index_name] = option
      end

      def get_global_indexes
        (@global_index) ? @global_index : {}

      end
      def get_global_index name
        @global_index[name]
      end

      def secondary_index index_name, range_key_name=nil, range_key_type=nil
        @secondary_index = {} if @secondary_index.nil?
        @secondary_index[index_name.to_sym] = [@key[0], @key[1], range_key_name, Dynamosaurus::DynamoBase::TYPES[range_key_type]] if range_key_name
      end

      def get_secondary_indexes
        @secondary_index ? @secondary_index : {}
      end
      def get_secondary_index name
        @secondary_index[name]
      end

      def get_indexes
        get_secondary_indexes.merge(get_global_indexes)
      end

      def get_index hash
        get_indexes.each{|key, value|
          if hash.size == 1 && hash.keys.first == value.first
            return {
              :index_name => key,
              :keys => [value[0]]
            }
          else hash.size == 2 && hash.keys.sort == [value[0], value[2]].sort
            return {
              :index_name => key,
              :keys => [value[0], value[2]]
            }
          end
        }
        nil
      end
      # end of indexes

      def table_prefix name
        @table_prefix = name
      end

      def res2hash hash
        new_hash = {}
        return new_hash if hash.nil?
        hash
      end

      # query
      def query_without_index value, option
        keys = {}

        value.each_with_index{|(k,v), i|
          keys[k] = {
            :comparison_operator => "EQ",
            :attribute_value_list => [v.to_s]
          }
        }
        Dynamosaurus.logger << "query index #{table_name} #{keys}"
        query keys, nil, option
      end

      def get_item_key value
        if value.is_a? Array
          {
            hash_key => value[0].to_s,
            range_key => value[1].to_s,
          }
        else
          {
            hash_key => value.to_s,
          }
        end
      end

      def get_from_key value, option={}
        return nil if value.nil?

        item_key = get_item_key(value)
        Dynamosaurus.logger << "get_item #{table_name} #{item_key}"

        res = dynamo_db.get_item(
          :table_name => table_name,
          :key => item_key
        )
        if res.item
          new :data => res.item
        else
          nil
        end
      end

      def get_from_index hash, option={}
        if index = get_index(hash)
          keys = {}

          index[:keys].each do |key|
            keys[key] = {
              :comparison_operator => "EQ",
              :attribute_value_list => [hash[key]]
            }
          end
          Dynamosaurus.logger << "query index #{table_name} #{keys}"
          query keys, index[:index_name], option
        end
      end

      def get_from_local_index hash, option={}
        if index = hash.delete(:index)
          keys = {}
          hash.each do |k, v|
            keys[k] = {
              :comparison_operator => "EQ",
              :attribute_value_list => [ v.to_s ]
            }
          end
          Dynamosaurus.logger << "query local_index #{table_name} #{keys}"
          query keys, index, option
        end
      end

      def query keys, index=nil, option={}
        query = {
          :table_name => table_name
        }
        unless keys.nil? || keys.empty?
          query[:key_conditions] = keys
        end

        if index
          query[:index_name] = index
        end

        query.merge!(option)
        res = dynamo_db.query(query)
        if query[:select] == "COUNT"
          res[:count]
        else
          if res.items
            return res.items.map{|item|
             new :data => item
            }
          end
        end
      end
      # end of query

      # public method
      def get value, option={}
        if value.is_a? Hash
          if value[:index]
            get_from_local_index value, option
          elsif option[:noindex]
            option.delete(:noindex)
            query_without_index value, option
          else
            get_from_index value, option
          end
        else
          get_from_key value, option
        end
      end

      def getOne value, option={}
        data = get(value, option)
        if data.nil? || data.empty?
          nil
        else
          data.first
        end
      end

      def get_orderd_key_from_hash hash
        get_orderd_key( (hash[hash_key.to_sym] || hash[hash_key.to_s]),
                       (hash[range_key.to_sym] || hash[range_key.to_s]))
      end

      def get_orderd_key_from_array array
        get_orderd_key(array[0], array[1])
      end

      def get_orderd_key value1, value2
        {
          hash_key.to_s => value1,
            range_key.to_s => value2
        }
      end

      def conv_key_array keys
        unless has_renge
          keys.map{|_key| {hash_key.to_s => _key.to_s } }
        else
          if keys.is_a?(Array)
            if keys[0].is_a?(Array)
              keys.map{|key| get_orderd_key_from_array(key)}
            elsif keys[0].is_a?(Hash)
              keys.map{|key| get_orderd_key_from_hash(key)}
            else
              [get_orderd_key_from_array(keys)]
            end
          else
            _keys = []
            ((p_key = keys[hash_key]).is_a?(Array) ? p_key : [p_key]).each do |key1|
              if (r_key = keys[range_key]).is_a?(Array)
                r_key.each do |key2|
                  _keys << get_orderd_key(key1, key2)
                end
              else
                _keys << get_orderd_key(key1, r_key)
              end
            end
            _keys
          end
        end
      end

      def batch_get_item keys
        return nil if keys.nil? or keys.empty?
        Dynamosaurus.logger << "batch_get_item #{table_name} #{keys}"
        _keys = conv_key_array(keys)

        res = dynamo_db.batch_get_item(
          :request_items => {
            table_name => {
              :keys => _keys
            }
          })

        if res.responses[table_name]
          return res.responses[table_name].map{|item|
            new :data => item
          }
        end
        nil
      end

      def first
        Dynamosaurus.logger << "first #{table_name}"
        res = dynamo_db.scan({
          :table_name => table_name,
          :limit => 1
        })
        if res.items && res.count > 0
          new :data => res.items[0]
        else
          nil
        end
      end

      def all
        Dynamosaurus.logger << "all #{table_name}"
        res = dynamo_db.scan({
          :table_name => table_name,
        })
        if res.items
          res.items.map{|item|
            new :data => item
          }
        end
      end

      def put hash, num_hash={}, return_values=nil
        new_hash = put_data(hash, num_hash)

        res = dynamo_db.put_item(
          :table_name => table_name,
          :item => new_hash,
          :return_values => return_values || "NONE"
        )
      end

      def put_data hash, num_hash={}
        new_hash = {}
        hash.each{|key, value|
          new_hash[key] = value unless value.nil?
        }
        num_hash.merge({:updated_at => Time.now.to_i}).each{|key, value|
          new_hash[key] = value.to_i unless value.nil?
        } if num_hash
        new_hash
      end

      def batch_write_item put_items = [], delete_items = []
        return nil if (put_items.nil? or put_items.empty?) and (delete_items.nil? or delete_items.empty?)
        Dynamosaurus.logger << "batch_write_item #{table_name}"

        requests = []
        if put_items and put_items.size > 0
          put_items.each do |item|
            if item["hash"] or item["num_hash"]
              new_item = put_data(item["hash"], item["num_hash"])
            else
              new_item = item.merge({:updated_at => Time.now.to_i})
            end
            requests << {
              put_request: {
                item: new_item
              }
            }
          end
        end

        if delete_items and delete_items.size > 0
          conv_key_array(delete_items).map{|key|
            requests << {
              delete_request: {
                key: key
              }
            }
          }
        end

        res = dynamo_db.batch_write_item(
          request_items: {
            table_name => requests
          },
          return_consumed_capacity: "TOTAL"
        )
        res
      end

      def save hash, num_hash={}, return_values=nil
        put(hash, num_hash, return_values)

        if has_renge
          get([hash[hash_key], hash[range_key]])
        else
          get(hash[hash_key])
        end
      end

      def add key=[], attribute_nums={}, options={}
        Dynamosaurus.logger << "update"

        attribute_updates = {}
        attribute_nums.each do |k, v|
          attribute_updates[k.to_s] = {
            :value => v,
            :action => "ADD"
          }
        end

        keys = {
          hash_key.to_sym => key.is_a?(Array) ? key[0] : key
        }
        keys[range_key.to_sym] = key[1]  if has_renge

        query ={
          :table_name => table_name,
          :key => keys,
          :attribute_updates => attribute_updates
        }
        query = query.merge(options)
        res = dynamo_db.update_item(query)

      end

      def delete_item value
        return nil if value.nil?

        old_item = dynamo_db.delete_item(
          :table_name => table_name,
          :key => get_item_key(value),
          :return_values => "ALL_OLD"
        )
        new :data => old_item.attributes
      end
    end
    # end of self class
  end
end
