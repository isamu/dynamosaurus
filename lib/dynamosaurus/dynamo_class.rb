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

      def key k, type, range_key=nil, range_key_type=nil
        @key = [k, Dynamosaurus::DynamoBase::TYPES[type]]
        @key << range_key << Dynamosaurus::DynamoBase::TYPES[range_key_type] if range_key
      end

      def get_key
       @key
      end

      def schema
        @schema = {}
        @schema[:table_name] = table_name
        @schema[:key_schema] = []
        @schema[:attribute_definitions] = []

        @schema[:provisioned_throughput] = {
          :read_capacity_units => 10,
          :write_capacity_units => 10
        }

        @schema[:key_schema]  << {
          :key_type => "HASH",
          :attribute_name => get_key[0].to_s
        }
        @schema[:attribute_definitions] << {:attribute_name => get_key[0].to_s, :attribute_type => get_key[1].to_s.upcase}

        if get_key.size == 4
          @schema[:key_schema]  << {
            :key_type => "RANGE",
            :attribute_name => get_key[2].to_s
          }
          @schema[:attribute_definitions] << {:attribute_name => get_key[2].to_s, :attribute_type => get_key[3].to_s.upcase}
        end

        unless get_global_indexes.empty?
          @schema[:global_secondary_indexes] = []
          get_global_indexes.each do |g_index|
            index_schema = {
              :index_name => g_index[0],
              :key_schema => [],
              :projection => {
                :projection_type => "KEYS_ONLY",
              },
              :provisioned_throughput => {
                :read_capacity_units => 10,
                :write_capacity_units => 10
              },
            }
            index_schema[:key_schema] << {
              :key_type => "HASH",
              :attribute_name => g_index[1][0]
            }
            @schema[:attribute_definitions] << {:attribute_name => g_index[1][0].to_s, :attribute_type => g_index[1][1].to_s.upcase}
            if g_index[1].size == 4
              index_schema[:key_schema] << {
                :key_type => "RANGE",
                :attribute_name => g_index[1][2]
              }
              @schema[:attribute_definitions] << {:attribute_name => g_index[1][2].to_s, :attribute_type => g_index[1][3].to_s.upcase}
            end

            @schema[:global_secondary_indexes] << index_schema
          end
        end
        unless get_secondary_indexes.empty?
          @schema[:local_secondary_indexes] = []
          get_secondary_indexes.each do |s_index_key, s_index_value|
            index_schema = {
              :index_name => s_index_key,
              :key_schema => [],
              :projection => {
                :projection_type => "KEYS_ONLY",
              },
            }
            index_schema[:key_schema] =[
              {
                :key_type => "HASH",
                :attribute_name => s_index_value[0]
              },
              {
                :key_type => "RANGE",
                :attribute_name => s_index_value[2]
              }
            ]
            @schema[:attribute_definitions] << {:attribute_name => s_index_value[2].to_s, :attribute_type => s_index_value[3].to_s.upcase}
            @schema[:local_secondary_indexes] << index_schema
          end
        end

        @schema
      end

      def global_index index_name, key, key_type, range_key=nil, range_key_type=nil
        @global_index = {} if @global_index.nil?
        @global_index[index_name] = [key, Dynamosaurus::DynamoBase::TYPES[key_type]]
        @global_index[index_name] << range_key <<  Dynamosaurus::DynamoBase::TYPES[range_key_type] if range_key
      end

      def get_global_indexes
        (@global_index) ? @global_index : {}

      end
      def get_global_index name
        @global_index[name]
      end

      def secondary_index index_name, range_key=nil, range_key_type=nil
        @secondary_index = {} if @secondary_index.nil?
        @secondary_index[index_name.to_sym] = [@key[0], @key[1], range_key, Dynamosaurus::DynamoBase::TYPES[range_key_type]] if range_key
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

      def table_prefix name
        @table_prefix = name
      end

      def key_list
        keys = get_key + get_secondary_indexes.values.flatten
        list = {}
        until keys.empty?
          convi = keys.shift(2)
          list[convi[0]] = convi[1]
        end
        list
      end

      def res2hash hash
        new_hash = {}
        return new_hash if hash.nil?
        hash
      end


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
            get_key[0] => value[0].to_s,
            get_key[2] => value[1].to_s,
          }
        else
          {
            get_key[0] => value.to_s,
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
          :table_name => table_name,
          :key_conditions => keys,
        }
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

      def conv_key_array keys
        if get_key.size == 2
          keys.map{|_key| {get_key[0].to_s => _key.to_s } }
        else
          _keys = []
          keys[get_key[0]].each do |key1|
            keys[get_key[2]].each do |key2|
              _keys << {
                get_key[0].to_s => key1,
                get_key[2].to_s => key2,
              }
            end
          end
          _keys
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
        my_keys = get_key

        if my_keys.size == 4
          get([hash[my_keys[0]], hash[my_keys[2]]])
        else
          get(hash[my_keys[0]])
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

        class_key = get_key
        keys = {
          class_key[0].to_sym => key.is_a?(Array) ? key[0] : key
        }
        keys[class_key[2].to_sym] = key[1]  if class_key.size > 2

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
