require "aws-sdk-core"

module Dynamosaurus
  class DynamoBase
    TYPES = {
      :string => :s,
      :number => :n
    }

    def dynamo_db
      self.class.dynamo_db
    end

    def initialize params
      data = params[:data] if params[:data]
      @data = Dynamosaurus::DynamoBase.res2hash(data)
    end

    def table_name
      self.class.table_name
    end

    def [] key
      @data[key]
    end

    def []=(key,value)
      @data[key] = value
    end

    def data
      @data
    end

    def exist?
      ! @data.empty?
    end

    def empty?
      @data.empty?
    end

    def try name
      @data[name.to_s]
    end

    def method_missing(name, *params)
      name = name.to_s[0..-2] if name.to_s[-1] == "="
      if exist? and @data.has_key?(name.to_s)
        if params.empty?
          @data[name.to_s]
        else
          @data[name.to_s] = params[0]
        end
      else
        super
      end
    end

    def key
      return @key if @key
      @key = self.class.get_key
    end

    def keys
      item_key = {}
      item_key[key[0].to_sym] = @data[key[0].to_s]
      item_key[key[2].to_sym] = @data[key[2].to_s] if key.size == 4
      item_key
    end

    def update attributes={}, attribute_nums={}, options={}
      Dynamosaurus.logger << "update"

      attribute_updates = {}
      attributes.each do |k, v|
        @data[k.to_s] = v
        attribute_updates[k.to_s] = {
          :value => v,
          :action => "PUT"
        }
      end

      attribute_nums.each do |k, v|
        @data[k.to_s] = v.to_i
        attribute_updates[k.to_s] = {
          :value => v.to_i,
          :action => "PUT"
        }
      end

      query = {
        :table_name => self.class.table_name,
        :key => keys,
        :attribute_updates => attribute_updates
      }.merge(options)
      res = dynamo_db.update_item(query)
    end

    def add attribute_nums={}, options={}
      if self.class.has_renge
        key_value = [@data[key[0].to_s], @data[key[2].to_s]]
      else
        key_value = @data[key[0].to_s]
      end
      attribute_nums.each do |k, v|
        @data[k.to_s] = @data[k.to_s] + v.to_i
      end
      self.class.add(key_value, attribute_nums, options)
    end

    def save
      attribute_updates = {}
      @data.each do |k, v|
        if key.index(k.to_sym).nil?
          attribute_updates[k.to_s] = {
            :value => v,
            :action => "PUT"
          }
        end
      end

      query = {
        :table_name => self.class.table_name,
        :key => keys,
        :attribute_updates => attribute_updates
      }
      res = dynamo_db.update_item(query)
    end

    def attr_delete attributes=[]
      Dynamosaurus.logger << "delete"

      attribute_updates = {}
      attributes.each do |k|
        attribute_updates[k.to_s] = {
          :action => "DELETE"
        }
        @data.delete(k.to_s)
      end

      res = dynamo_db.update_item(
        :table_name => self.class.table_name,
        :key => keys,
        :attribute_updates => attribute_updates
      )
    end

    def delete
      dynamo_db.delete_item(
        :table_name => self.class.table_name,
        :key => keys
      )
    end
  end
end
