require "dynamosaurus/version"
require "dynamosaurus/dynamo_base"
require "dynamosaurus/dynamo_class"
require "dynamosaurus/logger"
require "deep_merge"

module Dynamosaurus
  class << self
    attr_accessor :logger
  end

  class BlackHole < BasicObject
    def method_missing(method_name, *args)
      nil
    end
  end
end

Dynamosaurus.logger =  Dynamosaurus::BlackHole.new
