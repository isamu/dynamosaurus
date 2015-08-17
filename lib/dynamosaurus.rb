require "dynamosaurus/version"
require "dynamosaurus/dynamo_base"
require "dynamosaurus/dynamo_class"
require "dynamosaurus/logger"

module Dynamosaurus
  class << self
    attr_accessor :logger
  end
end
