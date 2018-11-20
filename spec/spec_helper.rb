$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'dynamosaurus'
require 'testmodel'
require 'aws-sdk'

ENV['DYNAMODB_SUFFIX'] = "_local"

Aws.config = {
  :endpoint => "http://localhost:8000",
  :region => 'local_test',
  :access_key_id => "test",
  :secret_access_key => "test",
}
