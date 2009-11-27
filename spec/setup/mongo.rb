$:.unshift(File.dirname(__FILE__) + '/../../lib')
$:.unshift(File.dirname(__FILE__) + '/../../../rspec/lib')

require 'rubygems'        # not required for Ruby 1.9
require 'mongo_mapper'

require File.dirname(__FILE__) + '/../../init'

MongoMapper.connection = Mongo::Connection.new('127.0.0.1', 27017, {
  :logger => Logger.new('/tmp/dj.log')
})
MongoMapper.database = 'test'

# Purely useful for test cases...
class Story
  include MongoMapper::Document

  def tell; text; end       
  def whatever(n, _); tell*n; end
  
  handle_asynchronously :whatever
end
