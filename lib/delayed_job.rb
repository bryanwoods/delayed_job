require File.dirname(__FILE__) + '/delayed/message_sending'
require File.dirname(__FILE__) + '/delayed/performable_method'

if defined?(MongoMapper)
  require File.dirname(__FILE__) + '/delayed/job/mongo_job'

  PERFORMABLE_METHOD_EXCEPTION = Exception
  PERFORMABLE_METHOD_STORE = MongoMapper::Document
else
  autoload :ActiveRecord, 'activerecord'
  require File.dirname(__FILE__) + '/delayed/job/active_record_job'

  PERFORMABLE_METHOD_EXCEPTION = ActiveRecord::RecordNotFound
  PERFORMABLE_METHOD_STORE = ActiveRecord::Base
end

require File.dirname(__FILE__) + '/delayed/job'
require File.dirname(__FILE__) + '/delayed/worker'

Object.send(:include, Delayed::MessageSending)   
Module.send(:include, Delayed::MessageSending::ClassMethods)  

if defined?(Merb::Plugins)
  Merb::Plugins.add_rakefiles File.dirname(__FILE__) / '..' / 'tasks' / 'tasks'
end
