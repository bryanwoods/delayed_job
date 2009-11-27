require 'rake'
require 'tasks/tasks'
require 'spec/rake/spectask'

task :default => :spec

Spec::Rake::SpecTask.new(:spec => ['spec:active_record', 'spec:mongo'])

namespace :spec do
  desc "Run specs for active_record adapter"
  Spec::Rake::SpecTask.new(:active_record) do |t|
    t.spec_files = FileList['spec/setup/active_record.rb', 'spec/*_spec.rb']
  end

  desc "Run specs for mongo_mapper adapter"
  Spec::Rake::SpecTask.new(:mongo) do |t|
    t.spec_files = FileList['spec/setup/mongo.rb', 'spec/*_spec.rb']
  end
end
