require 'rubygems'
require 'rspec/core/rake_task'
require 'rake/gempackagetask'

[:mysql, :postgresql].each do |adapter|
  desc "Run specs for #{adapter} adapter"
  RSpec::Core::RakeTask.new("spec:#{adapter.to_s}") do |t|
    t.rspec_opts = ["-c", "-f progress"]
    t.pattern = "spec/#{adapter}/**/*_spec.rb"
  end
end

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "spatial_adapter"
    gem.summary = "Spatial Adapter for ActiveRecord"
    gem.description = "Provides enhancements to ActiveRecord to handle spatial datatypes in PostgreSQL and MySQL."
    gem.authors = ["Pete Deffendol", "Guilhem Vellut"]
    gem.email = "pete@fragility.us"
    gem.homepage = "http://github.com/fragility/spatial_adapter"
    
    gem.files = FileList[
      "rails/*.rb",
      "lib/**/*.rb",
      "MIT-LICENSE",
      "README.rdoc",
      "VERSION"
    ]
    gem.test_files = FileList[
      "spec/**/*.rb",
      "spec/README.txt"
    ]
  
    gem.add_dependency 'activerecord', '>= 2.2.2'
    gem.add_dependency 'GeoRuby', '>= 1.3.0'
  end

  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "WARNING: Jeweler is not available for building packages. Install it with: gem install jeweler"
end

spec = Gem::Specification::load("spatial_adapter.gemspec")
Rake::GemPackageTask.new(spec) do |p|
  p.gem_spec = spec
  p.need_tar = true
  p.need_zip = true
end

task :deploy do
  gems = FileList['pkg/*.gem']
  FileUtils.cp gems, '/opt/gems/dev/gems'
  require 'rubygems'
  require 'rubygems/indexer'
  i=Gem::Indexer.new '/opt/gems/dev'
  i.generate_index
end
