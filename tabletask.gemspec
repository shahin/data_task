# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'table_task/version'

Gem::Specification.new do |spec|
  spec.name          = "tabletask"
  spec.version       = Rake::TableTask::VERSION
  spec.authors       = ["Shahin Saneinejad"]
  spec.email         = ["shahin.saneinejad@gmail.com"]
  spec.summary       = %q{A Rake task for managing tables in a datastore.}
  spec.description   = %q{Provides a Rake task for managing table builds, analogous to Rake's built-in FileTask.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'rake', '~> 10.0.4'
  spec.add_runtime_dependency 'pg', '~> 0.17.1'
  spec.add_runtime_dependency 'sqlite3'

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
end
