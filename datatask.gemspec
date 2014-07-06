# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'data_task/version'

Gem::Specification.new do |spec|
  spec.name          = "data_task"
  spec.version       = Rake::DataTask::VERSION
  spec.authors       = ["Shahin Saneinejad"]
  spec.email         = ["shahin.saneinejad@gmail.com"]
  spec.summary       = %q{A Rake task for managing data across multiple datastores.}
  spec.description   = %q{DataTask provides dependency-based programming for data workflows on top of the Rake build tool.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.required_ruby_version  = '>= 1.9.3'

  spec.add_runtime_dependency 'rake', '~> 10.0.4'
  spec.add_runtime_dependency 'pg', '~> 0.17.1'
  spec.add_runtime_dependency 'sqlite3'

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'minitest-around', '~> 0.2'
  spec.add_development_dependency 'minitest-spec-context', '~> 0.0.3'
  spec.add_development_dependency 'coveralls'
end
