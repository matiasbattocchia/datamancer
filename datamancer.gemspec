# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'datamancer/version'

Gem::Specification.new do |spec|
  spec.name          = "datamancer"
  spec.version       = Datamancer::VERSION
  spec.authors       = ["Matías Battocchia"]
  spec.email         = ["matias@riseup.net"]
  spec.description   = %q{A magical extract, transform, load (ETL) library for data integration.}
  spec.summary       = %q{}
  spec.homepage      = "https://github.com/matiasbattocchia/datamancer"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "activerecord"
  spec.add_development_dependency "sqlite3"
  spec.add_development_dependency "activerecord-jdbcsqlite3-adapter"
end
