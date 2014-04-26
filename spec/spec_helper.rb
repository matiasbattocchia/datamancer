require 'datamancer'
require 'csv'
require 'active_record'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

if defined? JRUBY_VERSION
  require 'activerecord-jdbc-adapter'
else
  require 'sqlite3'
end

include Datamancer

$dir = __dir__ + '/data'
