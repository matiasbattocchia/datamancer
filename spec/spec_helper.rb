require 'datamancer'
require 'csv'
require 'active_record'

if defined? JRUBY_VERSION
  require 'activerecord-jdbc-adapter'
else
  require 'sqlite3'
end

include Datamancer

$dir = __dir__ + '/data'
