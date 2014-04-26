require 'datamancer/version'
require 'datamancer/datastream'
require 'datamancer/database'

module Datamancer
  class MissingField < StandardError; end
  class ExistingField < StandardError; end
  
  def keyvalue file
    YAML::load_file file
  end
end

# TODO: CSV default types.
# TODO: Case insensitive and regex field-mapping.
# TODO: ETL by batch.
