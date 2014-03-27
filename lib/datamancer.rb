require 'datamancer/version'
require 'datamancer/datastream'
require 'datamancer/extract'
require 'datamancer/transform'
require 'datamancer/load'

module Datamancer
  class MissingField < StandardError; end
  class ExistingField < StandardError; end
  
  def keyvalue file
    YAML::load_file file
  end
end

# TODO: Decide on extract() default values issue; string?

# TODO: Case insensitive and regex field-mapping.

# TODO: Better errors and tests for them.

# TODO: ETL by batch.

# TODO: Methods alias.
# field => f
# unfield => u

# TODO: Field inclusion policy.
#
# OPTIONS
#
#   :include_all (DEFAULT)
#   :exclude_all
# 
# METHODS
#
#   unfield(name)
#   field(name, value = nil)
#
# CASES
#
#   unfield(name)      => include all, exclude 'name'
#   field(name)        => exclude all, include 'name'
#   field(name, value) => include 'name'
#
#   unfield(name1) and field(name2)        => include all, exclude 'name1', include 'name2'
#   unfield(name1) and field(name2, value) => include all, exclude 'name1', include 'name2'
#   field(name1) and field(name2, value)   => exclude all, include 'name1', include 'name2'
#
#   unfield(name1) and field(name2) and field(name2, value) => include all, exclude 'name1', include 'name2', include 'name3'
# 
# USAGE
#
#   Basically :include_all is passed explicity to be used along with field(name) (which otherwise would cause a
#   general exclusion of fields) for documentation purposes.
#
#   :exclude_all is useful for changing transform()'s default inclusive behaviour when using field(name, value) alone.
