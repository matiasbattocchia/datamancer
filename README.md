# Datamancer

**The Datamancer** is a form of sorcerer whose spells deal with data manipulation between databases.

Data targets (sources and destinations) can be **databases** supported by ActiveRecord and **CSV files**. Multiple targets can be present in a single ETL process.

To optimize, Datamancer relies in bulk SQL reading and writing, and does not instantiate ActiveRecord objects, which is used for the sole purpose of connecting to databases.

It is tested to run over JRuby as well.

## Installation

Add this line to your application's Gemfile:

    gem 'datamancer'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install datamancer

## Usage

### Extract

Method options:

* from
* table
* exclude
* separator

Field options:

* reject_if
* reject_unless
* map
* type
* type_default
* empty_default
* strip

### Transform

Method options:

* exclude
* join
* on
* unique

Field options: Not allowed.

### Load

Method options:

* to
* table
* exclude
* append
* batch

Field options:

* map

## Example

```ruby
require 'bundler/setup'
require 'datamancer'
require 'active_record'
require 'csv'

include Datamancer

bases = YAML.load_file('/home/matias/proyectos/panel/bases_de_datos.yml')

países_ISO =
extract from: 'country-list/country/cldr/es_AR/country.csv' do
  field :iso
  field :nombre, map: 'name'
end

países_UN =
extract from: 'countries/countries.csv', separator: ';', exclude: true do
  field :iso, map: 'cca2'
  field :número, map: 'ccn3', type: Integer
end

países =
transform países_ISO, join: países_UN, on: :iso

load países, to: bases['panel'], table: 'lk_com_pais', append: true do
  field :número, map: 'id_com_pais'
  field :iso, map: 'cd_com_pais'
  field :nombre, map: 'ds_com_pais'
end
```

## Future features

* Batch mode
* Error monitor
* Control files

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
