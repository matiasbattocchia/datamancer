# Datamancer

**The Datamancer** is a form of sorcerer whose spells deal with data manipulation between databases.

Data targets (sources and destinations) can be **databases** supported by ActiveRecord and **CSV files**. Multiple targets can be present in a single ETL process.

Datamancer relies in bulk SQL reading and writing, and does not instantiate ActiveRecord objects, which is used for the sole purpose of connecting to databases.

## Installation

Add this line to your application's Gemfile:

    gem 'datamancer'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install datamancer

## Usage

*Please see the specs, for now.*

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
