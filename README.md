# Datamancer

**The Datamancer** is a form of sorcerer whose spells deal with data manipulation between databases.

Sources and destinations can be **databases** supported by ActiveRecord and **CSV files**. Multiple sources and destinations can be present in a single ETL process.

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

### Datastreams

A **datastream** is a table with columns and rows made with an array of hashes.
Each element of the array (a hash) represents a row,
and each pair of a hash, an attribute (a column) and its value.

```ruby
people = [
  {name: 'Foo', age: 27},
  {name: 'Bar', age: 42}
]
```

### Extract

To get a datastream from a data source use the extract() method, like this:

```ruby
people = extract(from: 'people.csv')
```

If 'from' value is a string, extract will suppose that the source is a CSV file.
One option that can be used along this kind of source is 'separator', which value
will be used to interpret the file (defaults to comma).

Similarly, if 'from' attribute is a hash, it will be given to ActiveRecord to establish
a connection with the specified database and table. In case that the table is not indicated
there, the 'table' option can provide it.

Next, an example using a YAML file listing databases and showing further capacities of extract().

*databases.yml*

```yaml
warehouse:
  adapter: postgresql
  host: 192.168.0.1
  database: Warehouse
  username: foo
  password: 1234
```

```ruby
databases = YAML.load_file('databases.yml')

people = extract(from: databases['main'], table: 'people') do
  field :name
  field :age
end
```

extract() can take a block in order to manipulate data being extracted. All table or file columns
are brung by default; to get the opposite effect use 'exclude: true'.

Inside the block, field() is used to refer to columns. Without more arguments than columns' name
(as above) it performs no operation, but if columns are being excluded through the aforementioned
option, field() includes the referred columns. Both string and symbol are valid for column names.

To summarize, extract() options are:

* from: (hash or string)
* table: (string)
* exclude: (boolean)
* separator: (string)

And now we introduce field() options under extraction context:

* reject_if: (value or array of values) — Rejects row if condition is meet.
* reject_unless: (value or array of values) — Rejects row unless condition is meet.
* map: (string or symbol) — Alias column name in database.
* type: (class) — Casts data type on field.
* default: (value) — Set field to default value if nil.
* type_default: (class) — Casts data type even if field's value is nil. This way a nil
can be converted into a zero or an empty string.
* empty_default: (string) — Set field to default value if nil or empty.
* strip: (boolean) — Right and left strips the value to remove extra spaces in the string.

In addition to these options field() also takes blocks!

All together now:

```ruby
people = extract(from: databases['main'], table: 'people') do
  field 'name', map: 'Name', reject_if: nil
  field 'age', map: 'Age', type: Integer
end
```

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
