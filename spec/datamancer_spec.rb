require 'datamancer'
require 'csv'
require 'active_record'

if defined? JRUBY_VERSION
  require 'activerecord-jdbc-adapter'
else
  require 'sqlite3'
end

include Datamancer

describe Datamancer do

##############
# Extraction #
##############

  # IMPORTANT: Field values default to String!
  
  context 'EXTRACT spell' do

    {'CSV file' => __dir__ + '/source.csv',
     'database' => {adapter: 'sqlite3', database: __dir__ + '/source.sqlite3', table: 'users'}
    }.each do |source_type, source|

      context "against a #{source_type.upcase}" do

        before(:all) do
          @source = source
          @data = [{name: 'Foo', age: '27'}, {name: 'Bar', age: '42'}]
        end

        after(:all) do
          ActiveRecord::Base.connection.close
        end if source_type == 'database'

        it 'reads data from source implicitly' do
          expect(
            extract(from: @source)
          ).to eq(@data)

          expect(
            extract(from: @source) do
              field :name
            end
          ).to eq(@data)

          expect(
            extract(from: @source) do
              field :name
              field :age
            end
          ).to eq(@data)
        end

        it 'reads data from source explicitly' do
          expect(

            extract(from: @source, exclude: true) do
              field :name
            end

          ).to eq([{name: 'Foo'}, {name: 'Bar'}])
        end

        it 'raises an exception if data source is missing' do
          expect {

            extract(@source)

          }.to raise_error(ArgumentError, 'Extract requires a source, i.e. extract(from: source)')
        end

        it 'raises an exception if a required field is missing' do

          # source = case source_type
          # when 'CSV file' then "#{@source} file"
          # when 'database' then "#{@source[:database]}.#{@source[:table]} table"
          # end

          expect {

            extract(from: @source) do
              field :surname
            end

          }.to raise_error(MissingField,
            "Required field 'surname' was not found in '#{@source}'")
        end

        it 'lets fields to be strings' do
          expect(

            extract(from: @source) do
              field 'name'
              field 'age'
            end

          ).to eq(@data)
        end

        it 'maps fields' do
          expect(

            extract(from: @source) do
              field :years_old, map: 'age'
            end

          ).to eq([{name: 'Foo', years_old: '27'}, {name: 'Bar', years_old: '42'}])
        end

        it 'casts types on fields' do
          expect(

            extract(from: @source) do
              field :age, type: Integer
            end

          ).to eq([{name: 'Foo', age: 27}, {name: 'Bar', age: 42}])
        end
      end
    end
    ## Validations ##
    
    # exclusion
    # inclusion
    # format
    # presence
    # uniqueness (?)
    # length (?)
    # numericality (?)
    # custom validations (?)

    # When a field validation fails, the row gets dropped.
    # This action is logged and optionally the process can be halted.
    # Validations should accept options (:allow_nil, :if, etc).

    # it 'validates fields' do
      
    #   pending 'Not now...'

    #   expect(

    #     extract(from: @csv_file) do
    #       field :name
    #       field :age, reject_if: '27'
    #     end

    #   ).to eq([{name: 'Bar', age: '42'}])
    # end
    
    it 'imports YAML key-value tables' do
      @yml_file = __dir__ + '/simple.yml'
      expect(keyvalue(@yml_file)).to eq({'Argentina' => '+54', 'Brazil' => '+55'})
    end
  end

##################
# Transformation #
##################

  context 'TRANSFORMATION spell' do
    
    before(:all) do
      csv_file = __dir__ + '/source.csv'
      @data = extract from: csv_file
    end
 
    it 'passes fields implicitly' do
      expect(
        transform(@data)
      ).to eq(@data)

      expect(
        transform(@data) do
          field :name
        end
      ).to eq(@data)

      expect(
        transform(@data) do
          field :name
          field :age
        end
      ).to eq(@data)
    end

    it 'passes fields explicitly' do
# TODO: What happens when any field is passed?
      expect(

        transform(@data, exclude: true) do
          field :name
        end

      ).to eq([{name: 'Foo'}, {name: 'Bar'}])
    end

    it 'filters fields explicitly' do
# TODO: What happens when all fields are deleted?
      expect(

        transform(@data) do
          del_field :age
        end

      ).to eq([{name: 'Foo'}, {name: 'Bar'}])
    end
 
    it 'raises an exception if a field is missing' do
      expect {

        transform(@data) do
          field :surname
        end

      }.to raise_error(MissingField,
        "Required field 'surname' was not found")

      expect {

        transform(@data) do
          del_field :surname
        end

      }.to raise_error(MissingField,
        "Filtered field 'surname' was not found")
    end

    it 'changes fields (through expressions)' do
      expect(

        transform(@data) do
          field :age, true
        end

      ).to eq([{name: 'Foo', age: true}, {name: 'Bar', age: true}])
    end

    it 'creates fields' do
      expect(
        transform(@data) do
          new_field :new, true
        end
      ).to eq([{name: 'Foo', age: '27', new: true}, {name: 'Bar', age: '42', new: true}])

      expect{
        transform(@data) do
          new_field :new
        end
      }.to raise_error(ArgumentError)
      
      expect {
        transform(@data) do
          new_field :name, true
        end
      }.to raise_error(ExistingField,
          "New field 'name' already exists")
    end

    it 'lets field names to be strings and allows method interactions' do
      expect(

        transform(@data) do
          field 'name'
          del_field 'age'
          new_field 'new', true
        end

      ).to eq([{name: 'Foo', new: true}, {name: 'Bar', new: true}])
    end

    it 'generates field getters per row' do
      expect(

        transform(@data) do
          field :name, age
          field :age, name
          #field :namage, name.downcase + age.to_s
        end

      ).to eq([{name: '27', age: 'Foo'}, {name: '42', age: 'Bar'}])
    end
  end

  context 'aggregation' do

    before(:all) do
      csv_file = __dir__ + '/source2.csv'
      @data = extract from: csv_file do
        field :store
        field :coffee_cups, type: Integer
        field :name
        field :croissants, type: Integer
      end
    end

# IMPORTANT: Facts must be additionable. Maybe raise an error?
# WARNING: What happens with bad formatted data?
    it 'projects data to dimensions summing facts' do

      expect(

        aggregate(@data) do
          dim  :name
          fact :coffee_cups
          fact :croissants
        end

      ).to eq([{name: 'Foo', coffee_cups: 17, croissants: 14},
               {name: 'Bar', coffee_cups:  8, croissants:  6}])
    end
    
    it 'lets fields to be strings' do

      expect(

        aggregate(@data) do
          dim  'name'
          fact 'coffee_cups'
        end

      ).to eq([{name: 'Foo', coffee_cups: 17},
               {name: 'Bar', coffee_cups:  8}])
    end
  end

########
# Load #
########

  context 'LOAD spell' do

    {'CSV file' => __dir__ + '/destination.csv',
     'database' => {adapter: 'sqlite3', database: __dir__ + '/destination.sqlite3', table: 'users'}
    }.each do |destination_type, destination|

      context "against a #{destination_type.upcase}" do

        before(:all) do
          @destination = destination
          @data = [{name: 'Foo', age: '27'}, {name: 'Bar', age: '42'}]
          
          if destination_type == 'database'
            ActiveRecord::Base.establish_connection(destination)
            ActiveRecord::Base.connection.delete('DELETE FROM users')
          end
        end

        after(:all) do
          ActiveRecord::Base.connection.close
        end if destination_type == 'database'
        
        it 'writes data to destination implicitly' do
          load(@data, to: @destination, append: false)

          expect(extract from: @destination).to eq(@data)

          load(@data, to: @destination, append: false) do
            field :name
          end

          expect(extract from: @destination).to eq(@data)

          load(@data, to: @destination, append: false) do
            field :name
            field :age
          end

          expect(extract from: @destination).to eq(@data)
        end

        it 'appends data to destination'

        it 'writes data to destination explicitly' do
          load(@data, to: @destination, append: false, exclude: true) do
            field :name
          end

          expect(extract from: @destination).to eq(
            case destination_type
            when 'CSV file'
              [{name: 'Foo'}, {name: 'Bar'}]
            when 'database'
              [{name: 'Foo', age: ''}, {name: 'Bar', age: ''}]
            end)
        end

        it 'raises an exception if data destination is missing' do
          expect {

            load(@data, @destination)

          }.to raise_error(ArgumentError, 'Load requires a destination, i.e. load(data, to: destination)')
        end
        
        it 'raises an exception if a required field is missing' do
          expect {

            load(@data, to: @destination) do
              field :surname
            end

          }.to raise_error(MissingField,
            "Required field 'surname' was not found in '#{@destination}'")
        end

        it 'lets fields to be strings' do
          load(@data, to: @destination, append: false) do
            field 'name'
            field 'age'
          end

          expect(extract from: @destination).to eq(@data)
        end

        it 'maps fields' do
          data = [{name: 'Foo', years_old: '27'}, {name: 'Bar', years_old: '42'}]

          load(data, to: @destination, append: false) do
            field :years_old, map: 'age'
          end

          expect(extract from: @destination).to eq(@data)
        end

      end
    end
  end
end
