require 'spec_helper'

describe Datamancer do
  
  context 'EXTRACT spell' do

    sources = {'CSV file' => $dir + '/source.csv',
               'database' => {adapter: 'sqlite3', database: $dir + '/source.sqlite3', table: 'users'}}
    
    sources.each do |source_type, source|

      context "against a #{source_type.upcase}" do

        before(:all) do
          @source = source
          @data = [{name: 'Foo', age: 27}, {name: 'Bar', age: 42}]
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

          # TODO: Better explanation for this error.

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

          ).to eq([{name: 'Foo', years_old: 27}, {name: 'Bar', years_old: 42}])
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

    # TODO: Validations

    # exclusion
    # inclusion
    # format
    # presence
    # uniqueness (?)
    # length (?)
    # numericality (?)
    # custom validations (?)

    # When a field validation fails, the row gets dropped.
    # This action is logged and optionally the process halted.
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
      @yml_file = $dir + '/simple.yml'
      expect(keyvalue(@yml_file)).to eq({'Argentina' => '+54', 'Brazil' => '+55'})
    end

  end
end
