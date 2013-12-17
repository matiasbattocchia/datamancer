require 'spec_helper'

describe Datamancer do

  context 'LOAD spell' do

    destinations = {'CSV file' => $dir + '/destination.csv',
                    'database' => {adapter: 'sqlite3', database: $dir + '/destination.sqlite3', table: 'users'}}
    
    destinations.each do |destination_type, destination|

      context "against a #{destination_type.upcase}" do

        before(:all) do
          @destination = destination
          @data = [{name: 'Foo', age: 27}, {name: 'Bar', age: 42}]
          
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
              
              # TODO: Fix this empty DB columns thing.
              
              [{name: 'Foo', age: nil}, {name: 'Bar', age: nil}]
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
              field :agE
            end

            # TODO: Check for table.

          }.to raise_error(MissingField,
            "Required field 'agE' was not found in '#{@destination}'")
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
