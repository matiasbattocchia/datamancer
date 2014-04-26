require 'spec_helper'

describe Datamancer do

  context 'TRANSFORMATION spell' do
    
    # before(:all) do
    #   csv_file = $dir + '/source.csv'
    #   @data = extract from: csv_file
    # end
 
    
    # it 'passes fields implicitly' do
    #   expect(
    #     transform(@data)
    #   ).to eq(@data)

    #   expect(
    #     transform(@data) do
    #       field :name
    #     end
    #   ).to eq(@data)
    # end

    
    # it 'passes fields explicitly' do

    #   # TODO: What happens when no field is passed?
      
    #   expect(

    #     transform(@data, exclude: true) do
    #       field :name
    #     end

    #   ).to eq([{name: 'Foo'}, {name: 'Bar'}])
    # end

    
    # it 'filters fields explicitly' do

    #   # TODO: What happens when all fields are deleted?
      
    #   expect(

    #     transform(@data) do
    #       del_field :age
    #     end

    #   ).to eq([{name: 'Foo'}, {name: 'Bar'}])
    # end
 
    
    # it 'raises an exception if a field is missing' do
    #   expect {

    #     transform(@data) do
    #       field :agE
    #     end

    #   }.to raise_error(MissingField,
    #     "Required field 'agE' was not found")

    #   expect {

    #     transform(@data) do
    #       del_field :agE
    #     end

    #   }.to raise_error(MissingField,
    #     "Filtered field 'agE' was not found")
    # end

    
    # it 'changes fields through *true* expressions' do
    #   expect(

    #     transform(@data) do
    #       field :name, false
    #       field :age, true
    #     end

    #   ).to eq([{name: 'Foo', age: true}, {name: 'Bar', age: true}])
    # end


    # it 'creates fields' do
    #   expect(
    #     transform(@data) do
    #       new_field :new, true
    #     end
    #   ).to eq([{name: 'Foo', age: '27', new: true}, {name: 'Bar', age: '42', new: true}])

    #   # TODO: A better explanation for this error.
      
    #   expect{
    #     transform(@data) do
    #       new_field :new
    #     end
    #   }.to raise_error(ArgumentError)
      
    #   expect {
    #     transform(@data) do
    #       new_field :name, true
    #     end
    #   }.to raise_error(ExistingField,
    #       "New field 'name' already exists")
    # end

    
    # it 'lets field names to be strings and allows field methods to co-occur' do
    #   expect(

    #     transform(@data) do
    #       field 'name'
    #       del_field 'age'
    #       new_field 'new', true
    #     end

    #   ).to eq([{name: 'Foo', new: true}, {name: 'Bar', new: true}])
    # end

    
    # it 'changes fields sending messages as symbols' do
    #   expect(
    #     transform(@data) do
    #       field :name, :slice, 0
    #       field :age, :to_i
    #     end
    #   ).to eq([{name: 'F', age: 27}, {name: 'B', age: 42}])
      
    #   expect(
    #     transform(@data) do
    #       field :name, 'slice', 0
    #       field :age, 'to_i'
    #     end
    #   ).to eq([{name: 'slice', age: 'to_i'}, {name: 'slice', age: 'to_i'}])
    # end


    # it 'generates field getters per row' do
    #   expect(

    #     transform(@data) do
    #       new_field :namage, name.downcase + age
    #     end

    #   ).to eq([{name: 'Foo', age: '27', namage: 'foo27'}, {name: 'Bar', age: '42', namage: 'bar42'}])
    # end

#############################################################################

    it 'drops duplicated rows' do
      duplicated_data = @data + @data
      
      expect(
        transform(duplicated_data, unique: :name)
      ).to eq(@data)
      
      expect(
        transform(duplicated_data, unique: :name) do
          field :name
        end
      ).to eq(@data)
    end


    context 'combines records by' do

      before(:all) do
        @left_data =
        [{name: 'Foo', some_id: 1},
         {name: 'Bar', some_id: 2},
         {name: 'Baz', some_id: 2},
         {name: 'Foobar', some_id: nil}]

        @right_data =
        [{age: 0, some_id: nil},
         {age: 27, some_id: 1},
         {age: 33, some_id: 1},
         {age: 42, some_id: 2}]
      end


      it 'inner join' do

        # TODO: A better explanation for this error.

        expect {
          transform(@left_data, join: @right_data)
        }.to raise_error(ArgumentError)
        
        expect {
          transform(@left_data, join: @right_data, on: 'some_ID')
        }.to raise_error(ArgumentError)

        expect(
          transform(@left_data, join: @right_data, on: 'some_id') do
            del_field :some_id
            new_field :namage, name.downcase + age.to_s
          end
        ).to eq([{name: 'Foo', age: 27, namage: 'foo27'},
                 {name: 'Foo', age: 33, namage: 'foo33'},
                 {name: 'Bar', age: 42, namage: 'bar42'},
                 {name: 'Baz', age: 42, namage: 'baz42'}])
      end


      it 'left outer join'
      it 'right outer join'
      it 'full outer join'

    end
  end


  context 'aggregation' do

    before(:all) do
      csv_file = $dir + '/source2.csv'
      @data = extract from: csv_file do
        field :store
        field :coffee_cups, type: Integer
        field :name
        field :croissants, type: Integer
      end
    end

    it 'projects data to dimensions summing facts' do

      # TODO: Facts must be additionable. Maybe raise an error?
      # TODO: What happens with bad formatted data?

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
end
