require 'spec_helper'

describe Datastream do

  # describe '::new' do
  #   it 'creates an empty datastream' do
  #     ds = Datastream.new
  #     expect(ds).to be_an_instance_of Datastream
  #   end

  #   it 'creates a datastream from an array' do
  #     ds = Datastream.new []
  #     expect(ds).to be_an_instance_of Datastream
  #   end

  #   it 'throws an exception ...' do
  #     expect{ Datastream.new "" }.to 
  #   end
  # end
  it { should respond_to(:data) }
  # it { is_expected.to respond_to(:headers) }
  # it { is_expected.to respond_to(:data_types) }

  # map, pop, shift, unshift, all, first, last, length/count, empty?

  # En el bloque:
  # valores por defecto/vacío
  # casteo
  # mapeo
  # strip
  # bloques

  # describe '#raw'

  # describe '#push'
  # describe '#<<'

  describe '#where'

  # describe '#order'
  # describe '#sort'
  # describe '#reverse_order'
  # describe '#reverse_sort'

  # describe '#left_join'
  # describe '#right_join'
  # describe '#outer_join'

  describe '#union'
  describe '#|'
  describe '#union_all'
  describe '#+'

  describe '#except'
  describe '#except_all'
  describe '#-'

  describe '#intersect'
  describe '#&'
  describe '#intersect_all'

  describe '#distinct'
  describe '#uniq'
  describe '#unique'


  before(:each) do
    @ds = Datastream.new [{name: 'Foo', email: 'foo@mail'},
                          {name: 'Bar', email: 'bar@mail'}]
  end

  describe '#transform' do
    it 'returns a datastream' do
      expect(@ds.transform).to be_a Datastream
    end

    it 'returns a new datastream' do
      expect(@ds.transform.object_id).not_to eq(@ds.object_id)
    end

    it 'does not modifies itself' do
      
      @ds.transform do
        update :name, nil
      end
      
      expect(@ds.data).to eq(
        [{name: 'Foo', email: 'foo@mail'},
         {name: 'Bar', email: 'bar@mail'}])
    end
    
    it 'modifies the datastream' do
      
      ds = @ds.transform do
        update :name, nil
      end
      
      expect(ds.data).to eq(
        [{name: nil, email: 'foo@mail'},
         {name: nil, email: 'bar@mail'}])
    end
  end

  describe '#transform!' do
    # TODO: row number, count, create row, delete row, readonly_row access.

    it 'returns a datastream' do
      expect(@ds.transform!).to be_a Datastream
    end

    it 'returns itself' do
      expect(@ds.transform!.object_id).to eq(@ds.object_id)
    end

    describe '{ create }' do
      it 'adds a column to the datastream' do

        @ds.transform! do
          create :admin, false
          create 'mod', true
        end

        expect(@ds.data).to eq(
          [{name: 'Foo', email: 'foo@mail', admin: false, mod: true},
           {name: 'Bar', email: 'bar@mail', admin: false, mod: true}])
      end

      it 'raises an exception if the column already exists' do
        expect {

          @ds.transform! do
            create :name, true
          end

        }.to raise_error(ExistingColumn,
          "Column 'name' already exists")
      end
    end

    describe '{ read }' do
      it "returns current row's column value" do
        values = []

        @ds.transform! do
          values << read(:name) + ' email is ' + read('email')
        end

        expect(values).to eq(['Foo email is foo@mail', 'Bar email is bar@mail'])
      end
    
      it 'raises an exception if the column is missing' do
        expect {

          @ds.transform! do
            read :NAME
          end

        }.to raise_error(MissingColumn,
          "Column 'NAME' was not found")
      end
    end

    describe '{ [column_name] }' do

      # See http://www.dan-manges.com/blog/ruby-dsls-instance-eval-with-delegation.
      
      # TODO: Beware of column names that do not follow method naming conventions.

      it "returns current row's column value" do
        values = []

        @ds.transform! do
          values << name
        end

        expect(values).to eq(['Foo', 'Bar'])
      end

      it 'delegates other than column names references to the block-declaring context' do
        def some_method; end

        expect {

          @ds.transform! do
            some_method
          end

        }.not_to raise_error
      end
    end

    describe '{ update }' do
      it "writes a value in current row's column" do

        @ds.transform! do
          update :name, nil
          update 'email', nil
        end

        expect(@ds.data).to eq(
          [{name: nil, email: nil},
           {name: nil, email: nil}])
      end
      
      it 'does not overwrite { read }' do
        values = []

        @ds.transform! do
          update :name, nil
          
          values << read(:name)
        end

        expect(@ds.data).to eq(
          [{name: nil, email: 'foo@mail'},
           {name: nil, email: 'bar@mail'}])
        
        expect(values).to eq(['Foo', 'Bar'])
      end
      
      it 'does not overwrite { [column_name] }' do
        values = []

        @ds.transform! do
          update :name, nil
          
          values << name
        end

        expect(values).to eq(['Foo', 'Bar'])
      end
      
      it 'raises an exception if a column is missing' do
        expect {

          @ds.transform! do
            update :EMAIL, nil
          end

        }.to raise_error(MissingColumn,
          "Column 'EMAIL' was not found")
      end
    end

    describe '{ delete }' do
      it 'removes a column from the datastream' do

        @ds.transform! do
          delete :name
          delete 'email'
        end

        expect(@ds.data).to eq([{}, {}])
      end
    
      it 'raises an exception if a column is missing' do
        expect {

          @ds.transform! do
            delete :NAME
          end

        }.to raise_error(MissingColumn,
          "Column 'NAME' was not found")
      end
    end

    describe '{ rename }' do
      it 'renames a column' do

        @ds.transform! do
          rename :name, :nombre
          rename 'email', 'correo'
        end

        expect(@ds.data).to eq(
          [{nombre: 'Foo', correo: 'foo@mail'},
           {nombre: 'Bar', correo: 'bar@mail'}])
      end

      it 'raises an exception if the to be renamed column is missing' do
        expect {

          @ds.transform! do
            rename :NAME, :nombre
          end

        }.to raise_error(MissingColumn,
          "Column 'NAME' was not found")
      end

      it 'raises an exception if the new column name already exists' do
        expect {

          @ds.transform! do
            rename :name, :email
          end

        }.to raise_error(ExistingColumn,
          "Column 'email' already exists")
      end
    end

    describe '{ _ }'

    describe '{ row }' do
      it 'returns current row' do
        values = []

        @ds.transform! do
          values << row
        end

        expect(values).to eq(
          [{name: 'Foo', email: 'foo@mail'},
           {name: 'Bar', email: 'bar@mail'}])
      end
    end

    # describe '{ row_number }' do
    #   it "returns current row's index" do
    #     numbers = []

    #     @ds.transform! do
    #       numbers << row_number
    #     end

    #     expect(numbers).to eq([0, 1])
    #   end
    # end
  end

  describe '#select' do
    it 'returns a datastream' do
      expect(@ds.select).to be_a Datastream
    end

    it 'returns a new datastream' do
      expect(@ds.select.object_id).not_to eq(@ds.object_id)
    end

    it 'does not modifies itself' do
      
      ds = @ds.select :name 
      
      expect(@ds.data).to eq(
        [{name: 'Foo', email: 'foo@mail'},
         {name: 'Bar', email: 'bar@mail'}])
    end

    it 'retrieves specified columns' do
      ds = @ds.select :name

      expect(ds.data).to eq(
        [{name: 'Foo'},
         {name: 'Bar'}])
    end
  end

  describe '#select!' do
    it 'returns a datastream' do
      expect(@ds.select!).to be_a Datastream
    end

    it 'returns itself' do
      expect(@ds.select!.object_id).to eq(@ds.object_id)
    end

    it 'retrieves specified columns' do
      @ds.select! :name, 'email'

      expect(@ds.data).to eq(
        [{name: 'Foo', email: 'foo@mail'},
         {name: 'Bar', email: 'bar@mail'}])
    end

    it 'raises an exception if a column is missing' do
      expect {

        @ds.select! :NAME

      }.to raise_error(MissingColumn,
        "Column 'NAME' was not found")
    end
  end

  describe '#join' do

  end

  # describe '::new' do
  #   it 'creates a datastream from an array' do
  #     ds = Datastream.new @array
  #     expect(ds).to 
  #   end
  # end
end
