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

  # describe '::new' do
  #   it 'creates a datastream from an array' do
  #     ds = Datastream.new @array
  #     expect(ds).to 
  #     ds.data == []
  #     ds.headers == []
  #   end
  # end
  it { should respond_to(:data) }
  it { should respond_to(:headers) }
  # it { is_expected.to respond_to(:data_types) }

  # EXTRACT

  # describe '#raw'

  # En el bloque:
  # valores por defecto (null/empty)
  # casteo
  # mapeo
  # strip
  # bloques

  # TRANSFORM

  # Enumerable and Array instance methods could be wrapped but for now
  # they shall be accessed directly over 'datastream.data'.
  # #each, #map, #pop, #push/#<<, #shift, #unshift,
  # #all, #first, #last, #length/#count, #empty?

  # describe '#ungroup'

  # describe '#sort'
  # describe '#reverse_order'
  # describe '#reverse_sort'

  # describe '#left_join'
  # describe '#right_join'
  # describe '#outer_join'

  # describe '#union'
  # Alias #union to #|

  # Alias #union_all to #+.

  # describe '#except'
  # describe '#except_all'
  # Alias #except_all to #-.

  # describe '#intersect'
  # Alias #intersect to #&
  # describe '#intersect_all'

  # Alias #distinct to #uniq and #unique

  # Methods that do datastream damming:
  # #group, #distinct, aggregations...

  describe '#transform' do

    before(:each) do
      @ds = Datastream.new [{name: 'Foo', email: 'foo@mail'},
                            {name: 'Bar', email: 'bar@mail'}]
    end

    it 'does not modifies itself' do      
      @ds.transform do
        update :name, nil
      end

      expect(@ds.data).to eq(
        [{name: 'Foo', email: 'foo@mail'},
         {name: 'Bar', email: 'bar@mail'}])
    end

    it 'modifies the returned datastream' do
      ds = @ds.transform do
        update :name, nil
      end

      expect(ds.data).to eq(
        [{name: nil, email: 'foo@mail'},
         {name: nil, email: 'bar@mail'}])
    end
  end

  describe '#transform!' do

    before(:each) do
      @ds = Datastream.new [{name: 'Foo', email: 'foo@mail'},
                            {name: 'Bar', email: 'bar@mail'}]
    end

    # TODO: row number, count, create row, delete row, readonly_row access.

    it 'returns itself' do
      expect(@ds.transform!{}.object_id).to eq(@ds.object_id)
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
      
      it 'is a private method' do
        expect { @ds.create }.to raise_error NoMethodError
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
      
      it 'is a private method' do
        expect { @ds.read }.to raise_error NoMethodError
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
 
      it 'is a private method' do
        expect { @ds.name }.to raise_error NoMethodError
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

      it 'is a private method' do
        expect { @ds.update }.to raise_error NoMethodError
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

      it 'is a private method' do
        expect { @ds.delete }.to raise_error NoMethodError
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

      it 'is a private method' do
        expect { @ds.rename }.to raise_error NoMethodError
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

      it 'is a private method' do
        expect { @ds.row }.to raise_error NoMethodError
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

    before(:each) do
      @ds = Datastream.new [{name: 'Foo', email: 'foo@mail', admin: false},
                            {name: 'Bar', email: 'bar@mail', admin: false}]
    end

    it 'does not modifies itself' do
      ds = @ds.select :name
      
      expect(@ds.data).to eq(
        [{name: 'Foo', email: 'foo@mail', admin: false},
         {name: 'Bar', email: 'bar@mail', admin: false}])
    end

    it 'retrieves specified columns' do
      ds = @ds.select :name, 'email'

      expect(ds.data).to eq(
        [{name: 'Foo', email: 'foo@mail'},
         {name: 'Bar', email: 'bar@mail'}])
    end
  end

  describe '#select!' do

    before(:each) do
      @ds = Datastream.new [{name: 'Foo', email: 'foo@mail', admin: false},
                            {name: 'Bar', email: 'bar@mail', admin: false}]
    end

    it 'returns itself' do
      expect(@ds.select!(:name).object_id).to eq(@ds.object_id)
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

    before(:each) do
      @one_ds = Datastream.new [{number: 1,   letter: 'a', name: 'Foo'},
                                {number: 2,   letter: 'a', name: 'Bar'},
                                {number: 2,   letter: 'b', name: 'Baz'},
                                {number: nil, letter: 'c', name: 'Foobar'}]

      @another_ds = Datastream.new [{number: 1,   letter: 'a', city: 'Foobaria'},
                                    {number: 2,   letter: 'b', city: 'Barbaza'},
                                    {number: 3,   letter: 'b', city: 'Bazfooa'},
                                    {number: nil, letter: 'c', city: '01'}]
    end

    it 'does not modifies itself' do
      ds = @one_ds.join @another_ds

      expect(@one_ds.data).to eq(
        [{number: 1,   letter: 'a', name: 'Foo'},
         {number: 2,   letter: 'a', name: 'Bar'},
         {number: 2,   letter: 'b', name: 'Baz'},
         {number: nil, letter: 'c', name: 'Foobar'}])
    end

    it 'does not modifies the joining datastream' do
      ds = @one_ds.join @another_ds

      expect(@another_ds.data).to eq(
        [{number: 1,   letter: 'a', city: 'Foobaria'},
         {number: 2,   letter: 'b', city: 'Barbaza'},
         {number: 3,   letter: 'b', city: 'Bazfooa'},
         {number: nil, letter: 'c', city: '01'}])
    end
 
    it 'performs a natural join between two datastreams' do
      ds = @one_ds.join @another_ds

      expect(ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo', city: 'Foobaria'},
         {number: 2, letter: 'b', name: 'Baz', city: 'Barbaza'}])
    end
  end

    # it 'raises an exception if a column is missing' do
    #   expect {
    #     @one_ds.join @another_ds, :NUMBER
    #   }.to raise_error(MissingColumn,
    #     "Column 'NUMBER' was not found")
    # end

    # it 'performs an equi-join between two datasets' do
    #   ds = @one_ds.join @another_ds, :number

    #   expect(ds.data).to eq(
    #     [{number: 1, letter: 'a', name: 'Foo', city: 'Foobaria'},
    #      {number: 2, letter: 'b', name: 'Bar', city: 'Barbaza'},
    #      {number: 2, letter: 'b', name: 'Baz', city: 'Barbaza'}])
    # end

  describe '#join!' do

    before(:each) do
      @one_ds = Datastream.new [{number: 1,   letter: 'a', name: 'Foo'},
                                {number: 2,   letter: 'a', name: 'Bar'},
                                {number: 2,   letter: 'b', name: 'Baz'},
                                {number: nil, letter: 'c', name: 'Foobar'}]

      @another_ds = Datastream.new [{number: 1,   letter: 'a', city: 'Foobaria'},
                                    {number: 2,   letter: 'b', city: 'Barbaza'},
                                    {number: 3,   letter: 'b', city: 'Bazfooa'},
                                    {number: nil, letter: 'c', city: '01'}]
    end

    it 'returns itself' do
      expect(@one_ds.join!(@another_ds).object_id).to eq(@one_ds.object_id)
    end

    it 'performs a natural join between two datasets' do
      @one_ds.join! @another_ds

      expect(@one_ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo', city: 'Foobaria'},
         {number: 2, letter: 'b', name: 'Baz', city: 'Barbaza'}])
    end

    it 'raises an exception if the datastreams cannot be joined' do
      expect { @one_ds.join! Datastream.new }.to raise_error(NullJoin,
        'Datastreams do not share any column')
    end
  end

  describe '#join_and_delete' do

    before(:each) do
      @one_ds = Datastream.new [{number: 1,   letter: 'a', name: 'Foo'},
                                {number: 2,   letter: 'a', name: 'Bar'},
                                {number: 2,   letter: 'b', name: 'Baz'},
                                {number: nil, letter: 'c', name: 'Foobar'}]

      @another_ds = Datastream.new [{number: 1,   letter: 'a', city: 'Foobaria'},
                                    {number: 2,   letter: 'b', city: 'Barbaza'},
                                    {number: 3,   letter: 'b', city: 'Bazfooa'},
                                    {number: nil, letter: 'c', city: '01'}]
    end


    it 'does not modifies itself' do
      @one_ds.join_and_delete @another_ds

      expect(@one_ds.data).to eq(
        [{number: 1,   letter: 'a', name: 'Foo'},
         {number: 2,   letter: 'a', name: 'Bar'},
         {number: 2,   letter: 'b', name: 'Baz'},
         {number: nil, letter: 'c', name: 'Foobar'}])
    end

    it 'does not modifies the joining datastream' do
      @one_ds.join_and_delete @another_ds

      expect(@another_ds.data).to eq(
        [{number: 1,   letter: 'a', city: 'Foobaria'},
         {number: 2,   letter: 'b', city: 'Barbaza'},
         {number: 3,   letter: 'b', city: 'Bazfooa'},
         {number: nil, letter: 'c', city: '01'}])
    end

    it 'performs a natural join between two datasets and deletes joining columns' do
      ds = @one_ds.join_and_delete @another_ds

      expect(ds.data).to eq(
        [{name: 'Foo', city: 'Foobaria'},
         {name: 'Baz', city: 'Barbaza'}])
    end
  end

  describe '#join_and_delete!' do

    before(:each) do
      @one_ds = Datastream.new [{number: 1,   letter: 'a', name: 'Foo'},
                                {number: 2,   letter: 'a', name: 'Bar'},
                                {number: 2,   letter: 'b', name: 'Baz'},
                                {number: nil, letter: 'c', name: 'Foobar'}]

      @another_ds = Datastream.new [{number: 1,   letter: 'a', city: 'Foobaria'},
                                    {number: 2,   letter: 'b', city: 'Barbaza'},
                                    {number: 3,   letter: 'b', city: 'Bazfooa'},
                                    {number: nil, letter: 'c', city: '01'}]
    end

    it 'returns itself' do
      expect(@one_ds.join_and_delete!(@another_ds).object_id).to eq(@one_ds.object_id)
    end

    it 'performs a natural join between two datasets and deletes joining columns' do
      @one_ds.join_and_delete! @another_ds

      expect(@one_ds.data).to eq(
        [{name: 'Foo', city: 'Foobaria'},
         {name: 'Baz', city: 'Barbaza'}])
    end
  end

  describe '#union_all' do

    before(:each) do
      @one_ds = Datastream.new [{number: 1, letter: 'a'},
                                {number: 2, letter: 'a'}]

      @another_ds = Datastream.new [{letter: 'b', name: 'Baz'},
                                    {letter: 'c', name: 'Foobar'}]
    end

    it 'does not modifies itself' do
      @one_ds.union_all @another_ds

      expect(@one_ds.data).to eq(
        [{number: 1, letter: 'a'},
         {number: 2, letter: 'a'}])
    end

    it 'does not modifies the joining datastream' do
      @one_ds.union_all @another_ds

      expect(@another_ds.data).to eq(
        [{letter: 'b', name: 'Baz'},
         {letter: 'c', name: 'Foobar'}])
    end

    it 'combines two datastream into a single one' do
      ds = @one_ds.union_all @another_ds

      expect(ds.data).to eq(
        [{number: 1,   letter: 'a', name: nil},
         {number: 2,   letter: 'a', name: nil},
         {number: nil, letter: 'b', name: 'Baz'},
         {number: nil, letter: 'c', name: 'Foobar'}])
    end
  end

  describe '#union_all!' do

    before(:each) do
      @one_ds = Datastream.new [{number: 1, letter: 'a'},
                                {number: 2, letter: 'a'}]

      @another_ds = Datastream.new [{letter: 'b', name: 'Baz'},
                                    {letter: 'c', name: 'Foobar'}]
    end

    it 'returns itself' do
      expect(@one_ds.union_all!(@another_ds).object_id).to eq(@one_ds.object_id)
    end

    it 'combines two datastream into a single one' do
      @one_ds.union_all! @another_ds

      expect(@one_ds.data).to eq(
        [{number: 1,   letter: 'a', name: nil},
         {number: 2,   letter: 'a', name: nil},
         {number: nil, letter: 'b', name: 'Baz'},
         {number: nil, letter: 'c', name: 'Foobar'}])
    end
  end

  describe '#group' do

    before(:each) do
      @ds = Datastream.new [{number: 1, letter: 'a', name: 'Foo',    coins: 2},
                            {number: 1, letter: 'a', name: 'Bar',    coins: 5},
                            {number: 2, letter: 'a', name: 'Baz',    coins: 1},
                            {number: 2, letter: 'b', name: 'Foobar', coins: 3},
                            {number: 2, letter: 'b', name: 'Foobaz', coins: 2}]
    end
 
    # it 'aggregates a datastream' do
    #   pending
    #   @ds.group! :number, 'letter'

    #   expect(@ds.data).to eq(
    #     [{number: 1, letter: 'a', coins: 7},
    #      {number: 2, letter: 'a', coins: 1},
    #      {number: 2, letter: 'b', coins: 5}])
    # end

    ### GROUP ###

    ## Casos de uso ##
    # 
    # 1. Calcular la varianza por grupo [1/n * sum(x-X)**2].
    #    x es el elemento, X es el promedio del grupo.
    # 2. SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders 
    #    FROM (Orders
    #    INNER JOIN Employees
    #    ON Orders.EmployeeID=Employees.EmployeeID)
    #    GROUP BY LastName
    #    HAVING COUNT(Orders.OrderID) > 10;

    # orders.join(employees).group(:last_name).transform do
    #   create :number_of_orders, result.count
    #   delete :result
    # end.where(number_of_orders: '> 10')


    ## SQL aggregate functions ##
    #
    # They always require to be selected AS a different column in SQL.
    # We could work implicit columns for them, such as the same column
    # name for sum(), column_name_avr for avr(), and so on.
    #
    # Aggregated columns are always lost, thus it is ok to recycle them
    # in sum().
    #
    # Average()
    # Count() / Count(*) means count rows and Count(column),
    #   ignore rows with null values within column.
    # Maximum()
    # Median()
    # Minimum()
    # Mode()
    # Sum()
    # Aggregate(), es genérico, una especie de inject().
    #
    # Estos métodos se le aplican al datastream y quedan cacheados dentro del mismo (salvo aggregate
    # es genérico).
    #
    # orders.join(employees).group(:last_name).count(:orders).where(orders: gt 10)
    #
    # Si uno de los métodos de agregación se llama sobre un datastream que agrupa datastreams, pueden
    # pasar dos cosas: si la columna a la que se hace referencia está en el datastream objetivo,
    # funciona normalmente; pero si no, antes de tirar MissingColumn, busca en los datastreams agrupados.
    # En caso de encontrarla realiza la agregación y la "extrae" a la fila que contiene al grupo.
    # Opcionalmente se puede elegir el nombre de la nueva columna.

    # ## Opción A ##

    # # Suma objetos de clase Numeric, implícitamente; o explícitamente (objetos 
    # # de cualquier clase) si se especifica pasando un bloque la operación con
    # # un método, proc, o bloque.

    # @ds.group! :number, 'letter'

    # @ds.group! :number, 'letter' do
    #   aggregate :coins,  coin
    # end

    # # Una columna llamada 'count' posiblemente deba ser agregada para
    # # conservar la información del tamaño del grupo.
    
    # expect(@ds.data).to eq(
    #   [{number: 1, letter: 'a', coins: 7, count: 2},
    #    {number: 2, letter: 'a', coins: 1, count: 1},
    #    {number: 2, letter: 'b', coins: 5, count: 2}])

    # ## OPCIÓN B ##

    # # Agregar es colapsar sobre un arreglo por cada columna
    # # y por grupo, sus correspondientes valores. Ejemplo:
 
    # @ds.group!(:number, :letter)

    # expect(@ds.data).to eq(
    #   [{number: 1, letter: 'a', names: ['Foo','Bar'],       coins: [2,5] },
    #    {number: 2, letter: 'a', names: ['Baz'],             coins: [1]   },
    #    {number: 2, letter: 'b', names: ['Foobar','Foobaz'], coins: [3,2] }])

    # # Una columna llamada 'count' posiblemente deba ser agregada para
    # # conservar la información del tamaño del grupo, y valores nil
    # # no añadidos al arreglo.

    # # Una selección previa de columnas dejaría de lado las columnas
    # # que no deben ser agrupadas en caso de ser necesario.

    # # Entonces las funciones de agregación se implementan a
    # # través de una transformación:

    # @ds.transform! do
    #   update :coins, coins.reduce(:+) # Sum array elements.
    #   create :coin_average, coins.reduce(:+) / count
    #   create :coin_maximum, coins.max
    # end

    # # La opción B se ve linda; sin embargo me parece que no aporta nada útil.

    # # Para cumplir con el caso de uso, rápidamente:
    # # 1. Agrupar para obtener el promedio, no sobreescribir el datastream.
    # # 2. Unir los datastream original y agrupado.
    # # 3. Transformar.
    # #
    # # Una variante es obviar el paso 2 y utilizar where sobre el datastream
    # # agrupado. Es más legible, quizás menos eficiente.

    ## OPCIÓN C ##

    # @ds.group!(:number, :letter)

    # expect(@ds.data).to eq(
    #   [{number: 1, letter: 'a', result: Datastream },
    #    {number: 2, letter: 'a', result: Datastream },
    #    {number: 2, letter: 'b', result: Datastream }]) 

    # @ds.transform! do
    #   create :members, result.count
    #   create :coins, result.sum(:coins)
    #   create :variance, result.aggregate { ( coins - result.avr(:coins) ) ** 2 } / result.count
    #   delete :result
    # end

    # expect(@ds.data) to.eq(
    #   [{number: 1, letter: 'a', members: 2, coins: 7, variance: 5},
    #    {number: 1, letter: 'a', members: 1, coins: 1, variance: 2},
    #    {number: 2, letter: 'b', members: 2, coins: 5, variance: 3}]

    ####

    # Lo siguiente se lograría mejor con #ungroup, que sería indispensable
    # para procesos en paralelo. Mientras tanto, es una forma posible.

    # (group_ds = @ds.group(:number, :letter)).each do
    #   result.transform! do
    #     create :deviation, result.avr(:coins) - coins
    #   end
    # end

    # expect(@ds.data) to.eq(
    #   [{number: 1, letter: 'a', name: 'Foo',    coins: 2, deviation: 5},
    #    {number: 1, letter: 'a', name: 'Bar',    coins: 5, deviation: 2},
    #    {number: 2, letter: 'a', name: 'Baz',    coins: 1, deviation: 0},
    #    {number: 2, letter: 'b', name: 'Foobar', coins: 3, deviation: 2},
    #    {number: 2, letter: 'b', name: 'Foobaz', coins: 2, deviation: 3}]

    # expect(group_ds.data).to eq(
    #   [{number: 1, letter: 'a', result: Datastream },
    #    {number: 2, letter: 'a', result: Datastream },
    #    {number: 2, letter: 'b', result: Datastream }]) 

    ####

    it 'does not modifies itself' do
      @ds.group :name

      expect(@ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo',    coins: 2},
         {number: 1, letter: 'a', name: 'Bar',    coins: 5},
         {number: 2, letter: 'a', name: 'Baz',    coins: 1},
         {number: 2, letter: 'b', name: 'Foobar', coins: 3},
         {number: 2, letter: 'b', name: 'Foobaz', coins: 2}])
    end

    it 'partitions the datastream into groups' do
      ds = @ds.group :number, 'letter'

      ds.data.each do |row|
        row[:group] = row[:group].data
      end

      expect(ds.data).to eq(
        [{number: 1, letter: 'a', group: [{number: 1, letter: 'a', name: 'Foo',    coins: 2},
                                          {number: 1, letter: 'a', name: 'Bar',    coins: 5}]},
         {number: 2, letter: 'a', group: [{number: 2, letter: 'a', name: 'Baz',    coins: 1}]},
         {number: 2, letter: 'b', group: [{number: 2, letter: 'b', name: 'Foobar', coins: 3},
                                          {number: 2, letter: 'b', name: 'Foobaz', coins: 2}]}])
    end

    it 'raises an exception if a column is missing' do
      expect {

        @ds.group :NAME

      }.to raise_error(MissingColumn,
        "Column 'NAME' was not found")
    end
  end

  describe '#order!' do
    pending
  end

  describe '#where' do

    before(:each) do
      @ds = Datastream.new [{number: 1, letter: 'a', name: 'Foo', city: 'Foobaria'},
                            {number: 2, letter: 'a', name: 'Bar', city: 'Barbaza'},
                            {number: 2, letter: 'b', name: 'Baz', city: 'Bazfooa'},
                            {number: 2, letter: 'c', name: 'Foobaz', city: '01'},
                            {number: 3, letter: 'c', name: 'Foobar', city: 'Barbaza'}]
    end

    it 'does not modifies itself' do
      @ds.where name: 'Foo'

      expect(@ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo', city: 'Foobaria'},
         {number: 2, letter: 'a', name: 'Bar', city: 'Barbaza'},
         {number: 2, letter: 'b', name: 'Baz', city: 'Bazfooa'},
         {number: 2, letter: 'c', name: 'Foobaz', city: '01'},
         {number: 3, letter: 'c', name: 'Foobar', city: 'Barbaza'}])
    end

    it 'filters a datastream' do
      ds = @ds.where number: 1..2, letter: ['a', 'b'], 'name' => /ba/i, 'city' => 'Barbaza'

      expect(ds.data).to eq([{number: 2, letter: 'a', name: 'Bar', city: 'Barbaza'}])
    end
  end

  describe '#where!' do

    before(:each) do
      @ds = Datastream.new [{number: 1, letter: 'a', name: 'Foo', city: 'Foobaria'},
                            {number: 2, letter: 'a', name: 'Bar', city: 'Barbaza'},
                            {number: 2, letter: 'b', name: 'Baz', city: 'Bazfooa'},
                            {number: 2, letter: 'c', name: 'Foobaz', city: '01'},
                            {number: 3, letter: 'c', name: 'Foobar', city: 'Barbaza'}]
    end
    
    it 'returns itself' do
      expect(@ds.where!(name: 'Foo').object_id).to eq(@ds.object_id)
    end

    it 'filters a datastream' do
      @ds.where! number: 1..2, letter: ['a', 'b'], 'name' => /ba/i, 'city' => 'Barbaza'

      expect(@ds.data).to eq([{number: 2, letter: 'a', name: 'Bar', city: 'Barbaza'}])
    end

    it 'raises an exception if a column is missing' do
      expect {

        @ds.where! NAME: 'Foo'

      }.to raise_error(MissingColumn,
        "Column 'NAME' was not found")
    end
  end

  describe '#distinct' do

    before(:each) do
      @ds = Datastream.new [{number: 1, letter: 'a', name: 'Foo'},
                            {number: 1, letter: 'a', name: 'Bar'},
                            {number: 2, letter: 'a', name: 'Baz'},
                            {number: 2, letter: 'b', name: 'Foobaz'},
                            {number: 2, letter: 'b', name: 'Foobaz'}]
    end

    it 'does not modifies itself' do
      @ds.distinct :name

      expect(@ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo'},
         {number: 1, letter: 'a', name: 'Bar'},
         {number: 2, letter: 'a', name: 'Baz'},
         {number: 2, letter: 'b', name: 'Foobaz'},
         {number: 2, letter: 'b', name: 'Foobaz'}])
    end

    it 'lists only different values' do
      ds = @ds.distinct :number, 'letter'

      expect(ds.data).to eq(
        [{number: 1, letter: 'a'},
         {number: 2, letter: 'a'},
         {number: 2, letter: 'b'}])
    end

    it 'lists only different values (impltcitly)' do
      ds = @ds.distinct!

      expect(ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo'},
         {number: 1, letter: 'a', name: 'Bar'},
         {number: 2, letter: 'a', name: 'Baz'},
         {number: 2, letter: 'b', name: 'Foobaz'}])
    end
  end

  describe '#distinct!' do

    before(:each) do
      @ds = Datastream.new [{number: 1, letter: 'a', name: 'Foo'},
                            {number: 1, letter: 'a', name: 'Bar'},
                            {number: 2, letter: 'a', name: 'Baz'},
                            {number: 2, letter: 'b', name: 'Foobaz'},
                            {number: 2, letter: 'b', name: 'Foobaz'}]
    end
    
    it 'returns itself' do
      expect(@ds.distinct!(:name).object_id).to eq(@ds.object_id)
    end

    it 'lists only different values' do
      @ds.distinct! :number, 'letter'

      expect(@ds.data).to eq(
        [{number: 1, letter: 'a'},
         {number: 2, letter: 'a'},
         {number: 2, letter: 'b'}])
    end

    it 'removes duplicated rows' do
      @ds.distinct!

      expect(@ds.data).to eq(
        [{number: 1, letter: 'a', name: 'Foo'},
         {number: 1, letter: 'a', name: 'Bar'},
         {number: 2, letter: 'a', name: 'Baz'},
         {number: 2, letter: 'b', name: 'Foobaz'}])
    end

    it 'raises an exception if a column is missing' do
      expect {

        @ds.distinct! :NAME

      }.to raise_error(MissingColumn,
        "Column 'NAME' was not found")
    end
  end

end
