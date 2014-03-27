module Datamancer

  def raw input, args

    raise ArgumentError,
      'Raw requires a destination, i.e. raw(data, to: destination)' unless
        args.is_a?(Hash) && args[:to]

    ::ActiveRecord::Base.establish_connection(args[:to])

    # TODO: Test this.

    # table = args[:table] || args[:to][:table]
      
    # raise ArgumentError,
    #   'Raw requires a database table, i.e. raw(data, to: destination, table: table_name)' unless table

    # TODO: Method-overriding safeguard.
  
    input.first.each_key do |key|
      define_singleton_method key.downcase do
      
        # Some methods applied to fields might modify the original fields.
        # Fields could be duplicated in case this be a common problem.

        #@input_row[key].dup

        @input_row[key]
      end
    end

    define_singleton_method :db do
      ::ActiveRecord::Base.connection
    end
    
    define_singleton_method :query do |query|
      ::ActiveRecord::Base.connection.execute query
    end

    input.each do |row|
      @input_row = row
      
      yield if block_given?
    end
  end


  def load input, args

    raise ArgumentError,
      'Load requires a destination, i.e. load(data, to: destination)' unless
        args.is_a?(Hash) && args[:to]

    ## COLUMNS ##

    # define_singleton_method :field do |name, options = {}|
    #   @columns << (options[:map] || name)
    # end

    # @columns = []

    # yield if block_given?

    ## INSERTS ##

    define_singleton_method :field do |name, options = {}|
      name = name.to_sym

      raise MissingField,
        "Required field '#{name}' was not found in '#{args[:to]}'" unless @input_row.include? name

      @output_row[options[:map] || name] = @input_row[name]
      @output_row.delete(name) if !args[:exclude] && options[:map]
    end
 
    inserts = []

    input.each do |row|
      @input_row = row
      @output_row = args[:exclude] ? {} : row.dup

      yield if block_given?

      inserts << @output_row.values
    end

    columns = @output_row.keys

    ## LOAD ##

    # TODO: Set 'w' or 'w+' for CSV writing.
    
    if args[:to].is_a?(String)
      mode = if args[:append] then 'a' else 'w' end
      
      CSV.open(args[:to], mode) do |csv|
        csv << columns

        inserts.each do |insert|
          csv << insert
        end
      end

    else
      ::ActiveRecord::Base.establish_connection(args[:to])

      # TODO: Test this.

      table = args[:table] || args[:to][:table]
      
      raise ArgumentError,
        'Load requires a database table, i.e. load(data, to: destination, table: table_name)' unless table

      ::ActiveRecord::Base.connection.delete("DELETE FROM #{table}") unless args[:append]
      batch_size = args[:batch] || 1000

      pre_query = "INSERT INTO #{table} (#{columns.join(',')}) VALUES "

      # String values must be enclosed by single quotes.
      inserts.map! do |insert|
        insert.map! do |field|
          field.is_a?(String) ? "'#{field}'" : (field ? field : 'NULL')
        end

        "(#{insert.join(',')})"
      end

      until inserts.empty?
        query = pre_query + inserts.pop(batch_size).join(',')
        ::ActiveRecord::Base.connection.execute query
      end
    end
  end
end
