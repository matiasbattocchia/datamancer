module Datamancer

  def load input, args
# TODO: raise ArgumentError, 'load() requires an input' unless input
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

# TODO: Optimize this to_sym things everywhere.
    define_singleton_method :field do |name, options = {}|
      raise MissingField,
        "Required field '#{name}' was not found in '#{args[:to]}'" unless @input_row.include? name.to_sym

      @output_row[options[:map] || name.to_sym] = @input_row[name.to_sym]
      @output_row.delete(name.to_sym) if !args[:exclude] && options[:map]
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
      ActiveRecord::Base.establish_connection(args[:to])
      table = args[:table] || args[:to][:table]

      ActiveRecord::Base.connection.delete("DELETE FROM #{table}") unless args[:append]
      batch_size = args[:batch] || 1000

      pre_query = "INSERT INTO #{table} (#{columns.join(',')}) VALUES "

      # String values must be enclosed by single quotes.
      inserts.map! do |insert|
        insert.map! do |field|
          field.is_a?(String) ? "'#{field}'" : field
        end

        "(#{insert.join(',')})"
      end

      until inserts.empty?
        query = pre_query + inserts.pop(batch_size).join(',')
        ActiveRecord::Base.connection.execute query
      end
    end
  end
end
