module Datamancer

  def extract args
    raise ArgumentError,
      'Extract requires a source, i.e. extract(from: source)' unless
        args.is_a?(Hash) && args[:from]

    headers = case args[:from]
    when String
# TODO: The first row (headers row) is dropped; to drop more initial rows should be an option.
      csv = CSV.read args[:from]
      csv.shift
    when Hash
      ActiveRecord::Base.establish_connection args[:from]
      db = ActiveRecord::Base.connection
      table = args[:table] || args[:from][:table]
      db.columns(table).map(&:name)
    end

    @fields  = {}
    @actions = {}

    headers.each do |header|
      field   = header.to_sym
      mapping = header
      @fields[field] = mapping
    end unless args[:exclude]

    define_singleton_method :field do |name, options = {}, &block|
      mapping = options[:map] || name.to_s
      
      raise MissingField,
        "Required field '#{mapping}' was not found in '#{args[:from]}'" unless headers.include? mapping
      
      field   = name.to_sym
      @fields.delete(options[:map].to_sym) if options[:map]
      @fields[field]  = mapping
      @actions[field] = options
      @actions[field][:block] = block
    end

    yield if block_given?
    
    output = case args[:from]
    when String
      @fields.each do |field, mapping|
        index = headers.find_index(mapping)
        @fields[field] = index
      end

      csv

    when Hash
      columns = @fields.map { |field, mapping| "#{mapping} AS #{field}" }.join(', ')

      @fields.keys.each_with_index do |field, index|
        @fields[field] = index
      end

      db.select_rows("SELECT #{columns} FROM #{table}")
    end

    output.map! do |array_row|
      hash_row = {}

      @fields.each do |field, index|
        # Type defaults to String. Not necessary with CSV but needed when
        # importing XLS or database.
        value = array_row[index].to_s
        hash_row[field] = field_actions field, value
      end

      hash_row
    end

    #output.compact
    # row = ...
    #row.has_value?(:reject) ? nil : row
  end

  def field_actions field, value
    actions = @actions[field]
    return value unless actions

    ## Casting ##
    
# TODO: Better data type support.
    
    # Array
    # BigDecimal
    # Boolean
    # Date
    # DateTime
    # Float
    # Hash
    # Integer
    # Range
    # Regexp
    # String
    # Symbol
    # Time
    # TimeWithZone

    # Every data type should have a meaningful default value.
    # If a value is missing in a CSV an empty String is assigned
    # to it by Roo, instead of nil. For example "".to_i #=> 0.

    case actions[:type].to_s
    when 'Complex'
      value = value.to_c
    when 'Float'
      value = value.to_f
    when 'Integer'
      value = value.to_i
    when 'Rational'
      value = value.to_r
    when 'String'
      value = value.to_s
    when 'Symbol'
      value = value.to_sym
    end

    ## Block-passing ##

    # Replaces wrapping, trimming, etc.?

    if actions[:block]
      value = actions[:block].call(value)
    end

    ## Validation ##

    #if @options[name][:reject_if]
    #  value = :reject if value == @options[name][:reject_if]
    #end

    value
  end
end
