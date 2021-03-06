module Datamancer

  def extract args
    raise ArgumentError,
      'Extract requires a source, i.e. extract(from: source)' unless
        args.is_a?(Hash) && args[:from]

    headers = case args[:from]
    when String

      # TODO: The first row (headers row) is dropped; to drop more initial rows should be an option.
     
      # TODO: Test the separator option.

      csv = CSV.read args[:from], col_sep: (args[:separator] || ',')
      csv.shift
    when Hash
      ::ActiveRecord::Base.establish_connection args[:from]
      db = ::ActiveRecord::Base.connection

      # TODO: Test this.

      table = args[:table] || args[:from][:table]
      
      raise ArgumentError,
        'Extract requires a database table, i.e. extract(from: source, table: table_name)' unless table

      db.columns(table).map(&:name)
    end

    @fields  = {}
    @actions = {}
    
    headers.each do |header|
      field   = header.to_sym
      mapping = header
      @fields[field] = mapping
    end unless args[:exclude]

    # The reason behind default_actions is the possibility to
    # write reject_if: nil with the DSL.
    default_actions = {reject_if: :nil, reject_unless: :nil}

    define_singleton_method :field do |name, actions = {}, &block|
      actions[:type] ||= actions[:type_default]
      actions[:default] ||= actions[:empty_default]
      actions = default_actions.merge(actions)
      mapping = actions[:map] || name.to_s

      raise MissingField,
        "Required field '#{mapping}' was not found in '#{args[:from]}'" unless headers.include? mapping
      
      field = name.to_sym
      @fields.delete(actions[:map].to_sym) if actions[:map]
      @fields[field]  = mapping
      @actions[field] = actions
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

      #TODO: Test for column names with spaces.
      #TODO: Implement all the SQL escaping cases.

      columns = @fields.map { |field, mapping| "[#{mapping}] AS [#{field}]" }.join(', ')

      @fields.keys.each_with_index do |field, index|
        @fields[field] = index
      end

      # TODO: Test top, distinct, where.
      # TODO: Top for CSV.
      # TODO: Top for support several databases.

      if args[:distinct]
        db.select_rows(
          "SELECT DISTINCT #{columns} FROM #{table}")
      elsif args[:top]
        db.select_rows(
          "SELECT TOP #{args[:top]} #{columns} FROM #{table} #{'WHERE ' + args[:where] if args[:where]}")
      else
        db.select_rows(
          "SELECT #{columns} FROM #{table} #{'WHERE ' + args[:where] if args[:where]}")
      end
    end

    output.map! do |array_row|
      hash_row = {}

      @fields.each do |field, index|
        value = array_row[index]
        hash_row[field] = field_actions field, value, @actions[field]
      end

      if hash_row.has_value?(:reject)
        nil
      else
        hash_row
      end
    end.compact!

    output
  end

  def field_actions field, value, actions
    return value unless actions

    # TODO: Revisit the order of actions.

    ## Block-passing ##
    
    # TODO: Test this.

    if actions[:block]
      value = actions[:block].call(value)
    end

    ## Stripping ##

    # TODO: Test this.

    if actions[:strip]
      value.strip! if value.respond_to?(:strip!)
    end

    ## Casting ##

    # Indexes and :type_default are not good friends.
    # (Because of join while transforming.)

    # TODO: Test this.

    if value || actions[:type_default]

      # TODO: Better data types support. From Mongoid:

      # [ ] Array
      # [ ] BigDecimal
      # [ ] Boolean
      # [x] Float
      # [ ] Hash
      # [x] Integer
      # [ ] Range
      # [ ] Regexp
      # [x] String
      # [x] Symbol
      # [x] Date
      # [ ] DateTime
      # [ ] Time
      # [ ] TimeWithZone

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
      when 'Date'
        # From "1900-01-01 00:00:00.000" to "19000101".
        value = value.gsub('-', '')[0..7] 
      end
    end

    ## Default value ##
  
    # TODO: Test this.

    if value.nil? || (actions[:empty_default] && value.empty?)
      value = actions[:default]
    end

    ## Validation ##

    # TODO: Test this. Test to not reject nil by default.

    if actions[:reject_if].is_a? Array
      actions[:reject_if].each do |reject_value|
        value = :reject if reject_value == value
      end
    else
      value = :reject if actions[:reject_if] == value
    end

    if actions[:reject_unless].is_a? Array
      actions[:reject_unless].each do |reject_value|
        value = :reject if reject_value != value
      end
    else
      value = :reject if actions[:reject_unless] != :nil &&
                         actions[:reject_unless] != value
    end

    value
  end
end
