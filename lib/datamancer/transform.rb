module Datamancer

  def join left, right, attribute

    attribute = attribute.to_sym

    left_groups  = Hash.new { |hash, key| hash[key] = [] }
    right_groups = Hash.new { |hash, key| hash[key] = [] }

    left.each do |tuple|
      left_groups[tuple[attribute]] << tuple if tuple[attribute]
    end
    
    right.each do |tuple|
      right_groups[tuple[attribute]] << tuple if tuple[attribute]
    end

    output = Array.new
 
    left_groups.each do |key, left_group|
      
      if right_group = right_groups[key]

        left_group.each do |left_tuple|
          right_group.each do |right_tuple|
            output << left_tuple.merge(right_tuple)
          end
        end

      end
    end

    output
  end

  def where input, attributes

  end

  def unique input, attribute

    attribute = attribute.to_sym
    output = Array.new
    unique_values = Array.new

    input.each do |row|
      unless unique_values.include?(row[attribute])
        output << row
        unique_values << row[attribute]
      end
    end

    output
  end

  def transform input, args = {}

    if args[:join]
      raise ArgumentError unless args[:on]
      raise ArgumentError unless input.first.keys.include?(args[:on].to_sym)
      raise ArgumentError unless args[:join].first.keys.include?(args[:on].to_sym)

      input = join input, args[:join], args[:on]
    end

    if args[:unique]
      input = unique input, args[:unique]
    end

    # TODO: Method-overriding safeguard.
  
    input.first.each_key do |key|
      define_singleton_method key.downcase do
      
        # Some methods applied to fields might modify the original fields.
        # Fields could be duplicated in case this be a common problem.

        #@input_row[key].dup

        @input_row[key]
      end
    end

    define_singleton_method :row_number do
      @row_number
    end

    define_singleton_method :row do
      @supplementary_row
    end

    define_singleton_method :count do
      @count += 1
    end

    define_singleton_method :output do
      @output
    end

    define_singleton_method :switch do |slot|
      @slot = slot
    end

    define_singleton_method :field do |name, value = nil, *args|
      raise MissingField,
        "Required field '#{name}' was not found" unless @input_row.include?(name.to_sym)

      @output_row[name.to_sym] = if value.is_a?(Symbol)
                                   send(name.downcase).send *args.unshift(value)
                                 else
                                   value || send(name.downcase)
                                 end
    end

    define_singleton_method :del_field do |name|
      raise MissingField,
        "Filtered field '#{name}' was not found" unless @input_row.include?(name.to_sym)
      
      @output_row.delete(name.to_sym)
    end

    define_singleton_method :new_field do |name, value|
      raise ExistingField,
        "New field '#{name}' already exists" if @input_row.include?(name.to_sym)

      @output_row[name.to_sym] = value
    end

    # TODO: Test for count.

    @count = 0
    
    # TODO: Test for slots.
    
    @output = Hash.new { |h, k| h[k] = [] }

    input.each_with_index do |row, row_number|

      # TODO: Test for row_number.
      # TODO: Test for (supplementary) row.

      @row_number = row_number
      @input_row = row
      @supplementary_row = @input_row.dup
      @output_row = args[:exclude] ? {} : @input_row.dup

      yield if block_given?

      @output[@slot] << @output_row
    end

    @output.length == 1? @output[nil] : @output
  end

  def aggregate input

    define_singleton_method :dim do |name|
      name = name.to_sym
      @dimensions[name] = @row[name]
    end
    
    define_singleton_method :fact do |name|
      name = name.to_sym
      @facts[name] = @row[name]
    end

    aggregated_input = Hash.new { |hash, key| hash[key] = Hash.new }

    input.each_with_index do |row, row_number|
      @row = row
      @dimensions = {}
      @facts = {}

      yield if block_given?

      aggregated_input[@dimensions].merge!(@facts) { |_, fact, other_fact| fact + other_fact }
    end

    aggregated_input.map do |dimensions, facts|
      dimensions.merge(facts)
    end
  end
end
