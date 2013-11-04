module Datamancer

  def transform input, args = {}
# TODO: raise ArgumentError, 'transform() requires an input' unless input

# TODO: Method-overriding safeguard.
    input.first.each_key do |key|
      define_singleton_method key do
        @input_row[key]
      end
    end

    define_singleton_method :field do |name, value = nil|
      raise MissingField,
        "Required field '#{name}' was not found" unless respond_to?(name)
        
      @output_row[name.to_sym] = value || send(name)
    end

    define_singleton_method :del_field do |name|
      raise MissingField,
        "Filtered field '#{name}' was not found" unless respond_to?(name)
      
      @output_row.delete(name.to_sym)
    end

    define_singleton_method :new_field do |name, value|
      raise ExistingField,
        "New field '#{name}' already exists" if respond_to?(name)

      @output_row[name.to_sym] = value
    end

    input.map do |row|
      @input_row = row
      @output_row = args[:exclude] ? {} : row.dup

      yield if block_given?

      @output_row
    end
  end

  def aggregate input
# TODO: raise ArgumentError, 'aggregate() requires an input' unless input
    
    define_singleton_method :dim do |name|
      name = name.to_sym
      @dimensions[name] = @row[name]
    end
    
    define_singleton_method :fact do |name|
      name = name.to_sym
      @facts[name] = @row[name]
    end

    aggregated_input = Hash.new { |hash, key| hash[key] = Hash.new }

    input.each do |row|
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
