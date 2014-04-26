module Datamancer
  class ExistingColumn < StandardError; end
  class MissingColumn < StandardError; end

  class Datastream
    attr_accessor :data

    def initialize array = []
      @data = array
      @headers = array.first.keys if array.first

      unless array.empty?
        @headers.each do |column_name|
          define_singleton_method column_name.to_s.downcase.gsub(' ', '_') do
            # @readonly_row[column_name].dup may be safer, because of methods
            # that modify the object itself.
            
            @readonly_row[column_name]
          end
        end
      end
    end

    def transform &block
      duplicate.transform! &block
    end

    def transform! &block

      if block

        @outer_self = block.binding.eval 'self'

        @data.each do |row|
          @row = row
          @readonly_row = row.dup

          instance_eval &block

        end
      end

      self
    end

    def select *columns
      duplicate.select! *columns
    end

    def select! *columns

      columns.map!(&:to_sym)

      columns.each do |column_name|
        raise MissingColumn,
          "Column '#{column_name}' was not found" unless @headers.include? column_name
      end
      
      dead_columns = @headers - columns

      transform! do
        dead_columns.each do |column|
          delete column
        end
      end
    end

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

      # TODO: Test this:

      raise StandardError, 'Sadness: null join.' if output.empty?

      output
    end
  
    def add left, right
      first_row = left.first.merge right.first

      keys = first_row.keys

      valores_por_defecto = {}

      keys.each do |key|
        valores_por_defecto[key] = case first_row[key]
                                  when String then ''
                                  when Numeric then 0
                                  else nil end
      end

      output = []

      (left + right).each do |input_row|

        output_row = {}

        keys.each do |key|
          output_row[key] = input_row[key] || valores_por_defecto[key]
        end

        output << output_row
      end

      output
    end

    private
    
    def duplicate
      Datastream.new(@data.map(&:dup))
    end

    def method_missing method, *args, &block
      @outer_self.send method, *args, &block
    end

    def create column_name, value
      column_name = column_name.to_sym

      raise ExistingColumn,
        "Column '#{column_name}' already exists" if @row.include? column_name

      @row[column_name] = value
    end
    
    def read column_name
      column_name = column_name.to_sym

      raise MissingColumn,
        "Column '#{column_name}' was not found" unless @row.include? column_name

      @readonly_row[column_name]
    end

    def update column_name, value
      column_name = column_name.to_sym

      raise MissingColumn,
        "Column '#{column_name}' was not found" unless @row.include? column_name

      @row[column_name] = value
    end

    def delete column_name
      column_name = column_name.to_sym

      raise MissingColumn,
        "Column '#{column_name}' was not found" unless @row.include? column_name

      @row.delete column_name
    end

    def rename column_name, new_column_name
      # Directly operating over @row would be more efficient.

      create new_column_name, read(column_name)
      delete column_name
    end

    def row; @row end

  end
end
