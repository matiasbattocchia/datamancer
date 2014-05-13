module Datamancer
  class ExistingColumn < StandardError; end
  class MissingColumn < StandardError; end
  class NullJoin < StandardError; end

  class Datastream
    attr_reader :data, :headers

    def initialize array = []
      self.data = array
    end

    def data= array
      # TODO: Delete previous [column_name] methods before creating new.
      #
      # It is expected that column identifiers respond to #to_s, beyond this
      # they can be objects of any class.

      @data = array
      @headers = array.first ? array.first.keys : []

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
        @outer_self = block.binding.eval 'self'

        @data.each do |row|
          @row = row
          @readonly_row = row.dup

          instance_eval &block
        end

      self
    end

    def select column, *columns
      columns << column
      
      duplicate.select! *columns
    end

    def select! column, *columns
      columns << column

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

    def join datastream
      duplicate.join! datastream
    end

    # TODO: join! creates new rows. It should not.
    def join! datastream
      left = self.data
      right = datastream.data
      columns = self.headers & datastream.headers

      raise NullJoin, 'Datastreams do not share any column' if columns.empty?

      left_groups  = Hash.new { |hash, key| hash[key] = [] }
      right_groups = Hash.new { |hash, key| hash[key] = [] }

      left.each do |row|
        values = columns.map { |column| row[column] }

        left_groups[values] << row unless values.include? nil
      end

      right.each do |row|
        values = columns.map { |column| row[column] }

        right_groups[values] << row unless values.include? nil
      end

      output = []

      left_groups.each do |key, left_group|

        if right_group = right_groups[key]

          left_group.each do |left_tuple|
            right_group.each do |right_tuple|
              output << left_tuple.merge(right_tuple)
            end
          end

        end
      end

      # TODO: I do not like this. Why a datastream's data could be
      # modified directly? Anyway, #join! is likely to be reimplemented
      # many times as more features are added. It will improve.
      self.data = output
      self
    end

    def join_and_delete datastream
      duplicate.join_and_delete! datastream
    end

    def join_and_delete! datastream
      columns = self.headers & datastream.headers

      join!(datastream).transform! do
        columns.each { |column| delete column }
      end
    end

    def union_all datastream
      duplicate.union_all! datastream
    end
  
    # TODO: union_all! creates new rows. It should not.
    def union_all! datastream
      keys = self.headers | datastream.headers

      output = []

      (self.data + datastream.data).each do |input_row|

        output_row = {}

        keys.each do |key|
          output_row[key] = input_row[key]
        end

        output << output_row
      end

      self.data = output
      self
    end

    def where conditions
      duplicate.where! conditions
    end

    def where! conditions
      conditions = conditions.inject({}){|h,(k,v)| h[k.to_sym] = v; h}

      str = []

      conditions.each do |column_name, condition|
        raise MissingColumn,
          "Column '#{column_name}' was not found" unless @headers.include? column_name

        str << case condition
               when Array, Range then "conditions[:#{column_name}].include? row[:#{column_name}]"
               when Regexp then "row[:#{column_name}] =~ conditions[:#{column_name}]"
               else "row[:#{column_name}] == conditions[:#{column_name}]" end
      end

      str = 'not (' + str.join(' and ') + ')'
      @data.reject! { |row| eval str }

      self
    end

    def distinct *columns
      duplicate.distinct! *columns
    end

    def distinct! *columns
      if columns.empty?
        columns = @headers
      else
        columns.map!(&:to_sym)
      end

      columns.each do |column_name|
        raise MissingColumn,
          "Column '#{column_name}' was not found" unless @headers.include? column_name
      end

      # I guess that I could use Set from Stdlib here as well.
      distinct_values = {}

      @data.reject! do |row|
        key = columns.map { |column| row[column] }

        if distinct_values[key]
          true
        else
          distinct_values[key] = true
          false
        end
      end

      select! *columns
    end

    def group *columns
      if columns.empty?
        columns = @headers
      else
        columns.map!(&:to_sym)
      end

      columns.each do |column_name|
        raise MissingColumn,
          "Column '#{column_name}' was not found" unless @headers.include? column_name
      end
      
      distinct_values = {}

      @data.each do |row|
        key = columns.map { |column| row[column] }

        unless distinct_values[key]
          distinct_values[key] = group_row = {}
          columns.each { |column| group_row[column] = row[column] }
          group_row[:group] = []
        end

        distinct_values[key][:group] << row
      end

      output = []

      distinct_values.each do |key, row|
        row[:group] = Datastream.new row[:group]
        output << row
      end

      Datastream.new output
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
