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

    def join datastream
      duplicate.join! datastream
    end

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

    def union_all datastream
      duplicate.union_all! datastream
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
