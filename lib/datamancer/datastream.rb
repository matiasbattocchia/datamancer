class Array
  
  def where criteria
    self.select do |row|
      result = true

      criteria.each do |field, value|
        unless row[field] == value
          result = false
          break
        end
      end

      result
    end
  end

end
