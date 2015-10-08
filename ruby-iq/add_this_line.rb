class AddThisLine
  def initialize(line)
    if line
      @line = line.chomp
      @parts = @line.split("\t")
    end
  end

  def line
    @line
  end

  def parts
    @parts
  end

  def self.compare(add_this_line1, add_this_line2)
    fail "both lines can't be nil" if add_this_line1.line.nil? && add_this_line2.line.nil?
    if add_this_line1.line.nil?
      return 1      # This will make the merge logic treat add_this_line2 as the line to use
    elsif add_this_line2.line.nil?
      return -1
    else
      add_this_line1.parts[0] <=> add_this_line2.parts[0]
    end
  end
end