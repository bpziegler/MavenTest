require "json"

# These outputs come from AnalyzeGuidScores.java

num_guid = 40312887

keys = JSON.parse(IO.read("keys.json"))
counts = JSON.parse(IO.read("counts.json"))
cond_counts = JSON.parse(IO.read("cond_counts.json"))

#keys.each_with_index do |key, idx|
#  times_key_occurs = counts[idx]
#  percent = (0.0 + times_key_occurs) / num_guid
#  puts "#{key} = #{counts[idx]}   #{(percent * 100).round(1)}%"
#  
#  keys.each_with_index do |other_key, other_idx|
#    times_other_key_occurs = cond_counts[idx][other_idx]
#    after_percent = (0.0 + times_other_key_occurs) / times_key_occurs
#    before_percent = (0.0 + counts[other_idx]) / num_guid
#    puts "   #{other_key} = #{(after_percent * 100).round(1)}%  vs  #{(before_percent * 100).round(1)}%"
#  end
#end


keys.each_with_index do |key, idx|
  # For all the other keys, find out which ones are most likely to co-occur with this key
  ary = []
  keys.each_with_index do |other_key, other_idx|
    times_key_occurs_with_other = cond_counts[other_idx][idx]
    percent_key_occurs_with_other = ((0.0 + times_key_occurs_with_other) / counts[other_idx] * 100).round(1)
    ary << [other_key, percent_key_occurs_with_other]
  end
  
  ary.sort! {|a,b| b[1] <=> a[1] }
    
  times_key_occurs = counts[idx]
  percent = (0.0 + times_key_occurs) / num_guid
  puts "#{key} = #{counts[idx]}   #{(percent * 100).round(1)}%"
  
  ary.each do |vals|
    other_key = vals[0]
    percent = vals[1]
    puts "    %6.1f %%   %-25s" % [percent, other_key]
  end    
end