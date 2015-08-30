require "json"

ARGF.each do |line|
  line.chomp!
  json = JSON.parse(line)
  puts json["user-agent"]
end
