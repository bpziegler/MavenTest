
def comma_num(num)
  num.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse
end

last_bytes = 0

while true
  bytes = `du test-db/ -d 0 | cut -f 1`.chomp.to_i
  diff = bytes - last_bytes
  last_bytes = bytes
  puts "%-25s   %12s KB    %12s diff" % [Time.now, comma_num(bytes), comma_num(diff)]
  sleep(2)
end
