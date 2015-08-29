cookie_dirs = `s3cmd ls s3://localresponse/omnivore/cookie_monster/ | tail -n 31 | awk ' { print $2 } '`.split("\n")
# puts cookie_dirs.inspect

cookie_dirs.each do |dir|
  parts = dir.split("/")
  date = parts[parts.size-1]
  `mkdir -p cookies/#{date}`
  cmd = "s3cmd sync #{dir} cookies/#{date}/"
  puts cmd
end
