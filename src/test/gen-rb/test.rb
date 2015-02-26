require "hbase"
require "securerandom"
require "date"

socket = Thrift::Socket.new('localhost', 9090)
transport = Thrift::BufferedTransport.new(socket)
transport.open
protocol = Thrift::BinaryProtocol.new(transport)
client = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(protocol)

puts client

puts client.getTableNames

row = client.getRow("test3", "9=5441a31ba9848977~6=7171941600256897438", nil)
puts "row = #{row.inspect}"
puts "row[0].row = #{row[0].row.inspect}"
puts "row[0].columns = #{row[0].columns.inspect}"
return

now = DateTime.now.to_s

BATCH_SIZE = 500

batch_mutations = []
start_time = Time.now
elap = 0
ops_per_sec = 0

def make_secure_hash(size)
   hash = SecureRandom.base64(size)
   hash = hash.gsub("+", "_")
   hash = hash.gsub("/", "-")
   hash = hash.gsub("=", "")
   hash
end

(1..100*1000*1000).each do |x|
   src_hash = make_secure_hash(16)
   dest_hash = make_secure_hash(16)
   key = src_hash + "~" + dest_hash
   value = "#{x}-#{now}"
   mutation = Apache::Hadoop::Hbase::Thrift::Mutation.new({:column => "f1:a", :value => value, :writeToWAL => false })
   batch_mutation = Apache::Hadoop::Hbase::Thrift::BatchMutation.new({:row => key, :mutations => [mutation]})
   batch_mutations << batch_mutation
   if batch_mutations.size >= BATCH_SIZE
      client.mutateRows("test3", batch_mutations, nil)
      batch_mutations = []   # TODO:  Flush at end of loop
      elap = Time.now - start_time
      ops_per_sec = x / elap
      puts "%6d %6.1f %5.0f %30s %s" % [x, elap, ops_per_sec, key, value]
   end
   # result = client.mutateRow("test3", key, [mutation], nil)
   # puts "%6d %6.1f %5.0f %30s %s" % [x, elap, ops_per_sec, key, value]
end





