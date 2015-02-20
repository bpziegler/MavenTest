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

now = DateTime.now.to_s

BATCH_SIZE = 100

batch_mutations = []
start_time = Time.now
elap = 0
ops_per_sec = 0

(1..100*1000*1000).each do |x|
   hash = SecureRandom.base64(64)
   hash = hash.gsub("+", "_")
   hash = hash.gsub("/", "-")
   hash = hash.gsub("=", "")
   key = "#{x}-#{now}"
   value = "value-#{x}-#{hash}"
   mutation = Apache::Hadoop::Hbase::Thrift::Mutation.new({:column => "f1:a", :value => value})
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




