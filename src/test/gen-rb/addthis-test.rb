require "hbase"
require "securerandom"
require "date"


class AddThisLoader

  BATCH_SIZE = 500
  LOG_INTERVAL = 0.5
  MIN_LINES_LOG = 100
  def initialize
    socket = Thrift::Socket.new('localhost', 9090)
    transport = Thrift::BufferedTransport.new(socket)
    transport.open
    protocol = Thrift::BinaryProtocol.new(transport)
    @client = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(protocol)
    puts "TableNames = " + @client.getTableNames.inspect
  end


  def load_addthis(path)
    now = DateTime.now.to_s

    lastLog = Time.now
    @start_time = Time.now
    @line_num = 0
    @cur_bytes = 0
    @tot_bytes = File.size(path)
    @batch_mutations = []

    f = File.open(path, "r")
    f.each_line do |line|
      @line_num = @line_num + 1
      @cur_bytes = @cur_bytes + line.length

      process_line(line.strip)

      if (@line_num % MIN_LINES_LOG == 0) && (Time.now - lastLog >= LOG_INTERVAL)
        lastLog = Time.now
        output_status()
      end
    end

    f.close
  end


  def process_line(line)
    parts = line.split("\t")
    cookie_parts = parts[2]
    cookies = cookie_parts.split(",")

    return if cookies.size != 2
    
    cookies.each do |cookie|
      return if cookie == "0"
      return if cookie == "-1"
    end

    value = Time.now.to_i.to_s

    key1 = cookies[0] + "~" + cookies[1]
    put_key_value(key1, value)

    key2 = cookies[1] + "~" + cookies[0]
    put_key_value(key2, value)
  end


  def put_key_value(key, value)
    mutation = Apache::Hadoop::Hbase::Thrift::Mutation.new({:column => "f1:a", :value => value, :writeToWAL => false })
    batch_mutation = Apache::Hadoop::Hbase::Thrift::BatchMutation.new({:row => key, :mutations => [mutation]})
    @batch_mutations << batch_mutation
    if @batch_mutations.size >= BATCH_SIZE
      @client.mutateRows("test3", @batch_mutations, nil)
      @batch_mutations = []   # TODO:  Flush at end of loop
    end
  end


  def output_status()
    elap_sec = Time.now - @start_time
    bytes_per_sec = elap_sec > 0 ? @cur_bytes / elap_sec : 0
    remain_sec = bytes_per_sec > 0 ? (@tot_bytes - @cur_bytes) / bytes_per_sec : 0;
    per_done = 100.0 * @cur_bytes / @tot_bytes;
    line_per_sec = elap_sec > 0 ? @line_num / elap_sec : 0;

    line_status = "Line %7d   Done%% %5.1f   Elap %4.0f   Remain %4.0f   Line/sec %4.0f   %s" % [@line_num, per_done, elap_sec, remain_sec, line_per_sec, ""]

    puts(line_status)
  end
end


loader = AddThisLoader.new
loader.load_addthis("/Users/benziegler/test_data/addthis_map_test.txt")
