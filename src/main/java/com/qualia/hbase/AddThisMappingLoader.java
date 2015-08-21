package com.qualia.hbase;


import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import com.google.common.base.Splitter;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class AddThisMappingLoader {

    private TSocket transport;
    private Client client;
    private ObjectMapper mapper = new ObjectMapper();
    private Splitter tabSplitter = Splitter.on("\t");
    private Splitter commaSplitter = Splitter.on(",");
    private Splitter equalSplitter = Splitter.on("=");
    private HashMap<String, String> pidCodes = new HashMap<String, String>();
    private HashMap<String, ArrayList<BatchMutation>> tableToBatchMap = new HashMap<String, ArrayList<BatchMutation>>();
    private TableBatch propsBatch;
    private TableBatch mappingBatch;

    private static class TableBatch {
        private final Table table;
        private final ArrayList<Put> puts = new ArrayList<>();


        public TableBatch(Table table) {
            this.table = table;
        }


        public void put(byte[] row, byte[] family, byte[] col, byte[] value) {
            Put put = new Put(row);
            put.addColumn(family, col, value);
            puts.add(put);
        }


        public void flush(int minSize) throws IOException, InterruptedException {
            if (puts.size() >= minSize) {
                Object[] results = new Object[puts.size()];
                table.batch(puts, results);
                puts.clear();
            }
        }
    }


    // Helper to translate strings to UTF8 bytes
    private static byte[] bytes(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }


    public AddThisMappingLoader() {
        pidCodes.put("6", "adnxs");
        pidCodes.put("9", "fat");
        pidCodes.put("11127", "ltm");
    }


    private void load(String addThisMappingFile) throws Exception {
        initTransport();

        List<File> list = new ArrayList<File>();
        list.add(new File(addThisMappingFile));

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                try {
                    AddThisMappingLoader.this.processLine(line, curLine);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }


            public String getStatus() {
                return null;
            }
        });

        propsBatch.flush(1);
        mappingBatch.flush(1);

        transport.close();
    }


    private void initTransport() throws TTransportException, IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");

        Connection connection = ConnectionFactory.createConnection(config);
        Table propsTable = connection.getTable(TableName.valueOf("props"));
        Table mappingTable = connection.getTable(TableName.valueOf("mapping"));

        propsBatch = new TableBatch(propsTable);
        mappingBatch = new TableBatch(mappingTable);
    }


    protected void processLine(String line, long curLine) throws Exception {
        List<String> parts = tabSplitter.splitToList(line);
        String timestampStr = parts.get(0);
        DateTime dt = new DateTime(Long.valueOf(timestampStr));
        String dtStr = dt.toString();
        // System.out.println(dt);
        String lr_uid = parts.get(1);
        String pidUidPairsList = parts.get(2);
        List<String> pidUidPairs = commaSplitter.splitToList(pidUidPairsList);
        String lrDeviceKey = "pu_" + "lr_" + lr_uid;
        setKeyValBatch(propsBatch, lrDeviceKey, "f1", "last_seen", dtStr);

        ArrayList<String> mappings = new ArrayList<String>();
        mappings.add(lrDeviceKey);
        for (String pidUidPair : pidUidPairs) {
            List<String> pidUidParts = equalSplitter.splitToList(pidUidPair);
            String pidCode = pidUidParts.get(0);
            String pid = pidCodes.get(pidCode);
            String uid = pidUidParts.get(1);
            if (uid == "0" || uid == "-1") {
                continue;
            }

            String mappedDeviceKey = "pu_" + pid + "_" + uid;
            setKeyValBatch(propsBatch, mappedDeviceKey, "f1", "last_seen", dtStr);
            mappings.add(mappedDeviceKey);
        }

        // We want to spread out the distribution of "fat" rows (the one used first), to help partioning
        Collections.shuffle(mappings);
        String src = mappings.get(0);
        for (int i = 1; i < mappings.size(); i++) {
            String dest = mappings.get(i);
            setKeyValBatch(mappingBatch, src, "f1", dest, "");
            setKeyValBatch(mappingBatch, dest, "f1", src, "");
        }
    }


    private void setKeyValBatch(TableBatch batch, String rowKey, String family, String colName, String colValue)
            throws IOException, InterruptedException {
        batch.put(bytes(rowKey), bytes(family), bytes(colName), bytes(colValue));
        batch.flush(1000);
    }


    public static void main(String[] args) throws Exception {
        AddThisMappingLoader loader = new AddThisMappingLoader();
        String path = (args.length > 0) ? args[0] : null;
        if (path == null) {
            path = "test_data/batch-uids-localresponse-150627_20150628065001";
        }
        loader.load(path);
    }
}
