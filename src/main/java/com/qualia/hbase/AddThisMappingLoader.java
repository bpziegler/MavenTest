package com.qualia.hbase;


import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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


    // Helper to translate strings to UTF8 bytes
    private static ByteBuffer bytes(String s) {
        try {
            return ByteBuffer.wrap(s.getBytes("UTF-8"));
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

        for (String tableName : tableToBatchMap.keySet()) {
            ArrayList<BatchMutation> batchMutations = tableToBatchMap.get(tableName);
            if (batchMutations.size() >= 1) {
                flushBatch(tableName, batchMutations);
            }
        }

        transport.close();
    }


    private void initTransport() throws TTransportException {
        transport = new TSocket("localhost", 9090);
        TBinaryProtocol protocol = new TBinaryProtocol(transport, true, true);
        client = new Hbase.Client(protocol);
        transport.open();
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
        setKeyValBatch("props", lrDeviceKey, "f1:last_seen", dtStr);
        for (String pidUidPair : pidUidPairs) {
            List<String> pidUidParts = equalSplitter.splitToList(pidUidPair);
            String pidCode = pidUidParts.get(0);
            String pid = pidCodes.get(pidCode);
            String uid = pidUidParts.get(1);
            if (uid == "0" || uid == "-1") {
                continue;
            }

            String mappedDeviceKey = "pu_" + pid + "_" + uid;
            setKeyValBatch("props", mappedDeviceKey, "f1:last_seen", dtStr);

            setKeyValBatch("mapping", lrDeviceKey, "f1:" + mappedDeviceKey, "");
            setKeyValBatch("mapping", mappedDeviceKey, "f1:" + lrDeviceKey, "");
        }

    }


    private void setKeyValBatch(String tableName, String rowKey, String colName, String colValue) {
        Mutation mutation = new Mutation(false, bytes(colName), bytes(colValue), false);
        ArrayList<Mutation> mutationList = new ArrayList<Mutation>();
        mutationList.add(mutation);
        BatchMutation batchMutation = new BatchMutation(bytes(rowKey), mutationList);

        ArrayList<BatchMutation> batchMutations = tableToBatchMap.get(tableName);
        if (batchMutations == null) {
            batchMutations = new ArrayList<BatchMutation>();
            tableToBatchMap.put(tableName, batchMutations);
        }
        batchMutations.add(batchMutation);

        if (batchMutations.size() >= 1000) {
            flushBatch(tableName, batchMutations);
        }
    }


    private void flushBatch(String tableName, ArrayList<BatchMutation> batchMutations) {
        int numTry = 0;

        while (true) {
            try {
                client.mutateRows(bytes(tableName), batchMutations, null);
                batchMutations.clear();
                break;
            } catch (Exception e) {
                numTry++;
                System.out.println("NumTry = " + numTry + "   Exception:  " + e.getMessage());
                try {
                    Thread.sleep(numTry * 5);
                    initTransport();
                } catch (Exception e1) {
                }
            }
        }
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
