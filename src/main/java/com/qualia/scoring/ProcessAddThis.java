package com.qualia.scoring;


import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.rocksdb.RocksDBException;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.qualia.keystore_graph.KeyStoreTable;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class ProcessAddThis {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Splitter tabSplitter = Splitter.on("\t");
    private final MD5Helper md5Helper = new MD5Helper();
    private final KeyStoreTable urlTopicsTable = new KeyStoreTable("url_topics", true, false);
    private long numMatch;
    private long numMiss;


    private void run(String dirPath) throws IOException {

        MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
        lineProcessor.setUseGzip(true);
        lineProcessor.processDir(dirPath, new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                try {
                    ProcessAddThis.this.processLine(line, curLine);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


            @Override
            public String getStatus() {
                return String.format("match %,d   miss %,d", numMatch, numMiss);
            }
        });
    }


    protected void processLine(String line, long curLine) throws RocksDBException, JsonProcessingException, IOException {
        List<String> parts = tabSplitter.splitToList(line);
        String guid = parts.get(0);
        String url = parts.get(5);
        String id = md5Helper.stringToMD5Hex(url);
        byte[] keyBytes = KeyStoreTable.getBytesForValue(id);
        byte[] valBytes = urlTopicsTable.getDb().get(keyBytes);
        if (valBytes != null) {
            String valStr = new String(valBytes, Charsets.UTF_8);
            ArrayNode labelArray = (ArrayNode) mapper.readTree(valStr);
            System.out.println(guid + "   " + labelArray.toString());
            numMatch++;
        } else {
            numMiss++;
        }
    }


    public static void main(String[] args) throws IOException {
        ProcessAddThis program = new ProcessAddThis();
        program.run(args[0]);
    }

}
