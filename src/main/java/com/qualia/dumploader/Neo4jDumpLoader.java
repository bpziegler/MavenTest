package com.qualia.dumploader;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.rocksdb.RocksDBException;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.qualia.keystore_graph.KeyStoreTable;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


/*
 * Loads a neo4j "dump" (nodes.txt, relationships.txt, properties.txt) into RocksDB.
 * This first version is only loading the properties to fix those so we can get all properties mapped to the first property id.
 */
public class Neo4jDumpLoader {

    private final KeyStoreTable propMapping;
    private final KeyStoreTable propValues;
    private final Splitter commaSplitter = Splitter.on(",");
    private final Joiner commaJoiner = Joiner.on(",");


    public Neo4jDumpLoader() {
        propMapping = new KeyStoreTable("property-mapping", true, false);
        propValues = new KeyStoreTable("property-values", true, false);
    }


    public void load() throws IOException {
        File propFile = new File("properties.txt");
        ArrayList<File> files = new ArrayList<File>();
        files.add(propFile);

        MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
        lineProcessor.processFiles(files, new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                try {
                    Neo4jDumpLoader.this.processLine(line, curLine);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }


            @Override
            public String getStatus() {
                return null;
            }
        });

        System.out.println("Closing properties mapping table");
        propMapping.close();
        System.out.println("Closing properties values table");
        propValues.close();
        System.out.println("Finished");
    }


    protected void processLine(String line, long curLine) throws Exception {
        List<String> parts = commaSplitter.splitToList(line);
        long propId = Long.valueOf(parts.get(0));
        long nextPropId = Long.valueOf(parts.get(1));
        List<String> props = parts.subList(2, parts.size());
        String propsStr = commaJoiner.join(props);

        if (nextPropId != -1) {
            saveMapping(propId, nextPropId);
            saveMapping(nextPropId, propId);
        }

        propValues.put(KeyStoreTable.getBytesForValue(propId), propsStr);
    }


    private void saveMapping(long id1, long id2) throws Exception {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(bs);
        ds.writeLong(id1);
        ds.writeLong(id2);

        propMapping.put(bs.toByteArray(), "");
    }


    public static void main(String[] args) throws IOException {
        Neo4jDumpLoader program = new Neo4jDumpLoader();
        program.load();
    }

}
