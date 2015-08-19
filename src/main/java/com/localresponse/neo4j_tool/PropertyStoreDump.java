package com.localresponse.neo4j_tool;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyDumper;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;


public class PropertyStoreDump {

    private final int MB = 1024 * 1024;
    private final Runtime runtime = Runtime.getRuntime();

    private long lastLog = 0;
    private long used = 0;
    private Map<Integer, String> propertyKeyMap;


    private void dump(String dir) throws IOException {
        File storeFile = new File(dir, "neostore.propertystore.db");
        File propertyKeyFile = new File(dir, "neostore.propertystore.db.index");

        propertyKeyMap = PropertyKeyDumper.getPropertyKeyMap(propertyKeyFile);
        System.out.println("labelMap = " + propertyKeyMap.toString());

        StoreFactory storeFactory = new StoreFactory(new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM, null);
        PropertyStore store = storeFactory.newPropertyStore(storeFile);
        store.makeStoreOk();
        int size = store.getRecordSize();

        BufferedWriter bw = createFile("properties.txt");

        System.out.println("store.getRecordSize() = " + size);
        long startTime = System.currentTimeMillis();
        long maxId = store.getHighestPossibleIdInUse();
        for (long i = 0; i <= maxId; i++) {
            PropertyRecord record = store.forceGetRecord(i);
            if (record.inUse()) {
                used++;
                dumpRecord(bw, store, record);
            }

            long now = System.currentTimeMillis();
            if (now - lastLog >= 250) {
                lastLog = now;
                dumpStats(startTime, i, maxId);
            }
        }

        bw.close();

        dumpStats(startTime, maxId, maxId);
        System.out.println("done");
    }


    private void dumpRecord(BufferedWriter bw, PropertyStore store, PropertyRecord record) throws IOException {
        long nextPropertyId = record.getNextProp();
        List<PropertyBlock> blocks = record.getPropertyBlocks();
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (PropertyBlock oneBlock : blocks) {
            PropertyType type = oneBlock.forceGetType();
            Object value = type.getValue( oneBlock, null );
            String keyStr = propertyKeyMap.get((int) oneBlock.getKeyIndexId());
            sb.append(keyStr);
            sb.append("=");
            if (type.equals(PropertyType.SHORT_STRING) || type.equals(PropertyType.STRING) ) {
                sb.append("\"");
                sb.append(value);
                sb.append("\"");
            } else {
                sb.append(value);
            }
            if (i+1 < blocks.size()) {
                sb.append(",");
            }
            i++;
        }
        String line = String.format("%d,%d,%s\n", record.getId(), nextPropertyId, sb.toString());
        bw.write(line);
    }


    private BufferedWriter createFile(String path) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter bw = new BufferedWriter(osw);
        return bw;
    }


    private void dumpStats(long startTime, long cur, long maxId) {
        double elap = (0.0 + System.currentTimeMillis() - startTime) / 1000;
        double curRate = cur / elap;
        double remainTime = (curRate > 0) ? (maxId - cur) / curRate : 0;
        String durStr = DurationFormatUtils.formatDuration((long) remainTime * 1000, "H:mm:ss");
        long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / MB;

        String status = String.format(
                "Property %,12d   Elap %,8.1f   Remain %8s   %7.3f %%   MB %,8d   Line/Sec %,8.0f   Used %,12d", cur, elap,
                durStr, 100.0 * cur / maxId, usedMB, curRate, used);

        System.out.println(status);

    }


    public static void main(String[] args) throws IOException {
        String dir = "/Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";
        if (args.length > 0) {
            dir = args[0];
        }
        System.out.println("graph dir = " + dir);

        PropertyStoreDump program = new PropertyStoreDump();
        program.dump(dir);
    }

}
