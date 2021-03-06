package com.localresponse.neo4j_tool;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeDumper;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;


public class RelationshipStoreDump {

    private final int MB = 1024 * 1024;
    private final Runtime runtime = Runtime.getRuntime();

    private long lastLog = 0;
    private long used = 0;
    private Map<Integer, String> typeMap;


    private void dump(String dir) throws IOException {
        File storeFile = new File(dir, "neostore.relationshipstore.db");
        File labelFile = new File(dir, "neostore.relationshiptypestore.db");

        typeMap = RelationshipTypeDumper.getTypeMap(labelFile);
        System.out.println("typeMap = " + typeMap.toString());

        StoreFactory storeFactory = new StoreFactory(new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM, null);
        RelationshipStore store = storeFactory.newRelationshipStore(storeFile);
        store.makeStoreOk();
        int size = store.getRecordSize();

        BufferedWriter bw = createFile("relationships.txt");

        System.out.println("store.getRecordSize() = " + size);
        long startTime = System.currentTimeMillis();
        long maxId = store.getHighestPossibleIdInUse();
        for (long i = 0; i <= maxId; i++) {
            RelationshipRecord record = store.forceGetRecord(i);
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


    private void dumpRecord(BufferedWriter bw, RelationshipStore store, RelationshipRecord record) throws IOException {
        long node1 = record.getFirstNode();
        long node2 = record.getSecondNode();
        long type = record.getType();
        String typeStr = typeMap.get((int) type);
        String line = String.format("%d,%d,%d,%s,%d\n", record.getId(), node1, node2, typeStr, record.getNextProp());
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
                "Relationship %,12d   Elap %,8.1f   Remain %8s   %7.3f %%   MB %,8d   Line/Sec %,8.0f   Used %,12d", cur, elap,
                durStr, 100.0 * cur / maxId, usedMB, curRate, used);

        System.out.println(status);

    }


    public static void main(String[] args) throws Exception {
        String dir = "//Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";
        if (args.length > 0) {
            dir = args[0];
        }
        System.out.println("graph dir = " + dir);

        RelationshipStoreDump program = new RelationshipStoreDump();
        program.dump(dir);
    }

}
