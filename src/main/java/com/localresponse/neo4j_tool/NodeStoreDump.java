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
import org.neo4j.kernel.impl.nioneo.store.LabelDumper;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;
import org.neo4j.kernel.impl.util.StringLogger;


public class NodeStoreDump {

    private final int MB = 1024 * 1024;
    private final Runtime runtime = Runtime.getRuntime();

    private long lastLog = 0;
    private long used = 0;
    private Map<Integer, String> labelMap;


    public void dump(File nodeStoreFile, File labelFile) throws IOException {
        labelMap = LabelDumper.getLabelMap(labelFile);
        System.out.println("labelMap = " + labelMap.toString());

        StoreFactory storeFactory = new StoreFactory(new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM, null);
        NodeStore store = storeFactory.newNodeStore(nodeStoreFile);
        store.makeStoreOk();
        int size = store.getRecordSize();

        BufferedWriter bw = createFile("nodes.txt");

        System.out.println("store.getRecordSize() = " + size);
        long startTime = System.currentTimeMillis();
        long maxId = store.getHighestPossibleIdInUse();
        for (long i = 1; i <= maxId; i++) {
            NodeRecord record = store.forceGetRecord(i);
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


    private void dumpRecord(BufferedWriter bw, NodeStore store, NodeRecord record) throws IOException {
        NodeLabels labels = NodeLabelsField.parseLabelsField(record);
        long[] labelIds = labels.getIfLoaded();
        String label = labelMap.get((int) labelIds[0]);
        String line = String.format("%d,%s\n", record.getId(), label);
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
                "Line %,12d   Elap %,8.1f   Remain %8s   %7.3f %%   MB %,8d   Line/Sec %,8.0f   Used %,12d", cur, elap,
                durStr, 100.0 * cur / maxId, usedMB, curRate, used);

        System.out.println(status);

    }


    private void getLabels() {
    }


    public static void main(String[] args) throws IOException {
        String dir = "//Users/benziegler/work/neo4j-community-2.1.8/data/graph.db";

        File file = null;
        File labelFile;
        if (args.length > 0) {
            file = new File(args[0]);
            labelFile = new File(args[1]);
        } else {
            file = new File(dir, "neostore.nodestore.db");
            labelFile = new File(dir, "neostore.labeltokenstore.db");
        }
        System.out.println("Using file:  " + file.getAbsolutePath());

        NodeStoreDump program = new NodeStoreDump();
        program.dump(file, labelFile);
    }

}
