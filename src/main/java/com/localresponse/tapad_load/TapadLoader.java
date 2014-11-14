package com.localresponse.tapad_load;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class TapadLoader {

    public enum GraphLabels implements Label {
        Cookie, Device, Entity, IP;
    }

    private final String graphDir;
    private final String tapadFile;
    private final GraphDatabaseService srcDb;
    private final TapadStats stats = new TapadStats();
    private ArrayList<String> list;
    private BlockingQueue<LoadTask> tasks = new ArrayBlockingQueue<LoadTask>(100);
    private AtomicBoolean done = new AtomicBoolean();
    private List<LoadThread> threads = new ArrayList<LoadThread>();


    public TapadLoader(String graphDir, String tapadFile) {
        this.graphDir = graphDir;
        this.tapadFile = tapadFile;
        this.srcDb = new GraphDatabaseFactory().newEmbeddedDatabase(graphDir);
        registerShutdownHook(this.srcDb);

        String numStr = System.getProperty("TapadLoad.numThread");
        int numThread = (numStr != null) ? Integer.valueOf(numStr) : 4;
        System.out.println("Num Threads (TapadLoad.numThread system prop) = " + numThread);

        for (int i = 0; i < numThread; i++) {
            LoadThread loadThread = new LoadThread(tasks, done);
            threads.add(loadThread);
            loadThread.start();
        }
    }


    private void load() throws Exception {
        List<File> list = new ArrayList<File>();
        list.add(new File(tapadFile));

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                try {
                    TapadLoader.this.processLine(line, curLine);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }


            public String getStatus() {
                return String.format("numHit %,9d   numCreate %,9d   numExistFail %,9d", stats.numHit.get(),
                        stats.numCreate.get(), stats.numConstraint.get());
            }
        });

        done.set(true);

        for (LoadThread loadThread : threads) {
            loadThread.join();
        }

        srcDb.shutdown();
    }


    protected void processLine(String line, long curLine) throws InterruptedException {
        if (list == null) {
            list = new ArrayList<String>();
        }

        list.add(line);
        if (list.size() >= 100) {
            LoadTask task = new LoadTask(srcDb, list, stats);
            list = null;
            tasks.put(task);
        }
    }


    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }


    public static void main(String[] args) throws Exception {
        String graphDir = "/Users/benziegler/work/neo4j-community-2.1.2/data/graph.db";
        String tapadFile = "/Users/benziegler/work/tapad/LocalResponse_ids_full_20140827_203357";

        if (args.length >= 2) {
            graphDir = args[0];
            tapadFile = args[1];
        }

        System.out.println("graphDir = " + graphDir);
        System.out.println("tapadFile = " + tapadFile);

        TapadLoader loader = new TapadLoader(graphDir, tapadFile);
        loader.load();
    }

}
