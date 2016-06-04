package com.qualia.memgraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.qualia.keystore_graph.GlobalKey;

public class MemGraphMappingProcessor implements Runnable {

    private static final Logger LOG = Logger.getLogger(MemGraphMappingProcessor.class.getName());

    private static final int MAX_BATCH_SIZE = 100;

    private final ThreadLocal<MappingBatch> bufferThreadVar = new ThreadLocal<MappingBatch>() {
        @Override
        protected MappingBatch initialValue() {
            return new MappingBatch();
        }
    };

    private final BlockingQueue<MappingBatch> queue = new ArrayBlockingQueue<MappingBatch>(100);

    private final MemoryGraph memoryGraph = new MemoryGraph();
    private Thread thread;
    public AtomicBoolean done = new AtomicBoolean();
    private long numProcess = 0;

    public void addMapping(MappingList mapping) throws InterruptedException {
        bufferThreadVar.get().add(mapping);
        if (bufferThreadVar.get().size() >= MAX_BATCH_SIZE) {
            flush();
        }
    }

    private void processBuffer(MappingBatch mappingBatch) throws InterruptedException {
        queue.put(new MappingBatch(mappingBatch));
    }

    public void flush() throws InterruptedException {
        processBuffer(bufferThreadVar.get());
        bufferThreadVar.get().clear();
    }

    public void startThread() {
        thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        try {
            workerRun();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void workerRun() throws InterruptedException {
        while (true) {
            boolean canQuit = done.get();

            MappingBatch mappingBatch;
            while ((mappingBatch = queue.poll(10, TimeUnit.MILLISECONDS)) != null) {
                processMappingBatch(mappingBatch);
            }

            if (canQuit) {
                break;
            }
        }
    }

    private void processMappingBatch(MappingBatch mappingBatch) {
        for (MappingList mappingList : mappingBatch) {
            processMappingList(mappingList);
        }
    }

    private void processMappingList(MappingList mappingList) {
        List<GlobalKey> copy = new ArrayList<GlobalKey>(mappingList);
        Collections.shuffle(copy);

        GlobalKey first = copy.remove(0);
        for (GlobalKey oneOther : copy) {
            saveMappingPair(first, oneOther);
            saveMappingPair(oneOther, first);
        }

        numProcess++;
        if (numProcess % 100000 == 0) {
            String status = String.format("Processed %,12d", numProcess);
            // System.out.println(status);
        }
    }

    private void saveMappingPair(GlobalKey first, GlobalKey second) {
        memoryGraph.add(first.fastLong, second.fastLong);
    }

    public void join() throws InterruptedException {
        thread.join();
    }

}
