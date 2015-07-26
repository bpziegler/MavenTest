package com.qualia.cookie;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;


public class PutThread implements Runnable {

    private final AtomicBoolean doneFlag;
    private final BlockingQueue<Object> queue;
    private final AtomicInteger completedFiles;
    private final int maxFiles;


    public PutThread(BlockingQueue<Object> queue, AtomicBoolean doneFlag, AtomicInteger completedFiles, int maxFiles) {
        this.queue = queue;
        this.doneFlag = doneFlag;
        this.completedFiles = completedFiles;
        this.maxFiles = maxFiles;
    }


    @Override
    public void run() {
        try {
            runWorker();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void runWorker() throws RocksDBException {
        long num = 0;
        long start = System.currentTimeMillis();

        RocksDB.loadLibrary();
        Options options = new Options();
        // options.prepareForBulkLoad();
        options.setCreateIfMissing(true);
        options.setIncreaseParallelism(8);
        options.setMaxBackgroundCompactions(4);
        options.setMaxBackgroundFlushes(4);
        RocksDB db = RocksDB.open(options, "test-db");

        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.setDisableWAL(true);
        
        WriteBatch writeBatch = new WriteBatch();
        int numBatch = 0;

        while (true) {
            Object obj = queue.poll();
            if (obj == null) {
                if (doneFlag.get())
                    break;
            } else if (obj instanceof PutKeyValue) {
                num++;
                PutKeyValue putKeyVal = (PutKeyValue) obj;

                writeBatch.put(putKeyVal.getKey(), putKeyVal.getVal());
                numBatch++;

                if (numBatch % 1000 == 0) {
                    db.write(writeOptions, writeBatch);
                    numBatch = 0;
                    writeBatch.dispose();
                    writeBatch = new WriteBatch();
                }

                if (num % 50000 == 0) {
                    dumpStats(num, start);
                }
            } else {
                throw new RuntimeException("Unknown object type");
            }
        }

        if (numBatch > 0) {
            db.write(writeOptions, writeBatch);
        }
        
//        System.out.println("Compacting");
//        db.compactRange();

        db.close();
        dumpStats(num, start);
    }


    private void dumpStats(long num, long start) {
        double elap = (0.0 + System.currentTimeMillis() - start) / 1000.0;
        double putPerSec = (num + 0.0) / elap;
        double memUsed = (0.0 + Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
        System.out.println(String.format("File %,6d of %,6d   Put %,12d   Elap %,8.1f   Put/Sec %,8.0f   MB %6.0f",
                completedFiles.get(), maxFiles, num, elap, putPerSec, memUsed));
    }

}
