package com.qualia.cookie;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
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
    private int numCompactThreads;
    private int numFlushThreads;
    private int bulkLoad;


    public PutThread(BlockingQueue<Object> queue, AtomicBoolean doneFlag, AtomicInteger completedFiles, int maxFiles,
            int numCompactThreads, int numFlushThreads, int bulkLoad) {
        this.queue = queue;
        this.doneFlag = doneFlag;
        this.completedFiles = completedFiles;
        this.maxFiles = maxFiles;
        this.numCompactThreads = numCompactThreads;
        this.numFlushThreads = numFlushThreads;
        this.bulkLoad = bulkLoad;
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
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        options.setCreateIfMissing(true);
        if (bulkLoad != 0) {
            options.prepareForBulkLoad();
        }
        options.setIncreaseParallelism(numCompactThreads + numFlushThreads);
        options.setMaxBackgroundCompactions(numCompactThreads);
        options.setMaxBackgroundFlushes(numFlushThreads);
        RocksDB db = RocksDB.open(options, "test-db");

        WriteBatch writeBatch = new WriteBatch();
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.setDisableWAL(true);
        int numBatch = 0;

        while (true) {
            Object obj = queue.poll();
            if (obj == null) {
                if (doneFlag.get() && queue.size() == 0) // Note we check the queue size AGAIN for thread safety
                    break;
            } else if (obj instanceof PutKeyValue) {
                num++;
                PutKeyValue putKeyVal = (PutKeyValue) obj;

                writeBatch.put(putKeyVal.getKey(), putKeyVal.getVal());
                numBatch++;

                if (numBatch % 1000 == 0) {
                    db.write(writeOptions, writeBatch);
                    writeBatch.dispose();
                    numBatch = 0;
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

        if (bulkLoad != 0) {
            System.out.println("Major compaction");
            db.compactRange();
        }

        System.out.println("Closing");
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
