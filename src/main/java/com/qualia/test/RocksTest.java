package com.qualia.test;


import java.util.Random;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;


public class RocksTest {

    private static final int NUM_TEST = 8 * 1000 * 1000;


    private static void dumpStats(long start, long i, String desc) {
        long elap = System.currentTimeMillis() - start;
        double put_per_sec = (i + 0.0) / (elap / 1000.0);
        System.out.println(String.format("Num = %,12d   Elap = %8.3f   %8s = %,12.0f", i, (elap + 0.0) / 1000.0,
                desc + "/sec", put_per_sec));
    }


    public static void main(String[] args) throws RocksDBException {
        RocksDB.loadLibrary();
        RocksDB db = RocksDB.open("test-db");

        System.out.println("Starting " + NUM_TEST);
        long start = System.currentTimeMillis();

        Random rand = new Random();
        long seed = rand.nextLong();

        for (int i = 0; i < NUM_TEST; i++) {
            long keyVal = seed ^ i;

            byte[] key = Long.toString(keyVal).getBytes();
            byte[] val = Integer.toString(i).getBytes();
            db.put(key, val);

            if (i % 100000 == 0) {
                dumpStats(start, i, "Put");
            }
        }
        dumpStats(start, NUM_TEST, "Put");

        System.out.println("start scan");
        start = System.currentTimeMillis();

        RocksIterator iter = db.newIterator();
        iter.seekToFirst();

        long num = 0;
        while (iter.isValid()) {
            num++;
            iter.next();
        }
        iter.dispose();
        dumpStats(start, num, "Scan");

        db.close();
    }

}
