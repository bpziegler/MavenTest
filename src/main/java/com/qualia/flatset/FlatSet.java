package com.qualia.flatset;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;


public class FlatSet {

    private static final int MAX_MEMORYSET_SIZE = 64 * 1024;
    private static final int BYTES_PER_LONG = 8;
    private static final int LONGS_PER_KEY = 2;

    private final Set<LongWrapper> memorySet = new TreeSet<LongWrapper>();
    private long totArrayBytes = 0;
    private final ArrayList<long[]> arrays = new ArrayList<long[]>();

    private final ByteArrayOutputStream bs = new ByteArrayOutputStream();
    private final DataOutputStream ds = new DataOutputStream(bs);


    public void joinIds(long id1, long id2) {
        // Maintain a memory set until the set gets to a certain size,
        // then flush the memory set to a packed and sorted array.

        long[] longs = new long[2];
        longs[0] = id1;
        longs[1] = id2;

        memorySet.add(new LongWrapper(longs));
        if (memorySet.size() >= MAX_MEMORYSET_SIZE) {
            flushMemorySetToArrays();
            memorySet.clear();
        }
    }


    private void flushMemorySetToArrays() {
        long[] longArray = new long[memorySet.size() * LONGS_PER_KEY];

        int pos = 0;
        for (LongWrapper byteWrapper : memorySet) {
            long[] longs = byteWrapper.getLongs();
            for (int i = 0; i < longs.length; i++) {
                longArray[pos + i] = longs[i];
            }
            pos += longs.length;
        }

        addNewLongArray(longArray);
    }


    private void addNewLongArray(long[] longArray) {
        // System.out.println("Adding byteArray " + byteArray.length);
        totArrayBytes += longArray.length * BYTES_PER_LONG;
        arrays.add(longArray);
    }


    public byte[] getBytes(long id1, long id2) {
        try {
            byte[] result = null;
            bs.reset();
            ds.writeLong(id1);
            ds.writeLong(id2);
            ds.flush();
            result = bs.toByteArray();
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void dumpStats(long startTime, FlatSet flatSet, int i) {
        long elap = System.currentTimeMillis() - startTime;
        Runtime runtime = Runtime.getRuntime();
        double usedMem = (0.0 + runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        System.out.println(String.format("Elap %6.3f  Iter %,12d   Used MB %6.1f   Array MB %6.1f   Num Array %,5d",
                elap / 1000.0, i, usedMem, (0.0 + flatSet.totArrayBytes) / (1024.0 * 1024), flatSet.arrays.size()));
    }


    public static void main(String[] args) {
        System.out.println("Starting");
        long startTime = System.currentTimeMillis();

        final int NUM_TEST = 16 * 1024 * 1024;
        Random random = new Random();

        FlatSet flatSet = new FlatSet();

        for (int i = 0; i < NUM_TEST; i++) {
            long id1 = random.nextLong();
            long id2 = random.nextLong();

            flatSet.joinIds(id1, id2);
            flatSet.joinIds(id2, id1);

            if (i % 1000000 == 0) {
                dumpStats(startTime, flatSet, i);
            }
        }

        long elap = System.currentTimeMillis() - startTime;
        dumpStats(startTime, flatSet, NUM_TEST);
        System.out.println("Done");
    }

}
