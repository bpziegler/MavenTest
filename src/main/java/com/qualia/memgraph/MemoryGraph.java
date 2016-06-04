package com.qualia.memgraph;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Logger;

/*
 * TODO:
 * 
 * - Check if we will exceed java array max size (2^31)
 * 
 */

public class MemoryGraph {

    private static final Logger LOG = Logger.getLogger(MemoryGraph.class.getName());

    private static final int MAX_SET_SIZE = 4 * 1024 * 1024;

    private TreeSet<MemPair> memSet = new TreeSet<MemPair>();
    private TreeSet<MemPair> deletes = new TreeSet<MemPair>(); // TODO - support deletes

    // Using arrays of natives (longs) to be memory efficient (space and GC efficient)
    private long[] srcAry = new long[0];
    private long[] destAry = new long[0];

    public void add(long src, long dest) {
        if (contains(src, dest)) {
            return;
        }

        MemPair memPair = new MemPair(src, dest);
        memSet.add(memPair);

        if (memSet.size() >= MAX_SET_SIZE) {
            mergeMemorySetToArrays();
        }
    }

    public void mergeMemorySetToArrays() {
        // This currently creates two copies. In the future might be able to do an in-place merge to save memory.

        int aryLen = srcAry.length;
        int memSetLen = memSet.size();
        int totLen = srcAry.length + memSet.size();
        long[] newSrcAry = new long[totLen];
        long[] newDestAry = new long[totLen];

        long startTime = System.currentTimeMillis();

        int i = 0;
        int j = 0;
        int k = 0;
        MemPair curMemSet;

        Iterator<MemPair> iter = memSet.iterator();
        if (iter.hasNext()) {
            curMemSet = iter.next();
        } else {
            curMemSet = null;
        }

        while (i < srcAry.length && j < memSet.size()) {
            int comp = compareIdx(i, curMemSet.src, curMemSet.dest);

            if (comp < 0) {
                newSrcAry[k] = srcAry[i];
                newDestAry[k] = destAry[i];
                k++;
                i++;
            } else {
                newSrcAry[k] = curMemSet.src;
                newDestAry[k] = curMemSet.dest;
                k++;
                j++;
                if (iter.hasNext()) {
                    curMemSet = iter.next();
                } else {
                    curMemSet = null;
                }
            }
        }

        while (i < srcAry.length) {
            newSrcAry[k] = srcAry[i];
            newDestAry[k] = destAry[i];
            k++;
            i++;
        }

        while (j < memSet.size()) {
            newSrcAry[k] = curMemSet.src;
            newDestAry[k] = curMemSet.dest;
            k++;
            j++;
            if (iter.hasNext()) {
                curMemSet = iter.next();
            } else {
                curMemSet = null;
            }
        }

        srcAry = newSrcAry;
        destAry = newDestAry;
        memSet.clear();

        long elap = System.currentTimeMillis() - startTime;

        LOG.info(String.format("MB %,6d   Elap %,5d   Merged arrays %,d + %,d = %,d", getUsedMem() / (1024 * 1024), elap, aryLen,
                memSetLen, totLen));

    }

    public boolean contains(long src, long dest) {
        boolean found = binarySearchArrays(src, dest) >= 0;

        if (found) {
            return true;
        }

        return memSet.contains(new MemPair(src, dest));
    }

    private int binarySearchArrays(long src, long dest) {
        int left = 0;
        int right = srcAry.length - 1;

        while (true) {
            if (left > right)
                return -left - 1; // TODO: Test this!!

            int mid = (right - left) / 2 + left;

            int comp = compareIdx(mid, src, dest);
            if (comp < 0) {
                left = mid + 1;
            } else if (comp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
    }

    public static long getUsedMem() {
        long maxMem = Runtime.getRuntime().totalMemory();
        long freeMem = Runtime.getRuntime().freeMemory();
        return maxMem - freeMem;
    }

    private int compareIdx(int idx, long src, long dest) {
        int c1 = Long.compare(srcAry[idx], src);
        if (c1 != 0) {
            return c1;
        }
        return Long.compare(destAry[idx], dest);
    }
    
    public int size() {
        return srcAry.length + memSet.size();
    }

    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s  %5$s%6$s%n");

        LOG.info("Started");
        long startTime = System.currentTimeMillis();
        MemoryGraph memGraph = new MemoryGraph();

        for (long i = 0; i < 8000; i++) {
            for (long j = 0; j < 8000; j++) {
                memGraph.add(i, j);
            }
        }

        memGraph.mergeMemorySetToArrays();

        // for (int i = 0; i < 100; i++) {
        // LOG.info(String.format("%d %d", memGraph.srcAry[i], memGraph.destAry[i]));
        // }

        System.gc();
        LOG.info("Mem Used MB = " + getUsedMem() / (1024 * 1024));

        long elap = System.currentTimeMillis() - startTime;
        LOG.info("Finished   elap " + elap);
    }

}
