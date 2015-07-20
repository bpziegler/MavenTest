package com.qualia.test;


import gnu.trove.map.hash.TLongObjectHashMap;


public class TestMemoryMap {

    public static void main(String[] args) {
        final int NUM = 64 * 1000 * 1000;
        final TLongObjectHashMap<String> longObjectMap = new TLongObjectHashMap<String>();

        long start = System.currentTimeMillis();
        System.out.println("Start = " + start);

        for (int i = 0; i < NUM; i++) {
            longObjectMap.put(i, Integer.toString(i));
            if ((i & 0xFFFF) == 0) {
                long elap = System.currentTimeMillis() - start;
                Runtime runtime = Runtime.getRuntime();
                double usedMemory = (0.0 + runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                System.out.println(String.format("Num %,12d   Max %,12d   ElapMS %,6d   Mem %6.1f", i, NUM, elap, usedMemory));
            }
        }

        long elap = System.currentTimeMillis() - start;
        System.out.println("Elap = " + elap);
    }

}
