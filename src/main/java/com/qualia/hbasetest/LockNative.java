package com.qualia.hbasetest;

import gnu.trove.set.hash.TLongHashSet;

public class LockNative {

    public static void main(String[] args) {
        final int SIZE = 1000 * 1000;

        TLongHashSet longSet = new TLongHashSet(SIZE);

        long startTime = System.currentTimeMillis();

        for (long i = 0; i < SIZE; i++) {
            longSet.add(i);
        }

        long elap = System.currentTimeMillis() - startTime;
        System.out.println(elap);
    }

}
