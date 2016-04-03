package com.bziegler.bitset;

import java.util.HashSet;
import java.util.Set;

public class SpeedTest {

    public static void main(String[] args) {
        // Set<Integer> set = new BinaryArraySet<Integer>();
        Set<Integer> set = new HashSet<Integer>();

        final int NUM = 1000 * 1000;

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUM; i++) {
            set.add(i);
        }
        double elapSec = (0.0 + System.currentTimeMillis() - startTime) / 1000;
        double opsPerSec = (NUM + 0.0) / elapSec;
        System.out.println(String.format("Ops/Sec = %,12.1f", opsPerSec));
    }

}
