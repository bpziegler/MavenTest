package com.qualia.test;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class ShuffleTest {

    public static void main(String[] args) {
        System.out.println("start");

        final int NUM_RANGE = 3;
        final int NUM_PER_RANGE = 10 * 1000 * 1000;
        final int NUM = NUM_RANGE * NUM_PER_RANGE;

        List<Integer> list = new ArrayList<Integer>(NUM);

        for (int i = 0; i < NUM; i++) {
            list.add(i);
        }

        System.out.println("shuffle");
        Collections.shuffle(list);

        long lowSum = Long.MAX_VALUE;
        long highSum = Long.MIN_VALUE;

        for (int range = 0; range < 3; range++) {
            int low = NUM_PER_RANGE * range;
            int high = NUM_PER_RANGE * (range + 1);
            long sum = 0;
            for (int i = low; i < high; i++) {
                sum += list.get(i);
            }
            if (sum < lowSum) {
                lowSum = sum;
            }
            if (sum > highSum) {
                highSum = sum;
            }
            System.out.println(String.format("Range %d   Sum %,d", range, sum));
        }

        double diff = highSum - lowSum;
        double per = diff / lowSum;

        System.out.println("Percent diff between low and high sum = " + per * 100);
    }

}
