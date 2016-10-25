package com.qualia.test;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class QuickSortTest {

    private static void sort(List<Integer> list, int low, int high) {
        if (low >= high) {
            return;
        }

        int[] pivots = new int[3];

        // Pick a pivot
        int mid = (high - low) / 2 + low;
        pivots[0] = list.get(low);
        pivots[1] = list.get(mid);
        pivots[2] = list.get(low);

        bubbleSort(pivots);

        int pivotVal = pivots[1];
        
        
    }


    private static void bubbleSort(int[] ary) {
        int numChange = 0;
        int temp;
        do {
            for (int i = 0; i < ary.length - 1; i++) {
                if (ary[i] > ary[i + 1]) {
                    // swap them
                    numChange++;
                    temp = ary[i];
                    ary[i] = ary[i + 1];
                    ary[i + 1] = temp;
                }
            }
        } while (numChange > 0);
    }


    public static void main(String[] args) {
        final int NUM = 1000;

        Random random = new Random();
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 0; i < NUM; i++) {
            list.add(random.nextInt(Integer.MAX_VALUE));
        }

        sort(list, 0, list.size());
    }

}
