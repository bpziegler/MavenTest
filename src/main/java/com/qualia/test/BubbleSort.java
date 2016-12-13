package com.qualia.test;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class BubbleSort {

    public static void sort(List<Integer> list) {

        while (true) {
            int numChange = 0;
            for (int i = 0; i < list.size() - 1; i++) {
                if (list.get(i).compareTo(list.get(i + 1)) > 0) {
                    swap(list, i, i + 1);
                    numChange++;
                }
            }
            if (numChange == 0) {
                break;
            }
        }
    }


    private static void swap(List<Integer> list, int i, int j) {
        Integer tmp = list.get(i);
        list.set(i, list.get(j));
        list.set(j, tmp);
    }


    public static List<Integer> generateRandomList(int size) {
        List<Integer> list = new ArrayList<Integer>();

        Random rand = new Random();

        for (int i = 0; i < size; i++) {
            list.add(rand.nextInt());
        }

        return list;
    }


    public static void main(String[] args) {
        for (int size = 1; size < 5000; size++) {
            List<Integer> list = generateRandomList(size);
            long t1 = System.currentTimeMillis();
            sort(list);
            long t2 = System.currentTimeMillis();
            System.out.println(String.format("Size %d   Elap %d", size, t2 - t1));
        }
    }

}
