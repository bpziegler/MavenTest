package com.qualia.test;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class Sortable implements Comparable<Sortable> {

    public final double val;
    public static int numCompare = 0;


    public Sortable(double val) {
        this.val = val;
    }


    @Override
    public int compareTo(Sortable o) {
        int result = Double.compare(val, o.val);
        numCompare++;
        System.out.println(String.format("Compare %3d   %6.1f to %6.1f = %2d", numCompare, val, o.val, result));
        return result;
    }


    public static void main(String[] args) {
        Random rand = new Random();
        final int NUM = 10;
        List<Sortable> list = new ArrayList<Sortable>();
        for (int i = 0; i < NUM; i++) {
            Sortable val = new Sortable(rand.nextInt(100));
            list.add(val);
        }
        Collections.sort(list);
    }

}
