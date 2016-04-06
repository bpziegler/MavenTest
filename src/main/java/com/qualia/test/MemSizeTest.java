package com.qualia.test;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


public class MemSizeTest {

    public static class DoubleLong implements Comparable<DoubleLong> {
        double a;
        double b;


        @Override
        public int compareTo(DoubleLong o) {
            int comp = Double.compare(a, o.a);
            if (comp != 0)
                return comp;
            comp = Double.compare(b, o.b);
            return comp;
        }


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            long temp;
            temp = Double.doubleToLongBits(a);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(b);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            return result;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DoubleLong other = (DoubleLong) obj;
            if (Double.doubleToLongBits(a) != Double.doubleToLongBits(other.a))
                return false;
            if (Double.doubleToLongBits(b) != Double.doubleToLongBits(other.b))
                return false;
            return true;
        }
    }


    public static void main(String[] args) {
        Set<DoubleLong> list = new TreeSet<DoubleLong>();
        
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 50 * 1000 * 1000; i++) {
            DoubleLong dl = new DoubleLong();
            dl.a = i;
            list.add(dl);
            if (i % 1000000 == 0) {
                final int MB = 1024 * 1024;
                final Runtime runtime = Runtime.getRuntime();
                long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / MB;
                long elap = System.currentTimeMillis() - startTime;
                System.out.println(String.format("num %,10d   elap %,7d   MB %,6d", i, elap, usedMB));
            }
        }
    }

}
