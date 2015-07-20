package com.qualia.flatset;


public class LongWrapper implements Comparable<LongWrapper> {

    private final long[] longArray;


    public LongWrapper(long[] longArray) {
        this.longArray = longArray;
    }


    public long[] getLongs() {
        return longArray;
    }


    @Override
    public int compareTo(LongWrapper o) {
        int comp = Integer.compare(longArray.length, o.longArray.length);
        if (comp != 0) {
            return comp;
        }

        for (int i = 0; i < longArray.length; i++) {
            int bcomp = Long.compare(longArray[i], o.longArray[i]);
            if (bcomp != 0) {
                return bcomp;
            }
        }

        return 0;
    }

}
