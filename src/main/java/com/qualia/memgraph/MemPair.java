package com.qualia.memgraph;

public class MemPair implements Comparable<MemPair> {
    long src;
    long dest;

    public MemPair(long src, long dest) {
        super();
        this.src = src;
        this.dest = dest;
    }

    @Override
    public int compareTo(MemPair o) {
        int c1 = Long.compare(src, o.src);
        if (c1 != 0) {
            return c1;
        }
        return Long.compare(dest, o.dest);
    }
}
