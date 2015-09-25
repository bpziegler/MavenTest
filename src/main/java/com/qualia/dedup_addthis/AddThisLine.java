package com.qualia.dedup_addthis;


import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;


public class AddThisLine {

    private static final Splitter tabSplitter = Splitter.on("\t");

    public final String line;
    public final List<String> parts;


    public AddThisLine(String line) {
        this.line = line;
        if (line != null) {
            parts = tabSplitter.splitToList(line);
        } else {
            parts = null;
        }
    }


    public static int compareLines(AddThisLine line1, AddThisLine line2) {
        Preconditions.checkState((line1.line != null) || (line2.line != null), "line1 and line2 can't both be null!");
        if (line1.line == null) {
            return 1;
        } else if (line2.line == null) {
            return -1;
        } else {
            return line1.parts.get(0).compareTo(line2.parts.get(0));
        }
    }

}
