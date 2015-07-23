package com.localresponse.tapad_util;


import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class TapadUniqueLineAnalyzer {

    private Splitter tabSplitter = Splitter.on("\t");
    private Joiner tabJoiner = Joiner.on("\t");

    private long numLines = 0;
    private final HashFunction hashFunction = Hashing.murmur3_128(); // Simulates MD5 hash (but only 8 bytes)
    private final TLongHashSet hashSet = new TLongHashSet();
    private int numSkip;


    private void run(String tapadDir) throws IOException {
        File[] files = (new File(tapadDir)).listFiles();
        List<File> list = Arrays.asList(files);

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.setUseGzip(true);
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                TapadUniqueLineAnalyzer.this.processLine(line, curLine);
            }


            public String getStatus() {
                String status = TapadUniqueLineAnalyzer.this.getStatus();

                return status;
            }

        });
    }


    protected String getStatus() {
        double uniqPer = (hashSet.size() + 0d) / numLines;
        return String.format("numSkip = %,9d   numLines = %,9d   hash size = %,9d   %% Unique = %6.1f", numSkip, numLines,
                hashSet.size(), uniqPer * 100);
    }


    protected void processLine(String line, long curLine) {
        List<String> devices = new ArrayList<String>(tabSplitter.splitToList(line));

        if (devices.size() > 15) {
            numSkip++;
            return;
        }

        numLines++;

        Collections.sort(devices);

        String s = tabJoiner.join(devices);
        long hashVal = hashFunction.hashString(s, Charsets.UTF_8).asLong();

        hashSet.add(hashVal);
    }


    public static void main(String[] args) throws Exception {
        String tapadDir = null;

        if (args.length > 0) {
            tapadDir = args[0];
            System.out.println("tapadDir = " + tapadDir);
        }

        TapadUniqueLineAnalyzer program = new TapadUniqueLineAnalyzer();
        program.run(tapadDir);
    }

}
