package com.localresponse.tapad;


import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class CheckUnique {

    private final HashFunction hashFunction = Hashing.murmur3_128();

    private Splitter tabSplitter = Splitter.on("\t");
    private Joiner tabJoiner = Joiner.on("\t");

    private BufferedWriter bw;
    private long numNew = 0;
    private long numExist = 0;
    private TLongHashSet uniqueLines = new TLongHashSet();


    private void run(String tapadFile) throws IOException {
        FileOutputStream fs = new FileOutputStream("tapad_unique.txt");
        bw = new BufferedWriter(new OutputStreamWriter(fs), 128 * 1024);

        List<File> list = new ArrayList<File>();
        list.add(new File(tapadFile));
        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                CheckUnique.this.processLine(line, curLine);
            }


            public String getStatus() {
                String status = CheckUnique.this.getStatus();

                return status;
            }

        });

        bw.close();

        System.out.println(getStatus());
    }


    protected String getStatus() {
        return String.format("NumNew %,d   NumExist %,d", numNew, numExist);
    }


    protected void processLine(String line, long curLine) {
        boolean logLine = false;

        List<String> devices = new ArrayList<String>(tabSplitter.splitToList(line));
        Collections.sort(devices);
        String newLine = tabJoiner.join(devices);

        long hashVal = hashFunction.hashString(newLine, Charsets.UTF_8).asLong();

        if (uniqueLines.contains(hashVal)) {
            numExist++;
        } else {
            uniqueLines.add(hashVal);
            numNew++;
            logLine = true;
        }

        if (logLine) {
            try {
                bw.write(line + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String tapadFile = "/Users/benziegler/work/tapad/LocalResponse_ids_full_20140827_203357";

        if (args.length > 0) {
            tapadFile = args[0];
            System.out.println("tapadFile = " + tapadFile);
        }

        CheckUnique tapadSample = new CheckUnique();

        tapadSample.run(tapadFile);
    }
}
