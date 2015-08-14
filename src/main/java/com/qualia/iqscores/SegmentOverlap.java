package com.qualia.iqscores;


import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;


// Loads all of the segment files, and calculates overlap between all pairs.
// Hashes the entity id to an 8-byte long, so its possible (but unlikely) that some entities may collide in the hash.
public class SegmentOverlap {

    private final HashFunction hashFunction = Hashing.murmur3_128();
    private final Map<String, TLongHashSet> labelToUsersMap = new HashMap<String, TLongHashSet>();
    private final Map<String, String> idToLabelMap = new HashMap<String, String>();


    private void run(String dirPath) throws IOException {
        File dir = new File(dirPath);

        createLabelMap(dirPath);

        loadSegmentFiles(dir);

        calculateAllOverlaps();
    }


    private void loadSegmentFiles(File dir) throws IOException {
        File[] files = dir.listFiles();
        int num = 0;
        for (File oneFile : files) {
            num++;
            if (oneFile.getName().equals("labels")) {
                continue;
            }

            String label = idToLabelMap.get(oneFile.getName());
            if (label != null) {
                loadFile(oneFile, label);
                TLongHashSet set = labelToUsersMap.get(label);
                System.out.println(String.format("%3d of %3d   Label %-40s   File:  %s   %,12d", num, files.length, label,
                        oneFile.getName(), set.size()));
            }
        }
    }


    private void calculateAllOverlaps() {
        for (String label1 : labelToUsersMap.keySet()) {
            for (String label2 : labelToUsersMap.keySet()) {
                if (label1.equals(label2)) {
                    continue;
                }

                calculateOverlap(label1, label2);
            }
        }
    }


    private void calculateOverlap(String label1, String label2) {
        TLongHashSet set1 = labelToUsersMap.get(label1);
        TLongHashSet set2 = labelToUsersMap.get(label2);

        int numOverlap = 0;

        TLongIterator iter1 = set1.iterator();
        while (iter1.hasNext()) {
            long val1 = iter1.next();
            if (set2.contains(val1)) {
                numOverlap++;
            }
        }

        System.out.println(String.format("Overlap %-40s (%,12d) vs %-40s (%,12d):   %,12d", label1, set1.size(),
                label2, set2.size(), numOverlap));
    }


    private void createLabelMap(String dirPath) throws IOException {
        File labelFile = new File(dirPath, "labels");
        List<String> labelLines = Files.readLines(labelFile, Charsets.UTF_8);
        labelLines.remove(0); // Get rid of header
        Splitter tabSplitter = Splitter.on("\t");
        for (String oneLabelLine : labelLines) {
            List<String> parts = tabSplitter.splitToList(oneLabelLine);
            idToLabelMap.put(parts.get(0), parts.get(2));
        }
    }


    private void loadFile(File oneFile, String label) throws IOException {
        TLongHashSet userSet = labelToUsersMap.get(label);
        if (userSet == null) {
            userSet = new TLongHashSet();
            labelToUsersMap.put(label, userSet);
        }

        FileInputStream fis = new FileInputStream(oneFile);
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader br = new BufferedReader(isr);

        String line;
        while ((line = br.readLine()) != null) {
            long hash = hashFunction.hashString(line, Charsets.UTF_8).asLong();
            userSet.add(hash);
        }

        br.close();
    }


    public static void main(String[] args) throws IOException {
        String dir;
        if (args.length > 0) {
            dir = args[0];
        } else {
            dir = "/Users/benziegler/test_data/iq-scores";
        }

        SegmentOverlap program = new SegmentOverlap();
        program.run(dir);
    }

}
