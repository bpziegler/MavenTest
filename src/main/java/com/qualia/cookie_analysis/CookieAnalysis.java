package com.qualia.cookie_analysis;


import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class CookieAnalysis {

    private final ObjectMapper mapper = new ObjectMapper();
    private final HashFunction hashFunction = Hashing.murmur3_128(); // Simulates MD5 hash (but only 8 bytes)
    private final TLongIntHashMap hashMap = new TLongIntHashMap();
    private long numCookie;


    public void walk(String path, List<File> files) {

        File root = new File(path);
        File[] list = root.listFiles();

        if (list == null)
            return;

        for (File f : list) {
            if (f.isDirectory()) {
                walk(f.getAbsolutePath(), files);
            } else {
                files.add(f);
            }
        }
    }


    private void run(String dir) throws IOException {
        List<File> list = new ArrayList<File>();
        walk(dir, list);

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        multiLineProcessor.setUseGzip(true);
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                CookieAnalysis.this.processLine(line, curLine);
            }


            public String getStatus() {
                return String.format("%,8d   %,8d   Dups = %,8d", numCookie, hashMap.size(), numCookie - hashMap.size());
            }
        });

        int[] vals = hashMap.values();
        int maxSize = 0;
        for (int i = 0; i < vals.length; i++) {
            if (vals[i] > maxSize) {
                maxSize = vals[i];
            }
        }

        System.out.println("MaxSize = " + maxSize);
    }


    protected void processLine(String line, long curLine) {
        try {
            numCookie++;
            JsonNode tree = mapper.readTree(line);
            String ip = tree.get("ip").asText();
            long hash = hashFunction.hashString(ip, Charsets.UTF_8).asLong();
            int val = hashMap.get(hash);
            hashMap.put(hash, val+1);

            // ArrayNode mapping = (ArrayNode) tree.get("mapping");
            // if (mapping == null)
            // return;
            // for (JsonNode oneMapping : mapping) {
            // numCookie++;
            // String pid = oneMapping.get("pid").asText();
            // String uid = oneMapping.get("uid").asText();
            // String pid_uid = pid + "_" + uid;
            // long hash = hashFunction.hashString(pid_uid, Charsets.UTF_8).asLong();
            // int val = hashMap.get(hash);
            // hashMap.put(hash, val + 1);
            // }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws IOException {
        String dir = "/Users/benziegler/test_data/cookies";

        if (args.length > 0) {
            dir = args[0];
            System.out.println("dir = " + dir);
        }

        CookieAnalysis program = new CookieAnalysis();
        program.run(dir);
    }

}
