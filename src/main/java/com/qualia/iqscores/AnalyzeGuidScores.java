package com.qualia.iqscores;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Splitter;
import com.localresponse.add_this_mapping.ILineProcessor;
import com.localresponse.add_this_mapping.MultiFileLineProcessor;


public class AnalyzeGuidScores {

    private final static String[] KNOWN_TAGS = { "Advertising", "Agriculture", "Art", "Automotive", "Aviation",
            "Babies and Parenting", "Banking", "Beverages", "Biotechnology", "Business", "Computers and Electronics",
            "Crime", "Disasters", "Economics", "Education", "Elections", "Fashion", "Food", "Hardware", "Health",
            "Hotels", "Intellectual Property", "Investing", "Labor", "Law", "Marriage", "Mobile Devices", "Politics",
            "Real Estate", "Renewable Energy", "Robotics", "Science", "Social Media", "Software and Internet", "Space",
            "Sports", "Technology", "Television", "Traditional Energy", "Travel", "Video Games", "War", "Weather" };

    private final ObjectMapper mapper = new ObjectMapper();
    private final Splitter splitter = Splitter.on("\t");
    private final ArrayList<String> labels = new ArrayList<String>();
    private final int[] counts = new int[KNOWN_TAGS.length];
    private final int[][] cond_counts = new int[KNOWN_TAGS.length][];
    private final HashMap<String, Integer> fieldIdx = new HashMap<String, Integer>();
    private String last_guid;
    private long numGuid = 0;


    private void run(String path) throws IOException {
        List<File> list = Arrays.asList(new File(path));

        for (int i = 0; i < KNOWN_TAGS.length; i++) {
            fieldIdx.put(KNOWN_TAGS[i], i);
            cond_counts[i] = new int[KNOWN_TAGS.length];
        }

        MultiFileLineProcessor multiLineProcessor = new MultiFileLineProcessor();
        // multiLineProcessor.setUseGzip(true);
        multiLineProcessor.processFiles(list, new ILineProcessor() {
            public void processLine(String line, long curLine) {
                AnalyzeGuidScores.this.processLine(line, curLine);
            }


            public String getStatus() {
                try {
                    return mapper.writeValueAsString(counts);
                } catch (JsonGenerationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (JsonMappingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return null;
            }

        });

        System.out.println("NumGuid = " + numGuid);

        mapper.writeValue(new File("cond_counts.json"), cond_counts);
    }


    protected void processLine(String line, long curLine) {
        List<String> parts = splitter.splitToList(line);
        String guid = parts.get(0);
        String label = parts.get(1);

        if (!guid.equals(last_guid)) {
            flush();
            labels.clear();
        }
        labels.add(label);

        last_guid = guid;
    }


    private void flush() {
        numGuid++;

        for (String oneLabel : labels) {
            Integer idx = fieldIdx.get(oneLabel);
            counts[idx]++;
        }

        // Now update conditional counts

        for (String predLabel : labels) {
            Integer pred_idx = fieldIdx.get(predLabel);
            for (String oneLabel : labels) {
                Integer idx = fieldIdx.get(oneLabel);
                cond_counts[pred_idx][idx]++;
            }
        }
    }


    public static void main(String[] args) throws IOException {
        String path = "/Users/benziegler/test_data/iq-scores/date=20150122/guids_sorted";

        if (args.length > 0) {
            path = args[0];
            System.out.println("guid file path = " + path);
        }

        AnalyzeGuidScores program = new AnalyzeGuidScores();
        program.run(path);
    }

}
