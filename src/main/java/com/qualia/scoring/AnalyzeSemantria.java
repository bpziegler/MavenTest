package com.qualia.scoring;


import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class AnalyzeSemantria {

    private ObjectMapper mapper = new ObjectMapper();
    private long validLine;
    private long totTopics;
    private Map<String, ProcessedStats> urlStatsMap = new HashMap<String, ProcessedStats>();
    private Splitter dotSplitter = Splitter.on(".");
    private Joiner dotJoiner = Joiner.on(".");

    public static class ProcessedStats {
        int numProcessed;
        int numFailed;
    }


    private void run(String dirPath) throws IOException {

        MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
        lineProcessor.setUseGzip(true);
        lineProcessor.processDir(dirPath, new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                try {
                    AnalyzeSemantria.this.processLine(line, curLine);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


            @Override
            public String getStatus() {
                return String.format("valid %,d   topics %,d   hosts %,d", validLine, totTopics, urlStatsMap.size());
            }
        });

        saveMapToFile();
    }


    private void saveMapToFile() throws IOException {
        FileOutputStream fs = new FileOutputStream("host_stats.txt");
        OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
        BufferedWriter bw = new BufferedWriter(osw);

        for (String host : urlStatsMap.keySet()) {
            ProcessedStats stats = urlStatsMap.get(host);
            String line = String.format("%s\t%d\t%d\t", host, stats.numProcessed, stats.numFailed, stats.numProcessed
                    + stats.numFailed);
            bw.write(line);
            bw.newLine();
        }

        bw.close();
    }


    protected void processLine(String line, long curLine) throws JsonProcessingException, IOException,
            URISyntaxException {
        JsonNode jsonTree = mapper.readTree(line);
        String id = jsonTree.get("id").asText();
        String url = jsonTree.get("url").asText();
        String status = jsonTree.get("status").asText();
        ArrayNode topics = (ArrayNode) jsonTree.get("topics");
        if (id == null)
            return;
        if (url == null)
            return;
        validLine++;
        if (topics != null) {
            totTopics += topics.size();
        }

        URI uri = new URI(url);
        String host = uri.getHost();
//        List<String> parts = new ArrayList<String>(dotSplitter.splitToList(host));
//        while (parts.size() > 2) {
//            parts.remove(0);
//        }
//        String normalHost = dotJoiner.join(parts);

        ProcessedStats stats = urlStatsMap.get(host);
        if (stats == null) {
            stats = new ProcessedStats();
            urlStatsMap.put(host, stats);
        }
        boolean valid = (topics != null) && (topics.size() > 0) && (status.equals("PROCESSED"));
        if (valid) {
            stats.numProcessed++;
        } else {
            stats.numFailed++;
        }
    }


    public static void main(String[] args) throws IOException {
        AnalyzeSemantria program = new AnalyzeSemantria();
        program.run(args[0]);
    }

}
