package com.qualia.scoring;


import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.qualia.util.ILineProcessor;
import com.qualia.util.MultiFileLineProcessor;


public class AnalyzeSemantria {

    ObjectMapper mapper = new ObjectMapper();
    private long validLine;
    private long totTopics;


    private void run(String dirPath) throws IOException {
        MultiFileLineProcessor lineProcessor = new MultiFileLineProcessor();
        lineProcessor.setUseGzip(true);
        lineProcessor.processDir(dirPath, new ILineProcessor() {
            @Override
            public void processLine(String line, long curLine) {
                try {
                    AnalyzeSemantria.this.processLine(line, curLine);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


            @Override
            public String getStatus() {
                return String.format("valid %,d   topics %,d", validLine, totTopics);
            }
        });
    }


    protected void processLine(String line, long curLine) throws JsonProcessingException, IOException {
        JsonNode jsonTree = mapper.readTree(line);
        String id = jsonTree.get("id").asText();
        String url = jsonTree.get("url").asText();
        String status = jsonTree.get("status").asText();
        ArrayNode topics = (ArrayNode) jsonTree.get("topics");
        if (id == null)
            return;
        if (url == null)
            return;
        if (status == null)
            return;
        if (!status.equals("PROCESSED"))
            return;
        if (topics == null)
            return;
        if (topics.size() == 0)
            return;
        validLine++;
        totTopics += topics.size();
    }


    public static void main(String[] args) throws IOException {
        AnalyzeSemantria program = new AnalyzeSemantria();
        program.run(args[0]);
    }

}
