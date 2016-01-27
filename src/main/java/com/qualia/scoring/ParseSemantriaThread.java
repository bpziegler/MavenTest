package com.qualia.scoring;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.google.common.io.CountingInputStream;
import com.qualia.keystore_graph.Status;


public class ParseSemantriaThread implements Runnable {

    private final String filePath;
    private final BlockingQueue<Object> queue;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Status status;


    public ParseSemantriaThread(String filePath, BlockingQueue<Object> queue, Status status) {
        this.filePath = filePath;
        this.queue = queue;
        this.status = status;
    }


    public void safeRun() throws IOException, InterruptedException {
        FileInputStream fs = new FileInputStream(filePath);
        CountingInputStream cs = new CountingInputStream(fs);
        InputStream nextStream = cs;
        nextStream = new GZIPInputStream(nextStream);
        InputStreamReader isr = new InputStreamReader(nextStream);
        BufferedReader br = new BufferedReader(isr);

        String line;

        while ((line = br.readLine()) != null) {
            processLine(line);
        }

        br.close();
    }


    private void processLine(String line) throws IOException, InterruptedException {
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

        List<String> labelNames = new ArrayList<String>();
        for (JsonNode topicNode : topics) {
            labelNames.add(topicNode.get("title").asText());
        }

        SemantriaLine semantriaLine = new SemantriaLine(url, labelNames);
        queue.put(semantriaLine);
    }


    @Override
    public void run() {
        try {
            safeRun();
        } catch (IOException e) {
            // TODO:  Output an error somewhere
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO:  Output an error somewhere
            e.printStackTrace();
        }
    }

}
